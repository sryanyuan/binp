package slave

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sryanyuan/binp/rule"

	"github.com/sryanyuan/binp/binlog"

	"github.com/sryanyuan/binp/mconn"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
)

const (
	slaveStatusNone = iota
	slaveStatusRunning
	slaveStatusExited
	slaveStatusAbnormal
)

const (
	defaultEventBufferSize        = 10240
	defaultHeartbeatInterval      = 30
	defaultSwitchMasterRetryTimes = 30
)

var (
	// ErrUserClosed represents the slave is closed by user
	ErrUserClosed = errors.New("User closed")
)

// MasterStatus is status of master
type MasterStatus struct {
	Version string                 `json:"version"`
	Pos     mconn.ReplicationPoint `json:"position"`
}

// Slave represents a slave node like a mysql slave to participate the mysql replication
type Slave struct {
	cancelCtx         context.Context
	cancelFn          context.CancelFunc
	wg                sync.WaitGroup
	mu                sync.Mutex
	dss               []mconn.DataSource
	dsi               int64
	rc                *mconn.ReplicationConfig
	status            int64
	currentRplPoint   mconn.ReplicationPoint
	eq                *eventQueue
	conn              *mconn.Conn
	si                mconn.HandshakeInfo
	mariaDB           bool
	parser            *binlog.Parser
	lastHeartbeatTime uint32
}

// NewSlave creates a new slave
func NewSlave(dss []mconn.DataSource, rc *mconn.ReplicationConfig, srule rule.ISyncRule) *Slave {
	sl := &Slave{}
	// Create parser
	sl.parser = binlog.NewParser()
	sl.parser.SetSyncRule(srule)
	sl.cancelCtx, sl.cancelFn = context.WithCancel(context.Background())
	sl.rc = rc
	sl.dss = dss
	// Create event queue
	queueBufferSize := defaultEventBufferSize
	if 0 != rc.EventBufferSize {
		queueBufferSize = rc.EventBufferSize
	}
	sl.eq = newEventQueue(queueBufferSize)

	return sl
}

// Start starts the slave at the position
func (s *Slave) Start(pos mconn.ReplicationPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if atomic.LoadInt64(&s.status) != slaveStatusNone {
		return errors.New("slave already working")
	}

	if nil == s.dss || 0 == len(s.dss) {
		return errors.New("Empty data source")
	}

	s.currentRplPoint = pos
	logrus.Infof("Start sync from %v:%v(%v)",
		s.currentRplPoint.Filename, s.currentRplPoint.Offset, s.currentRplPoint.Gtid)
	err := s.prepare()
	if nil != err {
		return errors.Trace(err)
	}
	atomic.StoreInt64(&s.status, slaveStatusRunning)

	s.wg.Add(1)
	go s.pumpBinlog()

	return nil
}

// Stop stops the slave
func (s *Slave) Stop() {
	s.mu.Lock()
	s.mu.Unlock()

	if atomic.LoadInt64(&s.status) != slaveStatusRunning {
		return
	}
	atomic.StoreInt64(&s.status, slaveStatusExited)
	s.cancelFn()
	// Close the connection
	s.conn.Close()
	s.wg.Wait()
}

// Next gets the binlog event until a binlog comes or context timeout
func (s *Slave) Next(ctx context.Context) (*binlog.Event, error) {
	if atomic.LoadInt64(&s.status) != slaveStatusRunning {
		return nil, errors.New("slave not running")
	}

	select {
	case ev := <-s.eq.eventCh:
		{
			return ev, nil
		}
	case err := <-s.eq.errorCh:
		{
			return nil, err
		}
	case <-ctx.Done():
		{
			return nil, ctx.Err()
		}
	}
}

func (s *Slave) pushQueueError(err error) {
	select {
	case s.eq.errorCh <- err:
		{

		}
	default:
	}
}

func (s *Slave) pushQueueEvent(event *binlog.Event) {
	s.eq.eventCh <- event
}

func (s *Slave) prepare() error {
	if s.currentRplPoint.Offset < 4 {
		// MySQL binlog events is started at position 4 as a Format_desc event
		s.currentRplPoint.Offset = 4
	}

	if err := s.registerSlave(); nil != err {
		return errors.Trace(err)
	}

	// Send dump binlog command
	if err := s.conn.StartDumpBinlog(s.currentRplPoint); nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (s *Slave) disableBinlogChecksum() error {
	// We just set the binlog checksum to the global binlog_checksum
	_, err := s.conn.Exec("SET @master_binlog_checksum = @@global.binlog_checksum")
	if nil != err {
		return errors.Trace(err)
	}
	// Get the current checksum value
	rows, err := s.conn.Exec("SELECT @master_binlog_checksum")
	if nil != err {
		return errors.Trace(err)
	}
	if err = rows.Results.Next(); nil != err {
		if err == io.EOF {
			// No rows
		} else {
			rows.Results.Close()
			return errors.Trace(err)
		}
	}
	if nil == err {
		// Read rows
		checksum, err := rows.Results.GetAtString(0)
		if nil != err {
			return errors.Trace(err)
		}
		if strings.EqualFold(checksum, "CRC32") {
			s.parser.SetChecksum(binlog.ChecksumAlgCRC32)
		}
	}
	rows.Results.Close()

	return nil
}

func (s *Slave) enableBinlogHeartbeat() error {
	intervalNanoSecs := int64(defaultHeartbeatInterval * 1e9)
	// We just set the binlog checksum to the global binlog_checksum
	_, err := s.conn.Exec(fmt.Sprintf("SET @master_heartbeat_period = %d", intervalNanoSecs))
	if nil != err {
		return errors.Trace(err)
	}
	// Enable connection's read timeout
	s.conn.SetReadTimeout(time.Duration(defaultHeartbeatInterval*time.Second) * 3 / 2)
	return nil
}

func (s *Slave) nextDataSource() {
	atomic.AddInt64(&s.dsi, 1)
}

// GetDataSourceIndex get the current data source index used by replication replication
func (s *Slave) GetDataSourceIndex() int {
	return int(atomic.LoadInt64(&s.dsi)) % len(s.dss)
}

// GetDataSource get the current data source used by replication replication
func (s *Slave) GetDataSource() *mconn.DataSource {
	return s.getDataSource()
}

func (s *Slave) getDataSource() *mconn.DataSource {
	cur := int(atomic.LoadInt64(&s.dsi))
	return &s.dss[cur%len(s.dss)]
}

func (s *Slave) registerSlave() error {
	if s.conn != nil {
		s.conn.Close()
	}

	var err error

	// Connect to mysql
	ds := s.getDataSource()
	s.conn = &mconn.Conn{}
	err = s.conn.Connect(ds, "")
	if nil != err {
		return errors.Trace(err)
	}
	s.conn.GetHandshakeInfo(&s.si)
	logrus.Infof("Connect to mysql %s success", ds.Address())
	logrus.Infof("Master status: %v", &s.si)

	// Is mariadb ?
	if strings.Contains(strings.ToUpper(s.si.ServerVersion), "MARIADB") {
		s.mariaDB = true
	}

	// Set keepalive period
	if s.rc.KeepAlivePeriod != 0 {
		if s.conn.SetKeepalive(time.Second * time.Duration(s.rc.KeepAlivePeriod)) {
			logrus.Infof("Update mysql connection keepalive time to %v seconds success",
				s.rc.KeepAlivePeriod)
		} else {
			logrus.Infof("Update mysql connection keepalive time to %v seconds failed",
				s.rc.KeepAlivePeriod)
		}
	}

	// Disable binlog checksum
	if err = s.disableBinlogChecksum(); nil != err {
		return errors.Trace(err)
	}

	// Enable binlog heartbeat
	if err = s.enableBinlogHeartbeat(); nil != err {
		return errors.Trace(err)
	}

	// If is mariadb, enable gtid
	// https://github.com/alibaba/canal/wiki/BinlogChange(MariaDB5&10)
	if s.mariaDB {
		if _, err = s.conn.Exec("SET @mariadb_slave_capability=4"); nil != err {
			return errors.Trace(err)
		}
	}

	// Register slave
	if err = s.conn.RegisterSlave(s.rc); nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (s *Slave) onBinlogPumped(event *binlog.Event) error {
	if event.Header.LogPos > 0 {
		s.currentRplPoint.Offset = event.Header.LogPos
	}

	switch event.Header.EventType {
	case binlog.RotateEventType:
		{
			evt := event.Payload.Rotate
			// If using position replication, update the replication position
			s.currentRplPoint.Filename = evt.NextName
			s.currentRplPoint.Offset = uint32(evt.Position)
			logrus.Infof("Rotate to %v:%v(%v)", s.currentRplPoint.Filename, s.currentRplPoint.Offset, s.currentRplPoint.Gtid)
		}
	case binlog.WriteRowsEventV0Type, binlog.WriteRowsEventV1Type, binlog.WriteRowsEventV2Type,
		binlog.UpdateRowsEventV0Type, binlog.UpdateRowsEventV1Type, binlog.UpdateRowsEventV2Type,
		binlog.DeleteRowsEventV0Type, binlog.DeleteRowsEventV1Type, binlog.DeleteRowsEventV2Type:
		{
			s.currentRplPoint.Offset = event.Header.LogPos
		}
	case binlog.HeartbeatEventType:
		{
			s.lastHeartbeatTime = uint32(time.Now().Unix())
		}
	}

	return nil
}

func (s *Slave) pumpBinlog() {
	defer func() {
		s.wg.Done()
	}()

	// Read the following packet
	for {
		data, err := s.conn.ReadPacket()
		if nil != err {
			logrus.Errorf("Read packet from master error: %v", err)
			err = s.onPumpBinlogConnectionError()
			if nil != err {
				s.pushQueueError(err)
				return
			}
			// If retry success
			logrus.Infof("Retry sync at point %s:%d(%s) success",
				s.currentRplPoint.Filename, s.currentRplPoint.Offset, s.currentRplPoint.Gtid)
			continue
		}

		// Read binlog event
		switch data[0] {
		case mconn.PacketHeaderERR:
			{
				var perr mconn.PacketErr
				if err = perr.Decode(data); nil != err {
					s.pushQueueError(errors.Trace(err))
					return
				}
				s.pushQueueError(errors.Errorf("Error %v:%v", perr.ErrorCode, perr.ErrorMessage))
				return
			}
		case mconn.PacketHeaderEOF:
			{
				// Ignore ?
				continue
			}
		case mconn.PacketHeaderOK:
			{
				// Parse the binlog event
				event, err := s.parser.Parse(data)
				if nil != err {
					s.pushQueueError(errors.Trace(err))
					return
				}
				if !event.Payload.Parsed {
					//logrus.Debugf("Skip unparsed event, event type = %v", event.Header.EventType)
					continue
				}
				if err = s.onBinlogPumped(event); nil != err {
					s.pushQueueError(errors.Trace(err))
					return
				}
				s.pushQueueEvent(event)
			}
		default:
			{
				s.pushQueueError(errors.Errorf("Receive unknown binlog header %v", data[0]))
				return
			}
		}
	}
}

func (s *Slave) onPumpBinlogConnectionError() error {
	retryTimes := 0
	// If error occurs, check context has cancelled and retry
	for {
		select {
		case <-s.cancelCtx.Done():
			{
				return ErrUserClosed
			}
		case <-time.After(time.Second):
			{
				// Retry sync
				if s.rc.EnableGtid {
					// If using gtid, empty gtid is allowed
				} else {
					if s.currentRplPoint.Filename == "" {
						return errors.Errorf("Can't retry sync with invalid position %v.%v",
							s.currentRplPoint.Filename, s.currentRplPoint.Offset)
					}
				}
				logrus.Infof("Retry sync from position %v:%v(%v)",
					s.currentRplPoint.Filename, s.currentRplPoint.Offset, s.currentRplPoint.Gtid)
				// Do retry
				retryTimes++
				s.parser.Reset()
				if err := s.prepare(); nil != err {
					logrus.Errorf("Retry sync error %v, retry times %v", err, retryTimes)
					// Check need switch master
					if retryTimes%defaultSwitchMasterRetryTimes == 0 {
						logrus.Infof("Select next data source due to master down")
						s.nextDataSource()
					}
					continue
				}

				return nil
			}
		}
	}
}
