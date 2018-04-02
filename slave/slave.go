package slave

import (
	"context"
	"sync"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/anumber"

	"github.com/sryanyuan/binp/bevent"
)

const (
	slaveStatusNone = iota
	slaveStatusRunning
	slaveStatusExited
	slaveStatusAbnormal
)

const (
	defaultEventBufferSize = 10240
)

// ReplicationConfig specify the master information
type ReplicationConfig struct {
	Host            string   `json:"host" toml:"host"`
	Port            uint16   `json:"port" toml:"port"`
	Username        string   `json:"username" toml:"username"`
	Password        string   `json:"password" toml:"password"`
	Charset         string   `json:"charset" toml:"charset"`
	SlaveID         int64    `json:"slave-id" toml:"slave-id"`
	Pos             Position `json:"position" toml:"position"`
	EnableGtid      bool     `json:"enable-gtid" toml:"enable-gtid"`
	EventBufferSize int      `json:"event-buffer-size" toml:"event-buffer-size"`
}

// Slave represents a slave node like a mysql slave to participate the mysql replication
type Slave struct {
	cancelCtx  context.Context
	cancelFn   context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
	config     *ReplicationConfig
	status     anumber.AtomicInt64
	currentPos Position
	eq         *eventQueue
	conn       *slaveConn
}

// NewSlave creates a new slave
func NewSlave(cfg *ReplicationConfig) *Slave {
	sl := &Slave{}
	sl.cancelCtx, sl.cancelFn = context.WithCancel(context.Background())
	sl.config = cfg
	// Create event queue
	queueBufferSize := defaultEventBufferSize
	if 0 != cfg.EventBufferSize {
		queueBufferSize = cfg.EventBufferSize
	}
	sl.eq = newEventQueue(queueBufferSize)

	return sl
}

// Start starts the slave at the position
func (s *Slave) Start(pos Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status.Get() != slaveStatusNone {
		return errors.New("slave already working")
	}

	err := s.prepare()
	if nil != err {
		return errors.Trace(err)
	}
	s.status.Set(slaveStatusRunning)

	s.wg.Add(1)
	go s.pumpBinlog()

	return nil
}

// Stop stops the slave
func (s *Slave) Stop() {
	s.mu.Lock()
	s.mu.Unlock()

	if s.status.Get() != slaveStatusRunning {
		return
	}
	s.status.Set(slaveStatusExited)
	s.cancelFn()
	s.wg.Wait()
}

// Next gets the binlog event until a binlog comes or context timeout
func (s *Slave) Next(ctx context.Context) (*bevent.BinlogEvent, error) {
	if s.status.Get() != slaveStatusRunning {
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

func (s *Slave) prepare() error {
	s.currentPos = s.config.Pos
	if s.currentPos.Offset < 4 {
		// MySQL binlog events is started at position 4 as a Format_desc event
		s.currentPos.Offset = 4
	}

	if err := s.registerSlave(); nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (s *Slave) registerSlave() error {
	if s.conn != nil {
		s.conn.Close()
	}

	var err error

	s.conn = &slaveConn{}
	err = s.conn.Connect(s.config.Host, s.config.Port, s.config.Username, s.config.Password, "")
	if nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (s *Slave) pumpBinlog() {
	defer s.wg.Done()
}
