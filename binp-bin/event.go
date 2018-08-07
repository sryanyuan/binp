package main

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/sryanyuan/binp/utils"
	"github.com/sryanyuan/binp/worker"

	"github.com/sryanyuan/binp/observer"
	"github.com/sryanyuan/binp/rule"
	"github.com/sryanyuan/binp/tableinfo"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/binlog"
	"github.com/sryanyuan/binp/mconn"
	"github.com/sryanyuan/binp/slave"
	"github.com/sryanyuan/binp/storage"
)

// EventHandler receives the binlog from master and handle it
type EventHandler struct {
	slv    *slave.Slave
	cfg    *AppConfig
	strw   *storageReaderWriter
	wmgr   *worker.WorkerManager
	tables map[string]*tableinfo.TableInfo
	nchain *observer.NotifyChain

	fromDBs []*sql.DB
}

// NewEventHandler create a event handler
func NewEventHandler(s *slave.Slave, cfg *AppConfig) *EventHandler {
	return &EventHandler{
		slv:    s,
		cfg:    cfg,
		tables: make(map[string]*tableinfo.TableInfo),
		nchain: &observer.NotifyChain{},
	}
}

// Prepare do initialize work
func (e *EventHandler) Prepare() error {
	var err error
	// Check data source count
	if nil == e.cfg.DataSources || 0 == len(e.cfg.DataSources) {
		return errors.New("Empty data source")
	}
	// Using local storage by default
	if len(e.cfg.StorageSource) < 2 {
		return errors.Errorf("Invalid storage source %s", e.cfg.StorageSource)
	}
	index := strings.IndexByte(e.cfg.StorageSource, ':')
	if index < 0 || index >= len(e.cfg.StorageSource)-1 {
		return errors.Errorf("Invalid storage source %s", e.cfg.StorageSource)
	}
	stype := strings.ToLower(e.cfg.StorageSource[:index])
	svalue := e.cfg.StorageSource[index+1:]

	switch stype {
	case storage.LocalStorageSignature:
		{
			st, err := storage.NewLocalStorage(svalue)
			if nil != err {
				return errors.Trace(err)
			}
			e.strw = newStorageReaderWriter(st)
		}
	default:
		{
			return errors.Errorf("Unknown storage type %s", stype)
		}
	}

	// Initialize mysql master connection and destination workers
	e.fromDBs = make([]*sql.DB, 0, len(e.cfg.DataSources))
	for i := range e.cfg.DataSources {
		ds := &e.cfg.DataSources[i]
		var sourceConfig mconn.DBConfig
		sourceConfig.Host = ds.Host
		sourceConfig.Port = ds.Port
		sourceConfig.Username = ds.Username
		sourceConfig.Password = ds.Password
		sourceConfig.Type = "mysql"
		fromDB, err := utils.CreateDB(&sourceConfig)
		if nil != err {
			return errors.Annotate(err, "Create mysql master connection failed")
		}
		e.fromDBs = append(e.fromDBs, fromDB)
	}

	e.wmgr, err = worker.NewWorkerManager(&e.cfg.Worker)
	if nil != err {
		return errors.Annotate(err, "Create worker manager failed")
	}

	// Read the position from storage
	var position mconn.ReplicationPoint
	err = e.strw.readPoint(&position)
	if nil != err {
		if err != errStorageKeyNotFound {
			return errors.Trace(err)
		}
	}

	// Start workers
	if err = e.wmgr.Start(); nil != err {
		return errors.Trace(err)
	}

	// Start slave
	if err = e.slv.Start(position); nil != err {
		return errors.Trace(err)
	}

	return nil
}

// Close closes the event handler and stop the slave
func (e *EventHandler) Close() error {
	// Close the slave
	e.slv.Stop()
	// Stop workers
	e.wmgr.Stop()
	return nil
}

func (e *EventHandler) handleEvent() error {
	for {
		ctx := context.Background()
		ctx, cancelFn := context.WithTimeout(ctx, time.Second*5)
		event, err := e.slv.Next(ctx)
		cancelFn()
		// Timeout
		if err == context.DeadlineExceeded {
			continue
		}
		// Check error
		if nil != err {
			return errors.Trace(err)
		}
		// Parse events
		if err = e.onBinlogEvent(event); nil != err {
			return errors.Trace(err)
		}
	}
}

func (e *EventHandler) onBinlogEvent(event *binlog.Event) error {
	var err error

	// Here we interest is to record the current replication point(position or gtid)
	// and filter event we do not interested. Column filter and rewrite will also processed here.
	// Other process should be in observers
	switch event.Header.EventType {
	case binlog.FormatDescriptionEventType:
		{

		}
	case binlog.RotateEventType:
		{
			evt := event.Payload.Rotate
			logrus.Infof("Rotate to binlog %v:%v", evt.NextName, evt.Position)
		}
	case binlog.QueryEventType:
		{
			evt := event.Payload.Query
			logrus.Debugf("query %v", evt.Query)
		}
	case binlog.WriteRowsEventV0Type, binlog.WriteRowsEventV1Type, binlog.WriteRowsEventV2Type,
		binlog.UpdateRowsEventV0Type, binlog.UpdateRowsEventV1Type, binlog.UpdateRowsEventV2Type,
		binlog.DeleteRowsEventV0Type, binlog.DeleteRowsEventV1Type, binlog.DeleteRowsEventV2Type:
		{
			evt := event.Payload.Rows
			logrus.Debug(evt)
			if err = e.onRowsEvent(event); nil != err {
				return errors.Trace(err)
			}
		}
	case binlog.GTIDEventType:
		{
			evt := event.Payload.GTID
			logrus.Debugf("%v", evt)
		}
	case binlog.MariadbGTIDEventType:
		{
			evt := event.Payload.MariadbGTID
			logrus.Debugf("%v", evt)
		}
	}

	if err = e.nchain.Broadcast(event); nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (e *EventHandler) getTable(schema string, table string, desc *rule.SyncDesc) (*tableinfo.TableInfo, error) {
	var err error
	key := utils.GetTableKey(schema, table)
	ti, ok := e.tables[key]
	if ok && ti != nil {
		return ti, nil
	}

	// Load table information from master
	var tbl tableinfo.TableInfo
	tbl.Schema = schema
	tbl.Name = table
	// TODO: If get information failed, retry and switch data source(Master slave switch)
	if err = tbl.GetTableColumnInfo(e.fromDBs[e.slv.GetDataSourceIndex()], schema, table); nil != err {
		return nil, errors.Trace(err)
	}
	if err = tbl.GetTablePrimaryKeys(e.fromDBs[e.slv.GetDataSourceIndex()], schema, table); nil != err {
		return nil, errors.Trace(err)
	}

	// Handle table info with sync desc
	tbl.IndexColumns = make([]*tableinfo.ColumnInfo, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		if col.IsPrimary {
			tbl.IndexColumns = append(tbl.IndexColumns, col)
		}
	}
	if nil != desc {
		if desc.IndexKeys != nil {
			for _, ik := range desc.IndexKeys {
				col := tableinfo.FindColumnByName(tbl.Columns, ik)
				if nil == col {
					return nil, errors.Errorf("Can't find column %s by sync desc, table = %s",
						ik, key)
				}
				tbl.IndexColumns = append(tbl.IndexColumns, col)
			}
		}
	}
	if len(tbl.IndexColumns) == 0 {
		return nil, errors.Errorf("Table %s missing index key", key)
	}

	ti = &tbl
	e.tables[key] = ti

	return ti, nil
}

func (e *EventHandler) cleanTable(schema string, table string) {
	delete(e.tables, utils.GetTableKey(schema, table))
}
