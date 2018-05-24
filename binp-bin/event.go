package main

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/sryanyuan/binp/rule"

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
	wmgr   *workerManager
	tables map[string]*TableInfo

	fromDB *sql.DB
}

// NewEventHandler create a event handler
func NewEventHandler(s *slave.Slave, cfg *AppConfig) *EventHandler {
	return &EventHandler{
		slv:    s,
		cfg:    cfg,
		tables: make(map[string]*TableInfo),
	}
}

// Prepare do initialize work
func (e *EventHandler) Prepare() error {
	var err error
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
	sourceConfig := e.cfg.DataSource.DBConfig
	sourceConfig.Type = "mysql"
	e.fromDB, err = dbFromDBConfig(&sourceConfig)
	if nil != err {
		return errors.Annotate(err, "Create mysql master connection failed")
	}
	e.wmgr, err = newWorkerManager(e.cfg)
	if nil != err {
		return errors.Annotate(err, "Create worker manager failed")
	}

	// Read the position from storage
	var position mconn.Position
	err = e.strw.readPosition(&position)
	if nil != err {
		if err != errStorageKeyNotFound {
			return errors.Trace(err)
		}
	}

	// Start workers
	if err = e.wmgr.start(); nil != err {
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
	e.wmgr.stop()
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
			logrus.Debugf("%v", evt)
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
	return nil
}

func (e *EventHandler) getTable(schema string, table string, desc *rule.SyncDesc) (*TableInfo, error) {
	var err error
	key := getTableKey(schema, table)
	ti, ok := e.tables[key]
	if !ok {
		// Load table information from master
		var tbl TableInfo
		tbl.Schema = schema
		tbl.Name = table

		err = getTableColumnInfo(e.fromDB, schema, table, &tbl)
		if nil != err {
			return nil, errors.Trace(err)
		}
		err = getTableIndexInfo(e.fromDB, schema, table, &tbl)
		if nil != err {
			return nil, errors.Trace(err)
		}

		// Handle table info with sync desc
		tbl.IndexColumns = make([]*ColumnInfo, 0, len(tbl.Columns))
		for _, col := range tbl.Columns {
			if col.HasIndex {
				tbl.IndexColumns = append(tbl.IndexColumns, col)
			}
		}
		if nil != desc {
			if desc.IndexKeys != nil {
				for _, ik := range desc.IndexKeys {
					col := findColumnByName(tbl.Columns, ik)
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
	}

	return ti, nil
}

func (e *EventHandler) cleanTable(schema string, table string) {
	delete(e.tables, getTableKey(schema, table))
}
