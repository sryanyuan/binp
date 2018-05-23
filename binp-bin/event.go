package main

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/binlog"
	"github.com/sryanyuan/binp/mconn"
	"github.com/sryanyuan/binp/slave"
	"github.com/sryanyuan/binp/storage"
)

// EventHandler receives the binlog from master and handle it
type EventHandler struct {
	slv  *slave.Slave
	cfg  *AppConfig
	strw *storageReaderWriter

	fromDB *sql.DB
	toDBs  []*sql.DB
}

// NewEventHandler create a event handler
func NewEventHandler(s *slave.Slave, cfg *AppConfig) *EventHandler {
	return &EventHandler{
		slv: s,
		cfg: cfg,
	}
}

// Prepare do initialize work
func (e *EventHandler) Prepare() error {
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

	// Read the position from storage
	var position mconn.Position
	err := e.strw.readPosition(&position)
	if nil != err {
		if err != errStorageKeyNotFound {
			return errors.Trace(err)
		}
	}

	// Start slave
	if err := e.slv.Start(position); nil != err {
		return errors.Trace(err)
	}

	return nil
}

// Close closes the event handler and stop the slave
func (e *EventHandler) Close() error {
	// Close the slave
	e.slv.Stop()
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
