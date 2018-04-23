package main

import (
	"context"
	"time"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/binlog"
	"github.com/sryanyuan/binp/slave"
)

// EventHandler receives the binlog from master and handle it
type EventHandler struct {
	slv *slave.Slave
}

// NewEventHandler create a event handler
func NewEventHandler(s *slave.Slave) *EventHandler {
	return &EventHandler{
		slv: s,
	}
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
		}
	}
	return nil
}
