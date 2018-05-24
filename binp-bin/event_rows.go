package main

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/binlog"
)

func (e *EventHandler) onRowsEvent(evt *binlog.Event) error {
	revt := evt.Payload.Rows
	if nil == revt {
		return errors.New("Nil rows payload")
	}

	switch revt.Action {
	case binlog.RowWrite:
		{
			if err := e.onWriteRowsEvent(evt); nil != err {
				return errors.Trace(err)
			}
		}
	case binlog.RowUpdate:
		{
			if err := e.onUpdateRowsEvent(evt); nil != err {
				return errors.Trace(err)
			}
		}
	case binlog.RowDelete:
		{
			if err := e.onDeleteRowsEvent(evt); nil != err {
				return errors.Trace(err)
			}
		}
	default:
		{
			return errors.Errorf("Unknown rows event type %d", revt.Action)
		}
	}
	return nil
}

func (e *EventHandler) onWriteRowsEvent(evt *binlog.Event) error {
	revt := evt.Payload.Rows
	ti, err := e.getTable(revt.Table.SchemaName, revt.Table.TableName, revt.Rule)
	if nil != err {
		return errors.Trace(err)
	}

	for _, row := range revt.Rows {
		var job execJob
		job.action = revt.Action
		job.timestamp = evt.Header.Timestamp
		job.row = row
		job.table = ti
		if err := e.wmgr.dispatchJob(&job); nil != err {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *EventHandler) onUpdateRowsEvent(evt *binlog.Event) error {
	return nil
}

func (e *EventHandler) onDeleteRowsEvent(evt *binlog.Event) error {
	return nil
}
