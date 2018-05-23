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
	case binlog.WriteRowsEventV0Type, binlog.WriteRowsEventV1Type, binlog.WriteRowsEventV2Type:
		{
			if err := e.onWriteRowsEvent(evt); nil != err {
				return errors.Trace(err)
			}
		}
	case binlog.UpdateRowsEventV0Type, binlog.UpdateRowsEventV1Type, binlog.UpdateRowsEventV2Type:
		{
			if err := e.onUpdateRowsEvent(evt); nil != err {
				return errors.Trace(err)
			}
		}
	case binlog.DeleteRowsEventV0Type, binlog.DeleteRowsEventV1Type, binlog.DeleteRowsEventV2Type:
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
	return nil
}

func (e *EventHandler) onUpdateRowsEvent(evt *binlog.Event) error {
	return nil
}

func (e *EventHandler) onDeleteRowsEvent(evt *binlog.Event) error {
	return nil
}
