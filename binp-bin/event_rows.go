package main

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/binlog"
	"github.com/sryanyuan/binp/tableinfo"
	"github.com/sryanyuan/binp/worker"
)

func fillColumnValues(ti *tableinfo.TableInfo, row *binlog.Row) []*tableinfo.ColumnWithValue {
	cwvs := make([]*tableinfo.ColumnWithValue, 0, len(ti.Columns))
	for i, v := range ti.Columns {
		var cw tableinfo.ColumnWithValue
		cw.Column = v
		cw.Value = row.ColumnDatas[i]
		cwvs = append(cwvs, &cw)
	}
	return cwvs
}

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
		var job worker.WorkerEvent
		job.Etype = revt.Action
		job.Timestamp = evt.Header.Timestamp
		job.Ti = ti
		job.SDesc = revt.Rule
		// Fill row data
		job.Columns = fillColumnValues(ti, row)
		// Get event type
		if revt.Action == binlog.RowWrite {
			job.Etype = worker.WorkerEventRowInsert
		} else if revt.Action == binlog.RowUpdate {
			job.Etype = worker.WorkerEventRowUpdate
		} else if revt.Action == binlog.RowDelete {
			job.Etype = worker.WorkerEventRowDelete
		}
		if err := e.wmgr.DispatchWorkerEvent(&job, e.cfg.DispatchPolicy); nil != err {
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
