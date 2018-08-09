package main

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/binlog"
	"github.com/sryanyuan/binp/tableinfo"
	"github.com/sryanyuan/binp/worker"
)

func (e *EventHandler) onRowsEvent(evt *binlog.Event) error {
	revt := evt.Payload.Rows
	if nil == revt {
		return errors.New("Nil rows payload")
	}
	// Get table info and check binlog meta and table info
	ti, err := e.getTable(revt.Table.SchemaName, revt.Table.TableName, revt.Rule)
	if nil != err {
		return errors.Trace(err)
	}
	if len(ti.Columns) < int(revt.ColumnCount) {
		// Table info columns is less than binlog columns count
		// Force get table info again and check if new table info
		// is valid
		e.cleanTable(revt.Table.SchemaName, revt.Table.TableName)
		ti, err = e.getTable(revt.Table.SchemaName, revt.Table.TableName, revt.Rule)
		if nil != err {
			return errors.Trace(err)
		}
		if len(ti.Columns) < int(revt.ColumnCount) {
			return errors.Errorf("%s.%s: Invalid table information, table columns count is %d, but binlog columns count is %d",
				revt.Table.SchemaName, revt.Table.TableName, len(ti.Columns), int(revt.ColumnCount))
		}
	}

	for i := 0; i < len(revt.Rows); /* Determined by row event type */ {
		var job worker.WorkerEvent
		job.Etype = revt.Action
		job.Timestamp = evt.Header.Timestamp
		job.Ti = ti
		job.SDesc = revt.Rule
		// Fill row data
		job.Columns = tableinfo.FillColumnsWithValue(ti, revt.Rows[i].ColumnDatas)
		if revt.Action == binlog.RowUpdate {
			job.NewColumns = tableinfo.FillColumnsWithValue(ti, revt.Rows[i+1].ColumnDatas)
		}
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
		if revt.Action == binlog.RowUpdate {
			i += 2
		} else {
			i++
		}
	}
	return nil
}
