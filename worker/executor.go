package worker

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
	"github.com/sryanyuan/binp/rule"
	"github.com/sryanyuan/binp/tableinfo"
)

// WorkerEvent type
const (
	WorkerEventRowInsert = iota
	WorkerEventRowUpdate
	WorkerEventRowDelete
	WorkerEventDDL
)

// WorkerEvent holds the job IOutDest need to output
type WorkerEvent struct {
	Etype      int // Insert/Update/Delete
	Timestamp  uint32
	Point      mconn.ReplicationPoint
	Ti         *tableinfo.TableInfo
	Columns    []*tableinfo.ColumnWithValue
	NewColumns []*tableinfo.ColumnWithValue
	SDesc      *rule.SyncDesc
}

// IJobExecutor define the interface of output destination
type IJobExecutor interface {
	Attach(interface{}) error
	Begin() error
	Exec(*WorkerEvent) error
	Rollback() error
	Commit() error
}

func createExecutor(dest *DestinationConfig) (IJobExecutor, error) {
	// Create database executor
	exec, err := createExecutorByName(dest.Name, dest)
	if nil != err {
		return nil, errors.Trace(err)
	}
	return exec, nil
}

func createExecutors(dests []DestinationConfig) ([]IJobExecutor, error) {
	execs := make([]IJobExecutor, 0, len(dests))
	for i := range dests {
		dest := &dests[i]
		exec, err := createExecutor(dest)
		if nil != err {
			return nil, errors.Trace(err)
		}
		execs = append(execs, exec)
	}
	return execs, nil
}
