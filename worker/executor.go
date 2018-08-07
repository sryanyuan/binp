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
	Etype     int // Insert/Update/Delete
	Timestamp uint32
	Point     mconn.ReplicationPoint
	Ti        *tableinfo.TableInfo
	Columns   []*tableinfo.ColumnWithValue
	SDesc     *rule.SyncDesc
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

/*func executorFromDBConfig(dcs []mconn.DBConfig) (IJobExecutor, error) {
	if nil == dcs || 0 == len(dcs) {
		return nil, errors.New("Invalid executor input argument number")
	}
	// Every executor type must be same
	dtype := ""
	for i := range dcs {
		dc := &dcs[i]
		if "" == dtype {
			dtype = strings.ToLower(dc.Type)
		} else {
			if dtype != dc.Type {
				return nil, errors.New("Executor type must be same")
			}
		}
	}
	// Only mysql executor support multi db
	if dtype != "mysql" && len(dcs) > 1 {
		return nil, errors.New("Only mysql executor support multi destination")
	}

	if dtype == "mysql" {
		var me mysqlExecutor

		dbs := make([]*sql.DB, 0, len(dcs))
		for i := range dcs {
			dc := &dcs[i]
			db, err := utils.CreateDB(dc)
			if nil != err {
				return nil, errors.Trace(err)
			}
			dbs = append(dbs, db)
		}

		if err := me.Attach(dbs); nil != err {
			return nil, errors.Trace(err)
		}

		return &me, nil
	}

	return nil, errors.Errorf("Unknown executor type %s", dtype)
}
*/
