package worker

import (
	"bytes"
	"database/sql"
	"net"
	"reflect"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/utils"
	// Import mysql driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	// Auto switch destinate DB if current db is down
	switchDBIntervalSecOnDialError = 30
)

type mysqlExecutor struct {
	dbs         []*sql.DB
	inuse       int
	lastErr     error
	lastErrTime int64
	txn         *sql.Tx
	valuesCache []interface{}
	statement   bytes.Buffer
}

func init() {
	registerExecutor("mysql", func(name string, dest *DestinationConfig) (IJobExecutor, error) {
		if len(dest.DBs) == 0 {
			return nil, errors.New("Empty db")
		}

		var executor mysqlExecutor

		dbs := make([]*sql.DB, 0, len(dest.DBs))
		for _, v := range dest.DBs {
			v.Type = "mysql"
			db, err := utils.CreateDBWithArgs(v,
				utils.WithCharset(v.Charset), utils.WithInterpolateParams(dest.Text))
			if nil != err {
				return nil, errors.Trace(err)
			}
			dbs = append(dbs, db)
		}
		if err := executor.Attach(dbs); nil != err {
			return nil, errors.Trace(err)
		}
		return &executor, nil
	})
}

func (e *mysqlExecutor) Attach(v interface{}) error {
	if nil != e.txn {
		// Transaction not complete
		return errors.New("Transaction not completed")
	}
	if nil == v {
		return errors.New("Invalid attach value")
	}

	var dbs []*sql.DB
	var ok bool
	dbs, ok = v.([]*sql.DB)
	if !ok {
		return errors.New("Invalid []*sql.DB type")
	}

	// Every db should not null
	if nil == dbs {
		return errors.New("Nil []*sql.DB")
	}
	if len(dbs) == 0 {
		return errors.New("Empty db")
	}
	for _, v := range dbs {
		if nil == v {
			return errors.New("Nil *sql.DB")
		}
	}
	// Apply new dbs if not equal
	if nil == e.dbs ||
		len(e.dbs) != len(dbs) {
		// Here we must update inuse index to avoid out of index panic
		e.dbs = dbs
		// TODO use random index
		e.inuse = 0
		return nil
	}
	equal := true
	for i := range e.dbs {
		found := false
		for j := range dbs {
			if e.dbs[i] == dbs[j] {
				found = true
				break
			}
		}
		if !found {
			equal = false
			break
		}
	}
	if equal {
		return nil
	}
	// Not equal, apply the to dbs
	e.dbs = dbs
	// Select the current inuse index by jobID
	// TODO use random index
	e.inuse = 0
	log.Infof("Update executor dest db index = %v", e.inuse)
	return nil
}

func (e *mysqlExecutor) Begin() error {
	err := e.begin()
	e.updateLastError(err)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (e *mysqlExecutor) begin() error {
	var err error
	db := e.dbs[e.inuse]
	e.txn, err = db.Begin()
	return err
}

func (e *mysqlExecutor) Exec(job *WorkerEvent) error {
	err := e.exec(job)
	e.updateLastError(err)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (e *mysqlExecutor) statementGen(job *WorkerEvent) (string, []interface{}, error) {
	e.statement.Reset()
	e.valuesCache = e.valuesCache[0:0]
	switch job.Etype {
	case WorkerEventRowInsert:
		{
			return e.insertStatementGen(job)
		}
	case WorkerEventRowUpdate:
		{
			return e.updateStatementGen(job)
		}
	case WorkerEventRowDelete:
		{
			return e.deleteStatementGen(job)
		}
	default:
		{
			return "", nil, errors.Errorf("Invalid worker event %d to generate statement", job.Etype)
		}
	}
}

func (e *mysqlExecutor) insertStatementGen(job *WorkerEvent) (string, []interface{}, error) {
	e.statement.WriteString("REPLACE INTO `")
	e.statement.WriteString(job.SDesc.RewriteSchema)
	e.statement.WriteString("`.`")
	e.statement.WriteString(job.SDesc.RewriteTable)
	e.statement.WriteString("` (")
	for i, c := range job.Columns {
		if i != 0 {
			e.statement.WriteString(", ")
		}
		e.statement.WriteString(c.Column.Name)
	}
	e.statement.WriteString(") VALUES (")
	for i, c := range job.Columns {
		if i != 0 {
			e.statement.WriteString(", ")
		}
		e.statement.WriteString("?")
		e.valuesCache = append(e.valuesCache, c.Value)
	}
	e.statement.WriteString(")")
	return e.statement.String(), e.valuesCache, nil
}

func (e *mysqlExecutor) updateStatementGen(job *WorkerEvent) (string, []interface{}, error) {
	e.statement.WriteString("UPDATE '")
	e.statement.WriteString(job.SDesc.RewriteSchema)
	e.statement.WriteString("`.`")
	e.statement.WriteString(job.SDesc.RewriteTable)
	e.statement.WriteString("`")
	e.statement.WriteString("` SET ")
	cnt := 0
	for i := range job.Columns {
		if reflect.DeepEqual(job.Columns[i].Value, job.NewColumns[i].Value) {
			continue
		}
		if 0 != cnt {
			e.statement.WriteString(", ")
		}
		e.statement.WriteString("`")
		e.statement.WriteString(job.NewColumns[i].Column.Name)
		e.statement.WriteString("`")
		e.statement.WriteString(" = ?")
		e.valuesCache = append(e.valuesCache, job.NewColumns[i].Value)
		cnt++
	}
	if 0 == cnt {
		return "", nil, errors.Errorf("Table %s.%s missing update columns",
			job.Ti.Schema, job.Ti.Name)
	}
	cnt = 0
	e.statement.WriteString(" WHERE ")
	for _, v := range job.Columns {
		if !v.Column.IsPrimary {
			continue
		}
		if 0 != cnt {
			e.statement.WriteString(" AND ")
		}
		e.statement.WriteString("`")
		e.statement.WriteString(v.Column.Name)
		e.statement.WriteString("` = ?")
		e.valuesCache = append(e.valuesCache, v.Value)
		cnt++
	}
	if 0 == cnt {
		return "", nil, errors.Errorf("Table %s.%s missing index columns",
			job.Ti.Schema, job.Ti.Name)
	}
	return e.statement.String(), e.valuesCache, nil
}

func (e *mysqlExecutor) deleteStatementGen(job *WorkerEvent) (string, []interface{}, error) {
	e.statement.WriteString("DELETE FROM '")
	e.statement.WriteString(job.SDesc.RewriteSchema)
	e.statement.WriteString("`.`")
	e.statement.WriteString(job.SDesc.RewriteTable)
	e.statement.WriteString("`")
	e.statement.WriteString("` WHERE ")
	cnt := 0
	for _, v := range job.Columns {
		if !v.Column.IsPrimary {
			continue
		}
		if 0 != cnt {
			e.statement.WriteString(" AND ")
		}
		e.statement.WriteString("`")
		e.statement.WriteString(v.Column.Name)
		e.statement.WriteString("` = ?")
		e.valuesCache = append(e.valuesCache, v.Value)
		cnt++
	}
	if 0 == cnt {
		return "", nil, errors.Errorf("Table %s.%s missing index columns",
			job.Ti.Schema, job.Ti.Name)
	}
	return e.statement.String(), e.valuesCache, nil
}

func (e *mysqlExecutor) exec(job *WorkerEvent) error {
	if nil == e.txn {
		return errors.New("Exec out of transaction")
	}
	if nil == e.valuesCache {
		e.valuesCache = make([]interface{}, 0, len(job.Columns))
	}
	stmt, values, err := e.statementGen(job)
	if nil != err {
		return errors.Trace(err)
	}
	/*if _, err = e.txn.Exec(stmt, values...); nil != err {
		return errors.Trace(err)
	}*/
	logrus.Infof("Statement %s, values %v",
		stmt, values)
	return nil
}

func (e *mysqlExecutor) Rollback() error {
	err := e.rollback()
	e.updateLastError(err)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (e *mysqlExecutor) rollback() error {
	if nil == e.txn {
		return errors.New("Rollback out of transcation")
	}
	txn := e.txn
	e.txn = nil
	return txn.Rollback()
}

func (e *mysqlExecutor) Commit() error {
	err := e.commit()
	e.updateLastError(err)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (e *mysqlExecutor) commit() error {
	if nil == e.txn {
		return errors.New("Commit out of transaction")
	}
	txn := e.txn
	e.txn = nil
	return txn.Commit()
}

func isDialError(err error) bool {
	if nil == err {
		return false
	}

	if netErr, ok := err.(*net.OpError); ok {
		if netErr.Op == "dial" {
			return true
		}
	}
	return false
}

func (e *mysqlExecutor) updateLastError(err error) {
	if !isDialError(err) ||
		!isDialError(err) {
		e.lastErrTime = 0
		e.lastErr = err
		return
	}
	if len(e.dbs) == 0 {
		// Invalid if using % operation
		return
	}
	// Need check switch db
	tn := time.Now().Unix()
	if 0 == e.lastErrTime {
		// First time dial error
		e.lastErrTime = tn
	} else if tn-e.lastErrTime > switchDBIntervalSecOnDialError {
		// Do switch
		e.inuse = (e.inuse + 1) % len(e.dbs)
		// Reset the dial switch time
		e.lastErrTime = 0
		if len(e.dbs) > 1 {
			log.Warnf("Switch dest db index %v due to dial failed", e.inuse)
		}
	}

	e.lastErr = err
}
