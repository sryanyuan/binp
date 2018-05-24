package main

import (
	"database/sql"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"

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

func (e *mysqlExecutor) Exec(job *execJob) error {
	err := e.exec(job)
	e.updateLastError(err)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (e *mysqlExecutor) exec(job *execJob) error {
	if nil == e.txn {
		return errors.New("Exec out of transaction")
	}
	stmt := ""
	_, err := e.txn.Exec(stmt, job.row.ColumnDatas...)
	return err
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
