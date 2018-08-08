package utils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
)

// CreateDBOpFn set the option's field
type CreateDBOpFn func(*CreateDBOp)

// CreateDBOp holds all optional fields
type CreateDBOp struct {
	set               bool
	charset           *string
	interpolateParams *bool
	timeout           *int
}

func (op *CreateDBOp) applyOpts(ops ...CreateDBOpFn) {
	for _, v := range ops {
		v(op)
	}
}

func (op *CreateDBOp) format() string {
	if !op.set {
		return ""
	}
	options := "?"
	prevs := 0
	if nil != op.charset {
		options += "charset=" + *op.charset
		prevs++
	}
	if nil != op.interpolateParams {
		if prevs != 0 {
			options += "&"
		}
		flag := "false"
		if *op.interpolateParams {
			flag = "true"
		}
		options += "interpolateParams=" + flag
		prevs++
	}
	if nil != op.timeout {
		if prevs != 0 {
			options += "&"
		}
		options += fmt.Sprintf("timeout=%ds", *op.timeout)
		prevs++
	}
	return options
}

// WithCharset set the charset to the connection
func WithCharset(charset string) CreateDBOpFn {
	return func(op *CreateDBOp) {
		if "" == charset {
			return
		}
		op.charset = &charset
		op.set = true
	}
}

// WithTimeout set the timeout to the connetion
func WithTimeout(timeout int) CreateDBOpFn {
	return func(op *CreateDBOp) {
		if 0 == timeout {
			return
		}
		op.timeout = &timeout
		op.set = true
	}
}

// WithInterpolateParams set text generate mode to the connection
func WithInterpolateParams(enable bool) CreateDBOpFn {
	return func(op *CreateDBOp) {
		if !enable {
			return
		}
		op.interpolateParams = &enable
		op.set = true
	}
}

// CreateDBWithArgs create a database connection with args
func CreateDBWithArgs(dc *mconn.DBConfig, ops ...CreateDBOpFn) (*sql.DB, error) {
	var op CreateDBOp
	op.applyOpts(ops...)

	dtype := strings.ToLower(dc.Type)
	switch dtype {
	case "mysql":
		{
			dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", dc.Username, dc.Password, dc.Host, dc.Port)
			dsn += op.format()
			db, err := sql.Open(dtype, dsn)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return db, nil
		}
	default:
		{
			return nil, errors.Errorf("Unknown to type %s", dtype)
		}
	}
}

// CreateDB create a database connection with default args
func CreateDB(dc *mconn.DBConfig) (*sql.DB, error) {
	return CreateDBWithArgs(dc)
}
