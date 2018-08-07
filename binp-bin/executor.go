package main

import (
	"github.com/sryanyuan/binp/binlog"
	"github.com/sryanyuan/binp/mconn"
)

// execJob holds the job IOutDest need to output
type execJob struct {
	action    int // Insert/Update/Delete
	timestamp uint32
	pos       mconn.ReplicationPoint
	table     *TableInfo
	row       *binlog.Row
}

// IJobExecutor define the interface of output destination
type IJobExecutor interface {
	Attach(interface{}) error
	Begin() error
	Exec(*execJob) error
	Rollback() error
	Commit() error
}
