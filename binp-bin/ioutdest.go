package main

// execJob holds the job IOutDest need to output
type execJob struct {
}

// IOutDest define the interface of output destination
type IOutDest interface {
	Attach(interface{}) error
	Begin() error
	Exec() error
	Rollback() error
	Commit() error
}
