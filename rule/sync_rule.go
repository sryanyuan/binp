package rule

import (
	"github.com/juju/errors"
)

// SyncDesc errors
var (
	ErrInvalidSyncDescSchema = errors.New("invalid sync desc schema")
	ErrInvalidSyncDescTable  = errors.New("invalid sync desc table")
)

// SyncDesc describe a sync rule
type SyncDesc struct {
	Schema        string
	Table         string
	RewriteSchema string
	RewriteTable  string
	IndexKeys     []string
}

// Validate check if the sync desc is valid
func (d *SyncDesc) Validate() error {
	if "" == d.Schema {
		return errors.Trace(ErrInvalidSyncDescSchema)
	}
	return nil
}

// ISyncRule defines a rule which database and table can be synchronized
// Sync rule must be thread safe, it will be used in binlog parse thread
// and user worker threads. So NewRule must not be invoked once the worker
// is running
type ISyncRule interface {
	// schema and table, returns rewrite schema and table name
	CanSyncTable(string, string) *SyncDesc
	// insert new schema rule
	NewRule(*SyncDesc) error
}
