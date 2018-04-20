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
	if "" == d.Table {
		return errors.Trace(ErrInvalidSyncDescTable)
	}
	return nil
}

// ISyncRule defines a rule which database and table can be synchronized
type ISyncRule interface {
	// schema and table, returns rewrite schema and table name
	CanSyncTable(string, string) (string, string, bool)
	// insert new schema rule
	NewRule(*SyncDesc) error
}
