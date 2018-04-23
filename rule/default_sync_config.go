package rule

import (
	"encoding/json"

	"github.com/juju/errors"
)

type defaultTableConfig struct {
	Rewrite   string   `json:"rewrite" toml:"rewrite"`
	IndexKeys []string `json:"index-keys" toml:"index-keys"`
}

type defaultDatabaseConfig struct {
	Rewrite string                         `json:"rewrite" toml:"rewrite"`
	Tables  map[string]*defaultTableConfig `json:"tables"`
}

// DefaultSyncConfig is a default sync config
type DefaultSyncConfig struct {
	Databases map[string]*defaultDatabaseConfig `json:"databases"`
}

// NewDefaultSyncConfig creates a new DefaultSyncConfig
func NewDefaultSyncConfig() *DefaultSyncConfig {
	return &DefaultSyncConfig{}
}

// ParseJSON parses the json data to SyncDesc slice to create DefaultSyncRule
func (c *DefaultSyncConfig) ParseJSON(data []byte) ([]*SyncDesc, error) {
	if nil == data ||
		0 == len(data) {
		return nil, nil
	}
	var err error
	err = json.Unmarshal(data, c)
	if nil != err {
		return nil, errors.Trace(err)
	}

	sds, err := c.ToSyncDescs()
	if nil != err {
		return nil, errors.Trace(err)
	}
	return sds, nil
}

// ToSyncDescs convert self to SyncDesc slice
func (c *DefaultSyncConfig) ToSyncDescs() ([]*SyncDesc, error) {
	if nil == c.Databases {
		return nil, nil
	}

	sds := make([]*SyncDesc, 0, 32)
	for dbName, db := range c.Databases {
		if nil == db {
			// All sync
			var desc SyncDesc
			desc.Schema = dbName
			desc.RewriteSchema = dbName
			sds = append(sds, &desc)
			continue
		}
		if nil == db.Tables {
			// All sync
			var desc SyncDesc
			desc.Schema = dbName
			desc.RewriteSchema = db.Rewrite
			sds = append(sds, &desc)
			continue
		}
		for tblName, tbl := range db.Tables {
			var desc SyncDesc
			desc.Schema = dbName
			desc.RewriteSchema = db.Rewrite
			desc.Table = tblName
			if nil != tbl {
				desc.RewriteTable = tbl.Rewrite
				desc.IndexKeys = tbl.IndexKeys
			}
			sds = append(sds, &desc)
		}
	}

	return sds, nil
}
