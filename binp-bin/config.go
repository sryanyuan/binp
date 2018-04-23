package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
	"github.com/sryanyuan/binp/rule"
)

// AppConfig is the config of binp
type AppConfig struct {
	DataSource mconn.ReplicationConfig `json:"data-source" toml:"data-source"`
	Log        LogConfig               `json:"log" toml:"log"`
	SRule      rule.DefaultSyncConfig  `json:"sync-rule" toml:"sync-rule"`
}

func (c *AppConfig) fromJSONFile(path string) error {
	f, err := os.Open(path)
	if nil != err {
		return errors.Trace(err)
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if nil != err {
		return errors.Trace(err)
	}

	err = json.Unmarshal(data, c)
	if nil != err {
		return errors.Trace(err)
	}

	return nil
}
