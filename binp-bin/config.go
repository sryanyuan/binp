package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
	"github.com/sryanyuan/binp/rule"
)

// AppConfig is the config of binp
type AppConfig struct {
	DataSources []mconn.DataSource      `json:"data-sources" toml:"data-sources"`
	Replication mconn.ReplicationConfig `json:"replication" toml:"replication"`
	Tos         []mconn.DBConfig        `json:"tos" toml:"tos"`
	WorkerCount int                     `json:"worker-count" toml:"worker-count"`
	Log         LogConfig               `json:"log" toml:"log"`
	SRule       rule.DefaultSyncConfig  `json:"sync-rule" toml:"sync-rule"`
	// Storage source, support local (start with prefix ls: )
	StorageSource string `json:"storage-source" toml:"storage-source"`
}

func (c *AppConfig) fromFile(cpath string) error {
	f, err := os.Open(cpath)
	if nil != err {
		return errors.Trace(err)
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if nil != err {
		return errors.Trace(err)
	}

	// Get config file extension
	ext := path.Ext(cpath)
	if strings.ToLower(ext) == ".toml" {
		err = c.fromTOMLBinary(data)
	} else {
		err = c.fromJSONBinary(data)
	}

	if nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (c *AppConfig) fromJSONBinary(data []byte) error {
	err := json.Unmarshal(data, c)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (c *AppConfig) fromTOMLBinary(data []byte) error {
	_, err := toml.Decode(string(data), c)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}
