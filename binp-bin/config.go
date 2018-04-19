package main

import "github.com/sryanyuan/binp/mconn"

// AppConfig is the config of binp
type AppConfig struct {
	DataSource mconn.ReplicationConfig `json:"data-source" toml:"data-source"`
	Log        LogConfig               `json:"log" toml:"log"`
}
