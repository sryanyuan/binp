package main

import "github.com/sryanyuan/binp/slave"

// AppConfig is the config of binp
type AppConfig struct {
	DataSource slave.ReplicationConfig `json:"data-source" toml:"data-source"`
	Log        LogConfig               `json:"log" toml:"log"`
}
