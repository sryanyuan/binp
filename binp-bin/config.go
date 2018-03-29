package main

import "github.com/sryanyuan/binp/slave"

type AppConfig struct {
	DataSource slave.ReplicationConfig `json:"data-source" toml:"data-source"`
	Log        LogConfig               `json:"log" toml:"log"`
}
