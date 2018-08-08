package worker

import "github.com/sryanyuan/binp/mconn"

// WorkerConfig is worker config
type WorkerConfig struct {
	WorkerCount int                 `json:"worker-count" toml:"worker-count"`
	Tos         []DestinationConfig `json:"tos" toml:"tos"` // Multi destination output
}

// DestinationConfig is the data final destination config
type DestinationConfig struct {
	Name string
	/*
		Database destination fields
		Current support database: mysql or other mysql protocol compatible database (tidb ...)
	*/
	// Backup database connection
	DBs  []*mconn.DBConfig `json:"dbs" toml:"dbs"`
	Text bool              `json:"text" toml:"text"`
}
