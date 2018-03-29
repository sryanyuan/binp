package slave

// Position represents a binlog replication position, slave can
// start sync with the position
type Position struct {
	Filename string
	Offset   uint32
	Gtid     string
}
