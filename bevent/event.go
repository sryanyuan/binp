package bevent

const (
	_ = iota
	// EventTypeError represents the BEvent holds an error
	EventTypeError
)

// BinlogEvent represents a binlog event
type BinlogEvent struct {
	// Type represents the event type
	Type int
	// Err has value if Type == EventTypeError
	Err error
}
