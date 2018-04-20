package binlog

// Event type of binlog event
// https://dev.mysql.com/doc/internals/en/binlog-event-type.html
const (
	_ = iota
	// A start event is the first event of a binlog for binlog-version 1 to 3.
	StartEventV3Type
	QueryEventType
	StopEventType
	RotateEventType
	IntvarEventType
	LoadEventType
	SlaveEventType
	CreateFileEventType
	AppendBlockEventType
	ExecLoadEventType
	DeleteFileEventType
	NewLoadEventType
	RandEventType
	UserVarEventType
	// A format description event is the first event of a binlog for binlog-version 4.
	// It describes how the other events are layed out.
	FormatDescriptionEventType
	XidEventType
	BeginLoadQueryEventType
	ExecuteLoadQueryEventType
	TableMapEventType
	WriteRowsEventV0Type
	UpdateRowsEventV0Type
	DeleteRowsEventV0Type
	WriteRowsEventV1Type
	UpdateRowsEventV1Type
	DeleteRowsEventV1Type
	IncidentEventType
	HeartbeatEventType
	IgnoreableEventType
	RowsQueryEventType
	WriteRowsEventV2Type
	UpdateRowsEventV2Type
	DeleteRowsEventV2Type
	GTIDEventType
	AnonymousGtidEventType
	PreviousGtidsEventType
)

// MariaDB binlog events
const (
	MariadbAnnotateRowsEventType = 160 + iota
	MariadbBinlogCheckpointEventType
	MariadbGTIDEventType
	MariadbGTIDListEventType
)

// Flags of binlog event header's flag
// https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
const (
	LogEventBinlogInUseFlag = 1 << iota
	LogEventForcedRotateFlag
	LogEventThreadSpecificFlag
	LogEventSuppressUseFlag
	LogEventUpdateTableMapVersionFlag
	LogEventArtificialFlag
	LogEventRelayLogFlag
	LogEventIgnorableFlag
	LogEventNoFilterFlag
	LogEventMtsIsolateFlag
)

// Binlog checksum
const (
	ChecksumAlgOff   = 0x00
	ChecksumAlgCRC32 = 0x01
	ChecksumAlgUndef = 0xff
)

// EventHeader is the header of a binlog event
// https://dev.mysql.com/doc/internals/en/binlog-event-header.html
type EventHeader struct {
	// Timestamp seconds since unix epoch
	Timestamp uint32
	// EventType https://dev.mysql.com/doc/internals/en/binlog-event-type.html
	EventType uint8
	// ServerID
	// server-id of the originating mysql-server. Used to filter out events in circular replication.
	ServerID uint32
	// EventSize size of the event (header, post-header, body)
	EventSize uint32
	// LogPos  position of the next event
	LogPos uint32
	// Flags
	// https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
	Flags uint16
}

// EventSet has all event type
type EventSet struct {
	// Parsed is true when the event is parsed
	Parsed bool
	// FormatDescription event
	FormatDescription *FormatDescriptionEvent
	// Rotate event
	Rotate *RotateEvent
	// Query event
	Query *QueryEvent
	// Xid event
	Xid *XidEvent
	// Table map event
	TableMap *TableMapEvent
	// Rows event
	Rows *RowsEvent
	// Rows query event
	RowsQuery *RowsQueryEvent
	// Gtid event
	GTID *GTIDEvent
	// Mariadb gtid event
	MariadbGTID *MariadbGTIDEvent
}

// Decode decodes binary data to a binlog event
func (s *EventSet) Decode(tp int, tables map[int]*TableMapEvent, data []byte) error {
	return nil
}

// Event is a binlog event send from mysql
type Event struct {
	// Data is the original binlog data
	Data    []byte
	Header  EventHeader
	Payload EventSet
}

// IPayload defines a binlog payload
type IPayload interface {
	Decode([]byte) error
}
