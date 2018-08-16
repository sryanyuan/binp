package binlog

import (
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/rule"
	"github.com/sryanyuan/binp/serialize"
)

// Parser parse the binlog event
type Parser struct {
	tables   map[uint64]*TableMapEvent
	format   *FormatDescriptionEvent
	checksum uint8
	srule    rule.ISyncRule
}

// NewParser create a new binlog parser
func NewParser() *Parser {
	return &Parser{
		tables: make(map[uint64]*TableMapEvent),
	}
}

// Reset resets the parser
func (p *Parser) Reset() {
	p.checksum = ChecksumAlgOff
	p.format = nil
}

// SetChecksum set the checksum of server binlog
func (p *Parser) SetChecksum(cs uint8) {
	p.checksum = cs
}

// SetSyncRule set the sync rule for the parser
// Sync rule only effect rows event, ddl (query event) won't be effected
func (p *Parser) SetSyncRule(r rule.ISyncRule) {
	p.srule = r
}

// Parse parses binary data to binlog event
func (p *Parser) Parse(data []byte) (*Event, error) {
	// Skip ok header
	data = data[1:]
	// TODO: semi ack

	event, err := p.parseEvent(data)
	if nil != err {
		return nil, errors.Trace(err)
	}

	// Check events effect parse
	if event.Payload.Parsed {
		switch event.Header.EventType {
		case FormatDescriptionEventType:
			{
				p.format = event.Payload.FormatDescription
			}
		case TableMapEventType:
			{
				p.tables[event.Payload.TableMap.TableID] = event.Payload.TableMap
			}
		case WriteRowsEventV0Type, WriteRowsEventV1Type, WriteRowsEventV2Type,
			UpdateRowsEventV0Type, UpdateRowsEventV1Type, UpdateRowsEventV2Type,
			DeleteRowsEventV0Type, DeleteRowsEventV1Type, DeleteRowsEventV2Type:
			{
				if event.Payload.Rows.Flags&RowsEventFlagStmtEnd != 0 {
					//  If the table id is 0x00ffffff it is a dummy event that should have the end of statement flag set that
					// declares that all table maps can be freed. Otherwise it refers to a table defined by TABLE_MAP_EVENT.
					p.tables = make(map[uint64]*TableMapEvent)
				}
			}
		}
	} else {
		logrus.Debugf("Unparsed binlog event %d", event.Header.EventType)
	}

	return event, errors.Trace(err)
}

func (p *Parser) parseEvent(data []byte) (*Event, error) {
	var event Event
	// Parse header first
	offset, err := p.parseHeader(&event.Header, data)
	if nil != err {
		return nil, errors.Trace(err)
	}
	data = data[offset:]

	if err = p.parsePayload(&event, data); nil != err {
		return nil, errors.Trace(err)
	}
	return &event, nil
}

func (p *Parser) parseHeader(header *EventHeader, data []byte) (int, error) {
	offset := 0
	dlen := len(data)
	// Read timestamp
	if offset+3 >= dlen {
		return 0, errors.New("parse header data overflow")
	}
	header.Timestamp = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	// Read event type
	if offset >= dlen {
		return 0, errors.New("parse header data overflow")
	}
	header.EventType = data[offset]
	offset++
	// Read server id
	if offset+3 >= dlen {
		return 0, errors.New("parse header data overflow")
	}
	header.ServerID = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	// Reader event size
	if offset+3 >= dlen {
		return 0, errors.New("parse header data overflow")
	}
	header.EventSize = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	// Binlog version > 1
	// Read log pos
	if offset+3 >= dlen {
		return 0, errors.New("parse header data overflow")
	}
	header.LogPos = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	// Read flags
	if offset+1 >= dlen {
		return 0, errors.New("parse header data overflow")
	}
	header.Flags = binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	return offset, nil
}

func (p *Parser) parsePayload(event *Event, data []byte) error {
	var err error

	// If server binlog checksum is not empty, we need skip the last 4 bytes
	if p.checksum == ChecksumAlgCRC32 {
		data = data[:len(data)-4]
		// If is format event, skip the checksum type
		if event.Header.EventType == FormatDescriptionEventType {
			data = data[:len(data)-1]
		}
	}

	var payload IPayload

	switch event.Header.EventType {
	case RotateEventType:
		{
			evt := &RotateEvent{}
			event.Payload.Parsed = true
			event.Payload.Rotate = evt
			payload = evt
		}
	case FormatDescriptionEventType:
		{
			evt := &FormatDescriptionEvent{}
			event.Payload.Parsed = true
			event.Payload.FormatDescription = evt
			payload = evt
		}
	case QueryEventType:
		{
			evt := &QueryEvent{}
			event.Payload.Parsed = true
			event.Payload.Query = evt
			payload = evt
		}
	case XidEventType:
		{
			evt := &XidEvent{}
			event.Payload.Parsed = true
			event.Payload.Xid = evt
			payload = evt
		}
	case TableMapEventType:
		{
			// TODO: Get table map event from here and skip events filtered by sync rule
			evt := &TableMapEvent{}
			// Check the post header len from format description
			if nil == p.format {
				return errors.New("missing format description")
			}
			ei := TableMapEventType - 1
			if ei >= len(p.format.EventTypeHeaderLengths) {
				return errors.Errorf("incorrect index of event type header length, event type = %v, len = %v",
					event.Header.EventType, len(p.format.EventTypeHeaderLengths))
			}
			evt.tableIDSize = 6
			if p.format.EventTypeHeaderLengths[ei] == 6 {
				evt.tableIDSize = 4
			}

			event.Payload.Parsed = true
			event.Payload.TableMap = evt
			payload = evt
		}
	case WriteRowsEventV0Type, WriteRowsEventV1Type, WriteRowsEventV2Type,
		UpdateRowsEventV0Type, UpdateRowsEventV1Type, UpdateRowsEventV2Type,
		DeleteRowsEventV0Type, DeleteRowsEventV1Type, DeleteRowsEventV2Type:
		{
			evt := &RowsEvent{}
			event.Payload.Rows = evt
			event.Payload.Parsed = true

			// Find the table map event of the table
			if data, err = p.preParseRowsEvent(event, data); nil != err {
				return errors.Trace(err)
			}

			if event.Payload.Parsed {
				payload = evt
			}
		}
	case RowsQueryEventType:
		{
			evt := &RowsQueryEvent{}
			event.Payload.Parsed = true
			event.Payload.RowsQuery = evt
			payload = evt
		}
	case GTIDEventType:
		{
			evt := &GTIDEvent{}
			event.Payload.Parsed = true
			event.Payload.GTID = evt
			payload = evt
		}
	case MariadbGTIDEventType:
		{
			evt := &MariadbGTIDEvent{}
			evt.ServerID = event.Header.ServerID
			event.Payload.Parsed = true
			event.Payload.MariadbGTID = evt
			payload = evt
		}
	case HeartbeatEventType:
		{
			evt := &HeartbeatEvent{}
			event.Payload.Parsed = true
			event.Payload.Heartbeat = evt
			payload = evt
		}
	}

	if nil == payload {
		// Not parsed
		return nil
	}
	if err = payload.Decode(data); nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (p *Parser) preParseRowsEvent(event *Event, data []byte) ([]byte, error) {
	evt := event.Payload.Rows
	// Check the post header len from format description
	if nil == p.format {
		return data, errors.New("missing format description")
	}
	ei := int(event.Header.EventType) - 1
	if ei >= len(p.format.EventTypeHeaderLengths) {
		return data, errors.Errorf("incorrect index of event type header length, event type = %v, len = %v",
			event.Header.EventType, len(p.format.EventTypeHeaderLengths))
	}
	evt.tableIDSize = 6
	if p.format.EventTypeHeaderLengths[ei] == 6 {
		evt.tableIDSize = 4
	}
	// Get table map event
	r := serialize.NewBinReader(data)
	if 4 == evt.tableIDSize {
		tid, err := r.ReadUint32()
		if nil != err {
			return data, errors.Trace(err)
		}
		evt.TableID = uint64(tid)
	} else {
		tid, err := r.ReadUint48()
		if nil != err {
			return data, errors.Trace(err)
		}
		evt.TableID = uint64(tid)
	}
	tm, ok := p.tables[evt.TableID]
	if !ok {
		return data, errors.Errorf("missing table map event %d while parsing rows event",
			evt.TableID)
	}
	evt.Table = tm
	// Check sync rule if set
	if nil != p.srule {
		desc := p.srule.CanSyncTable(tm.SchemaName, tm.TableName)
		if nil == desc {
			event.Payload.Parsed = false
			return data, nil
		}
		evt.Rule = desc
	}
	// Get the rows event version
	et := event.Header.EventType
	if et >= WriteRowsEventV0Type && et <= DeleteRowsEventV0Type {
		evt.version = 0
	} else if et >= WriteRowsEventV1Type && et <= DeleteRowsEventV1Type {
		evt.version = 1
	} else {
		evt.version = 2
	}

	evt.Action = RowWrite
	if et == UpdateRowsEventV0Type ||
		et == UpdateRowsEventV1Type ||
		et == UpdateRowsEventV2Type {
		evt.Action = RowUpdate
	}
	if et == DeleteRowsEventV0Type ||
		et == DeleteRowsEventV1Type ||
		et == DeleteRowsEventV2Type {
		evt.Action = RowDelete
	}

	return r.LeftBytes(), nil
}
