package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
)

const (
	fixedEventHeaderLength = 19
)

// FormatDescriptionEvent see below
// https://dev.mysql.com/doc/internals/en/format-description-event.html
type FormatDescriptionEvent struct {
	BinlogVersion          uint16
	MysqlServerVersion     [50]byte
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte
}

// Decode decodes the binary data into payload
func (e *FormatDescriptionEvent) Decode(data []byte) error {
	var err error
	r := mconn.NewBinReader(data)

	e.BinlogVersion, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}
	v, err := r.ReadBytes(50)
	if nil != err {
		return errors.Trace(err)
	}
	copy(e.MysqlServerVersion[:], v)
	e.CreateTimestamp, err = r.ReadUint32()
	if nil != err {
		return errors.Trace(err)
	}
	e.EventHeaderLength, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	v = r.LeftBytes()
	e.EventTypeHeaderLengths = make([]byte, len(v))
	copy(e.EventTypeHeaderLengths[0:], v)

	return nil
}
