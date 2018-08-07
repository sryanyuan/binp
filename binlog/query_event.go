package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

// QueryEvent sees below
// https://dev.mysql.com/doc/internals/en/query-event.html
type QueryEvent struct {
	// Post header
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVarsLength uint16
	// Payload
	StatusVars string
	Schema     string
	unused     uint8
	Query      string
}

// Decode decodes the binary data into payload
func (e *QueryEvent) Decode(data []byte) error {
	r := serialize.NewBinReader(data)
	var err error

	e.SlaveProxyID, err = r.ReadUint32()
	if nil != err {
		return errors.Trace(err)
	}
	e.ExecutionTime, err = r.ReadUint32()
	if nil != err {
		return errors.Trace(err)
	}
	e.SchemaLength, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	e.ErrorCode, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}
	e.StatusVarsLength, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}

	e.StatusVars, err = r.ReadStringWithLen(int(e.StatusVarsLength))
	if nil != err {
		return errors.Trace(err)
	}
	e.Schema, err = r.ReadStringWithLen(int(e.SchemaLength))
	if nil != err {
		return errors.Trace(err)
	}
	_, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	e.Query, err = r.ReadEOFString()
	if nil != err {
		return errors.Trace(err)
	}
	r.End()

	return nil
}
