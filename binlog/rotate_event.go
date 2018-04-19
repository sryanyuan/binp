package binlog

import (
	"github.com/sryanyuan/binp/mconn"

	"github.com/juju/errors"
)

// RotateEvent see below
// https://dev.mysql.com/doc/internals/en/rotate-event.html
type RotateEvent struct {
	Position uint64
	NextName string
}

// Decode decodes the binary data into payload
func (e *RotateEvent) Decode(data []byte) error {
	r := mconn.NewBinReader(data)
	var err error

	e.Position, err = r.ReadUint64()
	if nil != err {
		return errors.Trace(err)
	}
	e.NextName, err = r.ReadEOFString()
	if nil != err {
		return errors.Trace(err)
	}
	r.End()

	return nil
}
