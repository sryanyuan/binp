package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

// XidEvent sees below
// https://dev.mysql.com/doc/internals/en/xid-event.html
type XidEvent struct {
	Xid uint64
}

// Decode decodes the binary data into payload
func (e *XidEvent) Decode(data []byte) error {
	var err error
	r := serialize.NewBinReader(data)

	e.Xid, err = r.ReadUint64()
	if nil != err {
		return errors.Trace(err)
	}

	return nil
}
