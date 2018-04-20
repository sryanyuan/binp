package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
)

// RowsQueryEvent sees below
// https://dev.mysql.com/doc/internals/en/rows-query-event.html
type RowsQueryEvent struct {
	QueryText string
}

// Decode decodes the binary data into payload
func (e *RowsQueryEvent) Decode(data []byte) error {
	r := utils.NewBinReader(data)
	// Skip one byte length
	_, err := r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	// Query text [string eof]
	e.QueryText, err = r.ReadEOFString()
	if nil != err {
		return errors.Trace(err)
	}
	r.End()
	return nil
}
