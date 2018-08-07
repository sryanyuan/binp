package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

// HeartbeatEvent is sent by master if no more binlog produce
// See control_events.h
type HeartbeatEvent struct {
	LogIdent string
}

// Decode decodes the binary data into payload
func (e *HeartbeatEvent) Decode(data []byte) error {
	r := serialize.NewBinReader(data)
	var err error
	e.LogIdent, err = r.ReadEOFString()
	if nil != err {
		return errors.Trace(err)
	}
	r.End()
	return nil
}
