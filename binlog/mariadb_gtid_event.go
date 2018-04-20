package binlog

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
)

// MariadbGTIDEvent no document ...
type MariadbGTIDEvent struct {
	DomainID       uint32
	ServerID       uint32
	SequenceNumber uint32
}

// Decode decodes the binary data into payload
func (e *MariadbGTIDEvent) Decode(data []byte) error {
	var err error
	r := utils.NewBinReader(data)

	e.SequenceNumber, err = r.ReadUint32()
	if nil != err {
		return errors.Trace(err)
	}
	e.DomainID, err = r.ReadUint32()
	if nil != err {
		return errors.Trace(err)
	}

	r.End()

	return nil
}

func (e *MariadbGTIDEvent) String() string {
	if e.DomainID == 0 &&
		e.ServerID == 0 &&
		e.SequenceNumber == 0 {
		return ""
	}

	return fmt.Sprintf("%d-%d-%d", e.DomainID, e.ServerID, e.SequenceNumber)
}
