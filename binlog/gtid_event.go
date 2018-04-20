package binlog

import (
	"fmt"

	"github.com/juju/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sryanyuan/binp/utils"
)

// GTIDEvent sees below
// rpl_gtid.h
type GTIDEvent struct {
	CommitFlag uint8
	SID        []byte
	GNO        int64
}

// Decode decodes the binary data into payload
func (e *GTIDEvent) Decode(data []byte) error {
	r := utils.NewBinReader(data)
	var err error
	e.CommitFlag, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	e.SID, err = r.ReadBytes(16)
	if nil != err {
		return errors.Trace(err)
	}
	e.GNO, err = r.ReadInt64()
	if nil != err {
		return errors.Trace(err)
	}
	r.End()
	return nil
}

func (e *GTIDEvent) String() string {
	u, err := uuid.FromBytes(e.SID)
	if nil != err {
		return fmt.Sprintf("N/A: %s", err.Error())
	}
	return fmt.Sprintf("%s:1-%d", u.String(), e.GNO)
}
