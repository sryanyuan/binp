package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
)

func decodeJSON(r *utils.BinReader, meta uint16) (interface{}, error) {
	l, err := r.ReadUint16()
	if nil != err {
		return nil, errors.Trace(err)
	}
	return readBlob(r, int(l))
}
