package binlog

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
)

func decodeDate(r *utils.BinReader) (interface{}, error) {
	v, err := r.ReadBytes(3)
	if nil != err {
		return nil, errors.Trace(err)
	}
	fv := utils.NumberFromBytesBigEndian(v)
	if 0 == fv {
		return "0000-00-00", nil
	}
	return fmt.Sprintf("%04d-%02d-%02d",
		fv/(16*32),
		fv/32%16,
		fv%32), nil
}
