package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

func decodeBit(r *serialize.BinReader, bits uint16, length int) (uint64, error) {
	var value uint64

	if bits <= 1 {
		if 1 != length {
			return 0, errors.Errorf("invalid bit length %v", length)
		}
		vd, err := r.ReadUint8()
		if nil != err {
			return 0, errors.Trace(err)
		}
		value = uint64(vd)
		return value, nil
	}

	vd, err := r.ReadBytes(length)
	if nil != err {
		return 0, errors.Trace(err)
	}
	return serialize.NumberFromBytesBigEndian(vd), nil
}
