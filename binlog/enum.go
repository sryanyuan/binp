package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
)

func decodeEnum(r *utils.BinReader, meta uint16) (interface{}, error) {
	flag := meta & 0xff
	switch flag {
	case 1:
		{
			v, err := r.ReadUint8()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return int32(v), nil
		}
	case 2:
		{
			bv, err := r.ReadBytes(2)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return int32(utils.NumberFromBytesBigEndian(bv)), nil
		}
	default:
		{
			return nil, errors.Errorf("Unknown ENUM packlen=%d", flag)
		}
	}
}
