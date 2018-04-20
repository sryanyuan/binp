package binlog

import (
	"time"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
)

const (
	datetimeMaxDecimals = 6
)

// my_time.cc
func decodeTimestamp2(r *utils.BinReader, dec uint16) (interface{}, error) {
	if dec > datetimeMaxDecimals {
		return nil, errors.Errorf("invalid timestamp2 decimal %d", dec)
	}
	bv, err := r.ReadBytes(4)
	if nil != err {
		return nil, errors.Trace(err)
	}
	sec := int64(utils.NumberFromBytesBigEndian(bv))
	usec := int64(0)
	switch dec {
	case 0:
	case 1, 2:
		{
			// Read next 1 byte
			nv, err := r.ReadUint8()
			if nil != err {
				return nil, errors.Trace(err)
			}
			usec = int64(nv) * 10000
		}
	case 3, 4:
		{
			v, err := r.ReadBytes(2)
			if nil != err {
				return nil, errors.Trace(err)
			}
			nv := utils.NumberFromBytesBigEndian(v)
			usec = int64(nv) * 100
		}
	case 5, 6:
		{
			v, err := r.ReadBytes(3)
			if nil != err {
				return nil, errors.Trace(err)
			}
			nv := utils.NumberFromBytesBigEndian(v)
			usec = int64(nv)
		}
	}

	if 0 == sec {
		return formatZeroTime(int(usec), int(dec)), nil
	}

	tm := time.Unix(sec, usec)
	return tm.String(), nil
}
