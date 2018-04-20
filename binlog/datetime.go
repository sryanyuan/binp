package binlog

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
)

const (
	datetimefIntIfs int64 = 0x8000000000
)

func decodeDatetime(r *utils.BinReader) (interface{}, error) {
	v, err := r.ReadUint64()
	if nil != err {
		return nil, errors.Trace(err)
	}
	d := v / 1000000
	t := v % 1000000

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		d/10000,
		(d%10000)/100,
		d%100,
		t/10000,
		(t%10000)/100,
		t%100), nil
}

// github.com/siddontang/go-mysql/replication/row_event.go
func decodeDatetime2(r *utils.BinReader, meta uint16) (interface{}, error) {
	bv, err := r.ReadBytes(5)
	if nil != err {
		return nil, errors.Trace(err)
	}
	intPart := int64(utils.NumberFromBytesBigEndian(bv)) - datetimefIntIfs
	var frac int64

	switch meta {
	case 1, 2:
		bv, err = r.ReadBytes(1)
		if nil != err {
			return nil, errors.Trace(err)
		}
		frac = int64(bv[0]) * 10000
	case 3, 4:
		bv, err = r.ReadBytes(2)
		if nil != err {
			return nil, errors.Trace(err)
		}
		frac = int64(utils.NumberFromBytesBigEndian(bv)) * 100
	case 5, 6:
		bv, err = r.ReadBytes(3)
		if nil != err {
			return nil, errors.Trace(err)
		}
		frac = int64(utils.NumberFromBytesBigEndian(bv))
	}

	if intPart == 0 {
		return formatZeroTime(int(frac), int(meta)), nil
	}

	tmp := intPart<<24 + frac
	// handle sign???
	if tmp < 0 {
		tmp = -tmp
	}

	// var secPart int64 = tmp % (1 << 24)
	ymdhms := tmp >> 24

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)

	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int((hms >> 12))

	tm := time.Date(year, time.Month(month), day, hour, minute, second, int(frac*1000), time.UTC)
	return tm.String(), nil
}
