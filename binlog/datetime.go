package binlog

import (
	"time"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

const (
	datetimefIntIfs int64 = 0x8000000000
)

func decodeDatetime(r *serialize.BinReader) (interface{}, error) {
	v, err := r.ReadUint64()
	if nil != err {
		return nil, errors.Trace(err)
	}
	if v == 0 {
		return "0000-00-00 00:00:00", nil
	}
	d := v / 1000000
	t := v % 1000000

	return formatTimeWithDecimals(time.Date(int(d/10000),
		time.Month((d%10000)/100),
		int(d%100),
		int(t/10000),
		int((t%10000)/100),
		int(t%100),
		0,
		time.UTC), 0), nil
}

// github.com/siddontang/go-mysql/replication/row_event.go
func decodeDatetime2(r *serialize.BinReader, meta uint16) (interface{}, error) {
	bv, err := r.ReadBytes(5)
	if nil != err {
		return nil, errors.Trace(err)
	}
	intPart := int64(serialize.NumberFromBytesBigEndian(bv)) - datetimefIntIfs
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
		frac = int64(serialize.NumberFromBytesBigEndian(bv)) * 100
	case 5, 6:
		bv, err = r.ReadBytes(3)
		if nil != err {
			return nil, errors.Trace(err)
		}
		frac = int64(serialize.NumberFromBytesBigEndian(bv))
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
	return formatTimeWithDecimals(tm, int(meta)), nil
}
