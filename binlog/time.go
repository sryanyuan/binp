package binlog

import (
	"encoding/binary"
	"fmt"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

const (
	timefOfs int64 = 0x800000000000
)

func formatZeroTime(frac int, dec int) string {
	if dec == 0 {
		return "0000-00-00 00:00:00"
	}

	s := fmt.Sprintf("0000-00-00 00:00:00.%06d", frac)

	// dec must < 6, if frac is 924000, but dec is 3, we must output 924 here.
	return s[0 : len(s)-(6-dec)]
}

func decodeTime(r *serialize.BinReader) (interface{}, error) {
	v, err := r.ReadUint32()
	if nil != err {
		return nil, errors.Trace(err)
	}
	return fmt.Sprintf("%02d:%02d:%02d",
		v/10000,
		(v%10000)/100,
		v%100), nil
}

// github.com/siddontang/go-mysql/replication/row_event.go
func decodeTime2(r *serialize.BinReader, meta uint16) (interface{}, error) {
	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)
	switch meta {
	case 1, 2:
		v, err := r.ReadBytes(3)
		if nil != err {
			return nil, errors.Trace(err)
		}
		intPart = int64(serialize.NumberFromBytesBigEndian(v)) - timefOfs
		v, err = r.ReadBytes(1)
		if nil != err {
			return nil, errors.Trace(err)
		}
		frac = int64(v[0])
		if intPart < 0 && frac > 0 {
			/*
			   Negative values are stored with reverse fractional part order,
			   for binary sort compatibility.

			     Disk value  intpart frac   Time value   Memory value
			     800000.00    0      0      00:00:00.00  0000000000.000000
			     7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
			     7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
			     7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
			     7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
			     7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

			     Formula to convert fractional part from disk format
			     (now stored in "frac" variable) to absolute value: "0x100 - frac".
			     To reconstruct in-memory value, we shift
			     to the next integer value and then substruct fractional part.
			*/
			intPart++     /* Shift to the next integer value */
			frac -= 0x100 /* -(0x100 - frac) */
		}
		tmp = intPart<<24 + frac*10000
	case 3, 4:
		v, err := r.ReadBytes(3)
		if nil != err {
			return nil, errors.Trace(err)
		}
		intPart = int64(serialize.NumberFromBytesBigEndian(v)) - timefOfs
		v, err = r.ReadBytes(2)
		if nil != err {
			return nil, errors.Trace(err)
		}
		frac = int64(binary.BigEndian.Uint16(v))
		if intPart < 0 && frac > 0 {
			/*
			   Fix reverse fractional part order: "0x10000 - frac".
			   See comments for FSP=1 and FSP=2 above.
			*/
			intPart++       /* Shift to the next integer value */
			frac -= 0x10000 /* -(0x10000-frac) */
		}
		tmp = intPart<<24 + frac*100
	case 5, 6:
		v, err := r.ReadBytes(6)
		if nil != err {
			return nil, errors.Trace(err)
		}
		tmp = int64(serialize.NumberFromBytesBigEndian(v)) - timefOfs
		return TimeStringFromInt64TimePacked(tmp), nil
	default:
		v, err := r.ReadBytes(3)
		if nil != err {
			return nil, errors.Trace(err)
		}
		intPart = int64(serialize.NumberFromBytesBigEndian(v)) - timefOfs
		tmp = intPart << 24
	}

	if intPart == 0 {
		return "00:00:00", nil
	}

	return TimeStringFromInt64TimePacked(tmp), nil
}

// TimeStringFromInt64TimePacked see log_event.cc TIME_from_longlong_time_packed
func TimeStringFromInt64TimePacked(tm int64) string {
	hms := int64(0)
	sign := ""
	if tm < 0 {
		tm = -tm
		sign = "-"
	}

	hms = tm >> 24

	hour := (hms >> 12) % (1 << 10) /* 10 bits starting at 12th */
	minute := (hms >> 6) % (1 << 6) /* 6 bits starting at 6th   */
	second := hms % (1 << 6)        /* 6 bits starting at 0th   */
	secPart := tm % (1 << 24)

	if secPart != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, secPart)
	}

	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second)
}
