package binlog

import (
	"bytes"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
)

const (
	digPerDec1 = 9
)

var (
	dig2bytes = [digPerDec1 + 1]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
)

func decimalBinSize(precision int, scale int) int {
	intg := precision - scale
	intg0 := intg / digPerDec1
	frac0 := scale / digPerDec1
	intg0x := intg - intg0*digPerDec1
	frac0x := scale - frac0*digPerDec1
	return intg0*4 + dig2bytes[intg0x] + frac0*4 + dig2bytes[frac0x]
}

func decodeDecimal(r *mconn.BinReader, precision int, scale int) (float64, error) {
	dsz := decimalBinSize(precision, scale)
	buf := make([]byte, dsz)
	// Read decimal data from reader
	decimalData, err := r.ReadBytes(dsz)
	if nil != err {
		return 0, errors.Trace(err)
	}
	copy(buf, decimalData)

	var fbuf bytes.Buffer
	value := uint32(buf[0])
	var mask uint32
	if value&0x80 == 0 {
		// Nagetive
		mask = uint32((1 << 32) - 1)
		fbuf.WriteString("-")
	}

	// Reset the sign flag
	buf[0] ^= 0x80

	return 0, nil
}
