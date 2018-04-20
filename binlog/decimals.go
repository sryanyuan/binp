package binlog

// my_decimal.h decimal.c my_global.h
// github.com/siddontang/go-mysql/replication/row_event.go

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/utils"
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

func decodeDecimal(r *utils.BinReader, precision int, scale int) (float64, error) {
	intg := precision - scale
	intg0 := intg / digPerDec1
	frac0 := scale / digPerDec1
	intg0x := intg - intg0*digPerDec1
	frac0x := scale - frac0*digPerDec1
	dsz := intg0*4 + dig2bytes[intg0x] + frac0*4 + dig2bytes[frac0x]
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

	pos, value := decodeDecimalDecompressValue(intg0x, buf, uint8(mask))
	fbuf.WriteString(strconv.FormatUint(uint64(value), 10))

	for i := 0; i < intg; i++ {
		value = binary.BigEndian.Uint32(buf[pos:]) ^ mask
		pos += 4
		fbuf.WriteString(fmt.Sprintf("%09d", value))
	}

	fbuf.WriteString(".")

	for i := 0; i < frac0; i++ {
		value = binary.BigEndian.Uint32(buf[pos:]) ^ mask
		pos += 4
		fbuf.WriteString(fmt.Sprintf("%09d", value))
	}

	if size, value := decodeDecimalDecompressValue(frac0x, buf[pos:], uint8(mask)); size > 0 {
		fbuf.WriteString(fmt.Sprintf("%0*d", frac0x, value))
		pos += size
	}

	f, err := strconv.ParseFloat(fbuf.String(), 64)
	if nil != err {
		return 0, errors.Trace(err)
	}
	return f, nil
}

func decodeDecimalDecompressValue(compIndx int, data []byte, mask uint8) (size int, value uint32) {
	size = dig2bytes[compIndx]
	databuff := make([]byte, size)
	for i := 0; i < size; i++ {
		databuff[i] = data[i] ^ mask
	}
	value = uint32(utils.NumberFromBytesBigEndian(databuff))
	return
}
