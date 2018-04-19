package mconn

func writeLengthPrefixString(buf []byte, v string) int {
	// Validate
	if len(v) > 0xff {
		panic("length prefix string too long")
	}
	if len(v)+1 > len(buf) {
		panic("length prefix string overflow")
	}
	pos := 0
	buf[pos] = uint8(len(v))
	copy(buf[1:], v)

	return 1 + len(v)
}

func numberFromBufferLittleEndian(buf []byte) uint64 {
	v := uint64(0)
	for i, b := range buf {
		v |= (uint64(b) << uint(i) * 8)
	}
	return v
}
