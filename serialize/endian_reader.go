package serialize

// NumberFromBytesLittleEndian parses the buf by little endian order
func NumberFromBytesLittleEndian(buf []byte) uint64 {
	return numberFromBufferLittleEndian(buf)
}

func numberFromBufferLittleEndian(buf []byte) uint64 {
	v := uint64(0)
	for i, b := range buf {
		v |= uint64(b) << (uint(i) * 8)
	}
	return v
}

// NumberFromBytesBigEndian parses the buf by big endian order
func NumberFromBytesBigEndian(buf []byte) uint64 {
	return numberFromBufferBigEndian(buf)
}

func numberFromBufferBigEndian(buf []byte) uint64 {
	l := len(buf)
	v := uint64(0)
	for i, b := range buf {
		v |= uint64(b) << (uint(l-i-1) * 8)
	}
	return v
}
