package utils

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
)

// Parse errors
var (
	ErrBinaryEventOverflow = errors.New("Binary event data overflow")
)

// BinReader reads values from the binary data
type BinReader struct {
	buf *bytes.Buffer
}

// NewBinReader creates a new reader
func NewBinReader(data []byte) *BinReader {
	return &BinReader{buf: bytes.NewBuffer(data)}
}

// End release the internal buffer
func (r *BinReader) End() {
	r.buf = nil
}

func (r *BinReader) next(v int) ([]byte, error) {
	data := r.buf.Next(v)
	if len(data) != v {
		return nil, ErrBinaryEventOverflow
	}
	return data, nil
}

// ReadUint8 reads uint8 from the buffer
func (r *BinReader) ReadUint8() (uint8, error) {
	data, err := r.next(1)
	if nil != err {
		return 0, errors.Trace(err)
	}
	return data[0], nil
}

// ReadInt8 reads int8 from the buffer
func (r *BinReader) ReadInt8() (int8, error) {
	v, err := r.ReadUint8()
	if nil != err {
		return 0, errors.Trace(err)
	}
	return int8(v), nil
}

// ReadUint16 reads uint16 from the buffer
func (r *BinReader) ReadUint16() (uint16, error) {
	data, err := r.next(2)
	if nil != err {
		return 0, errors.Trace(err)
	}
	return binary.LittleEndian.Uint16(data), nil
}

// ReadInt16 reads int16 from the buffer
func (r *BinReader) ReadInt16() (int16, error) {
	v, err := r.ReadUint16()
	if nil != err {
		return 0, errors.Trace(err)
	}
	return int16(v), nil
}

// ReadUint32 reads uint32 from the buffer
func (r *BinReader) ReadUint32() (uint32, error) {
	data, err := r.next(4)
	if nil != err {
		return 0, errors.Trace(err)
	}
	return binary.LittleEndian.Uint32(data), nil
}

// ReadInt32 reads int32 from the buffer
func (r *BinReader) ReadInt32() (int32, error) {
	v, err := r.ReadUint32()
	if nil != err {
		return 0, errors.Trace(err)
	}
	return int32(v), nil
}

// ReadUint24 reads the next 3 bytes as the uint32 number
func (r *BinReader) ReadUint24() (uint32, error) {
	data, err := r.next(3)
	if nil != err {
		return 0, errors.Trace(err)
	}

	v := uint32((uint32(data[2]) << 16) | (uint32(data[1]) << 8) | uint32(data[0]))
	return v, nil
}

// ReadInt24 reads the next 3 bytes as the int32 number
func (r *BinReader) ReadInt24() (int32, error) {
	data, err := r.next(3)
	if nil != err {
		return 0, errors.Trace(err)
	}
	var v int32
	var byte4 uint32
	if data[2]&0x80 != 0 {
		// Have sign flag
		byte4 = 0xff << 24
	}
	v = int32((byte4 << 24) | (uint32(data[2]) << 16) | (uint32(data[1]) << 8) | uint32(data[0]))
	return v, nil
}

// ReadUint48 reads uint48 from the buffer
func (r *BinReader) ReadUint48() (uint64, error) {
	data, err := r.next(6)
	if nil != err {
		return 0, errors.Trace(err)
	}
	return numberFromBufferLittleEndian(data), nil
}

// ReadUint64 reads uint64 from the buffer
func (r *BinReader) ReadUint64() (uint64, error) {
	data, err := r.next(8)
	if nil != err {
		return 0, errors.Trace(err)
	}
	return binary.LittleEndian.Uint64(data), nil
}

// ReadInt64 reads int64 from the buffer
func (r *BinReader) ReadInt64() (int64, error) {
	v, err := r.ReadUint64()
	if nil != err {
		return 0, errors.Trace(err)
	}
	return int64(v), nil
}

// ReadBytesUntilTerm read the buffer bytes until meet terminate byte(0)
func (r *BinReader) ReadBytesUntilTerm() ([]byte, error) {
	lb := r.LeftBytes()
	i := bytes.IndexByte(lb, 0)
	if i < 0 {
		return nil, errors.New("terminate character not found")
	}
	lb = lb[:i]
	r.buf.Next(i + 1)
	return lb, nil
}

// ReadStringUntilTerm read the buffer bytes until meet terminate byte(0)
func (r *BinReader) ReadStringUntilTerm() (string, error) {
	v, err := r.ReadBytesUntilTerm()
	if nil != err {
		return "", errors.Trace(err)
	}
	return string(v), nil
}

// ReadStringWithLen reads string with given length
func (r *BinReader) ReadStringWithLen(l int) (string, error) {
	data, err := r.next(l)
	if nil != err {
		return "", errors.Trace(err)
	}
	return string(data), nil
}

// ReadLenString reads length string from the buffer
func (r *BinReader) ReadLenString() (string, error) {
	l, err := r.ReadUint8()
	if nil != err {
		return "", errors.Trace(err)
	}
	v, err := r.next(int(l))
	if nil != err {
		return "", errors.Trace(err)
	}
	return string(v), nil
}

// ReadEOFString reads string with eof from the buffer
func (r *BinReader) ReadEOFString() (string, error) {
	v := r.LeftBytes()
	if len(v) == 0 {
		return "", ErrBinaryEventOverflow
	}
	return string(v), nil
}

// ReadBytes reads bytes with given size
func (r *BinReader) ReadBytes(sz int) ([]byte, error) {
	l, err := r.next(sz)
	if nil != err {
		return nil, errors.Trace(err)
	}
	return l, nil
}

// ReadLenencInt reads the lenenc int from the buffer
func (r *BinReader) ReadLenencInt() (uint64, error) {
	// Read the first byte
	fv, err := r.next(1)
	if nil != err {
		return 0, errors.Trace(err)
	}
	flag := fv[0]
	var num []byte

	switch {
	case flag < 0xfb:
		{
			return uint64(flag), nil
		}
	case flag == 0xfc:
		{
			// We need read the next 2 bytes
			num, err = r.next(2)
			if nil != err {
				return 0, errors.Trace(err)
			}
		}
	case flag == 0xfd:
		{
			// We need read the next 3 bytes
			num, err = r.next(3)
			if nil != err {
				return 0, errors.Trace(err)
			}
		}
	case flag == 0xfe:
		{
			// We need read the next 8 bytes
			num, err = r.next(3)
			if nil != err {
				return 0, errors.Trace(err)
			}
		}
	}

	if nil == num {
		return 0, errors.New("invalid lenenc int flag")
	}
	return numberFromBufferLittleEndian(num), nil
}

// ReadLenencString reads the lenenc string from the buffer
func (r *BinReader) ReadLenencString() (string, error) {
	l, err := r.ReadLenencBytes()
	if nil != err {
		return "", errors.Trace(err)
	}
	return string(l), nil
}

// ReadLenencBytes reads the lenenc bytes from the buffer
func (r *BinReader) ReadLenencBytes() ([]byte, error) {
	l, err := r.ReadLenencInt()
	if nil != err {
		return nil, errors.Trace(err)
	}

	v, err := r.next(int(l))
	if nil != err {
		return nil, errors.Trace(err)
	}

	return v, nil
}

// LeftBytes reads the left bytes in the buffer
func (r *BinReader) LeftBytes() []byte {
	return r.buf.Bytes()
}

// Empty returns true if unread buffer is empty
func (r *BinReader) Empty() bool {
	return r.buf.Len() == 0
}
