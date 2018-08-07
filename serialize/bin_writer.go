package serialize

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
)

// BinWriter writes value to binary buffer
type BinWriter struct {
	buf *bytes.Buffer
}

// NewBinWriter creates a new writer
func NewBinWriter(b []byte) *BinWriter {
	buf := bytes.NewBuffer(b)
	if nil != b {
		// Should reset
		buf.Reset()
	}
	return &BinWriter{
		buf: buf,
	}
}

// End release the internal buffer
func (w *BinWriter) End() {
	w.buf = nil
}

// WriteUint8 writes uint8 to the buffer
func (w *BinWriter) WriteUint8(v uint8) error {
	err := w.buf.WriteByte(v)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteInt8 writes int8 to the buffer
func (w *BinWriter) WriteInt8(v int8) error {
	err := w.WriteUint8(uint8(v))
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteUint16 writes uint16 to the buffer
func (w *BinWriter) WriteUint16(v uint16) error {
	var datas [2]byte
	binary.LittleEndian.PutUint16(datas[:], v)
	_, err := w.buf.Write(datas[:])
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteInt16 writes int16 to the buffer
func (w *BinWriter) WriteInt16(v int16) error {
	err := w.WriteUint16(uint16(v))
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteUint32 writes uint32 to the buffer
func (w *BinWriter) WriteUint32(v uint32) error {
	var datas [4]byte
	binary.LittleEndian.PutUint32(datas[:], v)
	_, err := w.buf.Write(datas[:])
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteInt32 writes int32 to the buffer
func (w *BinWriter) WriteInt32(v int32) error {
	err := w.WriteUint32(uint32(v))
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteUint64 writes uint64 to the buffer
func (w *BinWriter) WriteUint64(v uint64) error {
	var datas [8]byte
	binary.LittleEndian.PutUint64(datas[:], v)
	_, err := w.buf.Write(datas[:])
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteInt64 writes int64 to the buffer
func (w *BinWriter) WriteInt64(v int64) error {
	err := w.WriteUint64(uint64(v))
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteStringWithTerm writes a string with '\0' terminate
func (w *BinWriter) WriteStringWithTerm(s string) error {
	var err error

	if len(s) != 0 {
		_, err = w.buf.WriteString(s)
		if nil != err {
			return errors.Trace(err)
		}
	}

	err = w.WriteUint8(0)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteLenString writes a string with length as prefix
func (w *BinWriter) WriteLenString(s string) error {
	var err error

	err = w.WriteUint8(uint8(len(s)))
	if nil != err {
		return errors.Trace(err)
	}

	if len(s) != 0 {
		_, err = w.buf.WriteString(s)
		if nil != err {
			return errors.Trace(err)
		}
	}

	return nil
}

// WriteLenBytes writes byte slice with length as prefix
func (w *BinWriter) WriteLenBytes(data []byte) error {
	var err error

	err = w.WriteUint8(uint8(len(data)))
	if nil != err {
		return errors.Trace(err)
	}

	if len(data) != 0 {
		_, err = w.buf.Write(data)
		if nil != err {
			return errors.Trace(err)
		}
	}

	return nil
}

// WriteBytes write bytes to the buffer
func (w *BinWriter) WriteBytes(data []byte) error {
	_, err := w.buf.Write(data)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// WriteEOFString write bytes to the buffer
func (w *BinWriter) WriteEOFString(s string) error {
	_, err := w.buf.WriteString(s)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

// Bytes return the write bytes buffer
func (w *BinWriter) Bytes() []byte {
	return w.buf.Bytes()
}
