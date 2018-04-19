package binlog

import (
	"encoding/binary"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
)

// TableMapEvent event of TableMapEvent
// https://dev.mysql.com/doc/internals/en/table-map-event.html
type TableMapEvent struct {
	tableIDSize  uint8
	TableID      uint64
	Flags        uint16
	SchemaName   string
	TableName    string
	ColumnCount  uint64
	ColumnDefine []byte
	ColumnMeta   []uint16
	NullBitmask  []byte
}

func (e *TableMapEvent) decodeColumnMetaDef(meta []byte) error {
	var err error
	mr := mconn.NewBinReader(meta)
	e.ColumnMeta = make([]uint16, e.ColumnCount)

	for i, v := range e.ColumnDefine {
		switch v {
		case mconn.FieldTypeString:
			{
				// due to Bug37426 layout of the string metadata is a bit tightly packed:
				// 1 byte0 | 1 byte1 The two bytes encode type and length
				// http://bugs.mysql.com/37426
				b, err := mr.ReadBytes(2)
				if nil != err {
					return errors.Trace(err)
				}
				e.ColumnMeta[i] = binary.BigEndian.Uint16(b)
			}
		case mconn.FieldTypeNewDecimal:
			{
				// High is precious, low is decimals
				b, err := mr.ReadBytes(2)
				if nil != err {
					return errors.Trace(err)
				}
				e.ColumnMeta[i] = binary.BigEndian.Uint16(b)
			}
		case mconn.FieldTypeVarString,
			mconn.FieldTypeVarChar,
			mconn.FieldTypeBit:
			{
				e.ColumnMeta[i], err = mr.ReadUint16()
				if nil != err {
					return errors.Trace(err)
				}
			}
		case mconn.FieldTypeBlob,
			mconn.FieldTypeDouble,
			mconn.FieldTypeFloat,
			mconn.FieldTypeGeometry,
			mconn.FieldTypeJSON,
			mconn.FieldTypeTime2,
			mconn.FieldTypeDateTime2,
			mconn.FieldTypeTimestamp2:
			{
				b, err := mr.ReadUint8()
				if nil != err {
					return errors.Trace(err)
				}
				e.ColumnMeta[i] = uint16(b)
			}
		case mconn.FieldTypeNewDate,
			mconn.FieldTypeEnum,
			mconn.FieldTypeSet,
			mconn.FieldTypeTinyBlob,
			mconn.FieldTypeMediumBlob,
			mconn.FieldTypeLongBlob:
			{
				return errors.Errorf("invalid binlog field type %v", v)
			}
		default:
			{
				e.ColumnMeta[i] = 0
			}
		}
	}
	return nil
}

// Decode decodes the binary data into payload
func (e *TableMapEvent) Decode(data []byte) error {
	var err error
	r := mconn.NewBinReader(data)

	if e.tableIDSize == 4 {
		tid, err := r.ReadUint32()
		if nil != err {
			return errors.Trace(err)
		}
		e.TableID = uint64(tid)
	} else {
		e.TableID, err = r.ReadUint48()
		if nil != err {
			return errors.Trace(err)
		}
	}

	e.Flags, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}

	e.SchemaName, err = r.ReadLenString()
	if nil != err {
		return errors.Trace(err)
	}
	_, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}

	e.TableName, err = r.ReadLenString()
	if nil != err {
		return errors.Trace(err)
	}
	_, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}

	e.ColumnCount, err = r.ReadLenencInt()
	if nil != err {
		return errors.Trace(err)
	}

	e.ColumnDefine, err = r.ReadBytes(int(e.ColumnCount))
	if nil != err {
		return errors.Trace(err)
	}

	columnMetaDef, err := r.ReadLenencBytes()
	if nil != err {
		return errors.Trace(err)
	}
	if err = e.decodeColumnMetaDef(columnMetaDef); nil != err {
		return errors.Trace(err)
	}

	leftBytes := r.LeftBytes()
	if len(leftBytes) != (int(e.ColumnCount)+7)/8 {
		return errors.New("invalid null mask")
	}
	e.NullBitmask = leftBytes

	return nil
}
