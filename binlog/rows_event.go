package binlog

import (
	"fmt"
	"math"
	"time"

	"github.com/sryanyuan/binp/rule"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
	"github.com/sryanyuan/binp/serialize"
)

// Rows event flags
const (
	RowsEventFlagStmtEnd = 0x01
)

// Specify the rows event action
const (
	RowWrite = iota
	RowUpdate
	RowDelete
)

// Row is a row data contain columns
type Row struct {
	ColumnDatas []interface{}
}

// RowsEvent see below
// https://dev.mysql.com/doc/internals/en/rows-event.html
type RowsEvent struct {
	tableIDSize uint8
	version     int
	Action      int
	TableID     uint64
	Table       *TableMapEvent
	Flags       uint16
	ExtraData   []byte
	ColumnCount uint64
	Bitmap1     []byte
	Bitmap2     []byte
	Rows        []*Row
	// Sync desc
	Rule *rule.SyncDesc
}

func isBitSet(bitmap []byte, i int) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}

// Reference to log_event_print_value (log_event.cc)
func readValue(r *serialize.BinReader, tp uint8, meta uint16) (interface{}, error) {

	switch tp {
	case mconn.FieldTypeNull:
		{
			return nil, nil
		}
	case mconn.FieldTypeLong:
		{
			v, err := r.ReadInt32()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeTiny:
		{
			v, err := r.ReadInt8()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeShort:
		{
			v, err := r.ReadInt16()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeInt24:
		{
			v, err := r.ReadInt24()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeDate:
		{
			v, err := r.ReadUint24()
			if nil != err {
				return nil, errors.Trace(err)
			}
			if 0 == v {
				return "0000-00-00", nil
			}
			return fmt.Sprintf("%04d-%02d-%02d", v/(32*16), v/32%16, v%32), nil
		}
	case mconn.FieldTypeLongLong:
		{
			v, err := r.ReadInt64()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeNewDecimal:
		{
			precision := int(meta >> 8)
			decimals := int(meta & 0xff)
			fv, err := decodeDecimal(r, precision, decimals)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return fv, nil
		}
	case mconn.FieldTypeFloat:
		{
			v, err := r.ReadUint32()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return math.Float32frombits(v), nil
		}
	case mconn.FieldTypeDouble:
		{
			v, err := r.ReadUint64()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return math.Float64frombits(v), nil
		}
	case mconn.FieldTypeBit:
		{
			nbits := ((meta >> 8) * 8) + (meta & 0xff)
			l := int((nbits + 7) / 8)
			v, err := decodeBit(r, nbits, l)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeTimestamp:
		{
			v, err := r.ReadUint32()
			if nil != err {
				return nil, errors.Trace(err)
			}
			tm := time.Unix(int64(v), 0)
			return formatTimeWithDecimals(tm, 0), nil
		}
	case mconn.FieldTypeTimestamp2:
		{
			v, err := decodeTimestamp2(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeDateTime:
		{
			v, err := decodeDatetime(r)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeDateTime2:
		{
			v, err := decodeDatetime2(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeTime:
		{
			v, err := decodeTime(r)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeTime2:
		{
			v, err := decodeTime2(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeNewDate:
		{
			v, err := decodeDate(r)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeYear:
		{
			v, err := r.ReadUint8()
			if nil != err {
				return nil, errors.Trace(err)
			}
			return fmt.Sprintf("%d", 1900+int(v)), nil
		}
	case mconn.FieldTypeEnum:
		{
			v, err := decodeEnum(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeSet:
		{
			n := int(meta & 0xFF)
			nbits := n * 8

			v, err := decodeBit(r, uint16(nbits), n)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeBlob:
		{
			v, err := decodeBlob(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeVarChar,
		mconn.FieldTypeVarString:
		{
			v, err := decodeVarChar(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeString:
		{
			if meta >= 256 {
				b0 := uint8(meta >> 8)
				b1 := uint8(meta & 0xff)

				if b0&0x30 != 0x30 {
					meta = uint16(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
					tp = byte(b0 | 0x30)
				} else {
					meta = uint16(meta & 0xff)
					tp = b0
				}
			}
			v, err := decodeVarChar(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeJSON:
		{
			v, err := decodeJSON(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	case mconn.FieldTypeGeometry:
		{
			v, err := decodeBlob(r, meta)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return v, nil
		}
	default:
		{
			return nil, errors.Errorf("Don't know how to handle column type=%d meta=%d",
				tp, meta)
		}
	}

	//return nil, errors.Errorf("Unknown type %v", tp)
}

func (e *RowsEvent) readRow(r *serialize.BinReader, mask []byte) (*Row, error) {
	row := &Row{}
	row.ColumnDatas = make([]interface{}, int(e.ColumnCount))
	// Check how many column is presented
	count := 0
	for i := 0; i < int(e.ColumnCount); i++ {
		if isBitSet(mask, i) {
			count++
		}
	}
	count = (count + 7) / 8

	nullMask, err := r.ReadBytes(count)
	if nil != err {
		return nil, errors.Trace(err)
	}

	columnIndex := 0
	for i := 0; i < int(e.ColumnCount); i++ {
		if !isBitSet(mask, i) {
			continue
		}

		bitByteIndex := columnIndex / 8
		nullByte := nullMask[int(bitByteIndex)]
		bitMask := uint8(1 << (uint(columnIndex) % 8))
		columnIndex++
		if nullByte&bitMask != 0 {
			// Is null
			continue
		}

		row.ColumnDatas[i], err = readValue(r, e.Table.ColumnDefine[i], e.Table.ColumnMeta[i])
		if nil != err {
			return nil, errors.Trace(err)
		}
	}

	return row, nil
}

// Decode decodes the binary data into payload
func (e *RowsEvent) Decode(data []byte) error {
	var err error
	r := serialize.NewBinReader(data)

	// Table map event is fetched by parser, not here
	/*if e.tableIDSize == 4 {
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

	// Find table map event in parser's cache
	tm, ok := e.tables[e.TableID]
	if !ok {
		return errors.Errorf("%v not found in table map cache", e.TableID)
	}
	e.Table = tm*/

	e.Flags, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}

	if e.version == 2 {
		el, err := r.ReadUint16()
		if nil != err {
			return errors.Trace(err)
		}
		// extra_data [length=extra_data_len - 2], zero or more Binlog::RowsEventExtraData
		if el-2 > 0 {
			eb, err := r.ReadBytes(int(el) - 2)
			if nil != err {
				return errors.Trace(err)
			}
			e.ExtraData = eb
		}
	}

	e.ColumnCount, err = r.ReadLenencInt()
	if nil != err {
		return errors.Trace(err)
	}
	// Bit mask of columns1
	bitmapLen := int(e.ColumnCount+7) / 8
	e.Bitmap1, err = r.ReadBytes(bitmapLen)
	if nil != err {
		return errors.Trace(err)
	}
	if e.Action == RowUpdate &&
		e.version > 0 {
		e.Bitmap2, err = r.ReadBytes(bitmapLen)
		if nil != err {
			return errors.Trace(err)
		}
	}

	// Read rows
	e.Rows = make([]*Row, 0, 8)
	for {
		if r.Empty() {
			break
		}
		row, err := e.readRow(r, e.Bitmap1)
		if nil != err {
			return errors.Trace(err)
		}
		e.Rows = append(e.Rows, row)
		// Is update event and version > 0 ?
		if e.Action == RowUpdate &&
			e.version > 0 {
			row, err = e.readRow(r, e.Bitmap2)
			if nil != err {
				return errors.Trace(err)
			}
			e.Rows = append(e.Rows, row)
		}
	}

	r.End()

	return nil
}
