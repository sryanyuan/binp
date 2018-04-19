package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
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
	tables      map[uint64]*TableMapEvent
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
}

func isBitSet(bitmap []byte, i int) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}

// Reference to log_event_print_value (log_event.cc)
func readValue(r *mconn.BinReader, tp uint8, meta uint16) (interface{}, error) {

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
			precision := int8(meta >> 8)
			decimals := int8(meta & 0xff)
			_ = precision
			_ = decimals
		}
	}

	return nil, errors.Errorf("Unknown type %v", tp)
}

func (e *RowsEvent) readRow(r *mconn.BinReader, mask []byte) (*Row, error) {
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
		columnIndex++
		nullByte := nullMask[int(bitByteIndex)]
		bitMask := uint8(1 << (uint(bitByteIndex) % 8))
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

	if e.version == 2 {
		el, err := r.ReadUint16()
		if nil != err {
			return errors.Trace(err)
		}
		eb, err := r.ReadBytes(int(el))
		if nil != err {
			return errors.Trace(err)
		}
		e.ExtraData = eb
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

	// Find table map event in parser's cache
	tm, ok := e.tables[e.TableID]
	if !ok {
		return errors.Errorf("%v not found in table map cache", e.TableID)
	}
	e.Table = tm

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

	return nil
}
