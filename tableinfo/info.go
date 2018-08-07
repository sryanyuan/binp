package tableinfo

import (
	"fmt"
	"strconv"
)

// ColumnInfo hold column base info
type ColumnInfo struct {
	Index         int
	Name          string
	Type          string
	IsPrimary     bool
	Nullable      bool
	Default       string
	HasDefault    bool
	Unsigned      bool
	AutoIncrement bool
}

// ColumnWithValue holds column value
type ColumnWithValue struct {
	Column *ColumnInfo
	Value  interface{}
}

// ValueToString converts value to string type
func (c *ColumnWithValue) ValueToString() string {
	fv := convertUnsigned(c.Value, c.Column.Unsigned)
	switch av := fv.(type) {
	case nil:
		{
			return "null"
		}
	case bool:
		{
			if av {
				return "true"
			}
			return "false"
		}
	case int:
		{
			return strconv.FormatInt(int64(av), 10)
		}
	case int8:
		{
			return strconv.FormatInt(int64(av), 10)
		}
	case int16:
		{
			return strconv.FormatInt(int64(av), 10)
		}
	case int32:
		{
			return strconv.FormatInt(int64(av), 10)
		}
	case int64:
		{
			return strconv.FormatInt(int64(av), 10)
		}
	case uint:
		{
			return strconv.FormatUint(uint64(av), 10)
		}
	case uint8:
		{
			return strconv.FormatUint(uint64(av), 10)
		}
	case uint16:
		{
			return strconv.FormatUint(uint64(av), 10)
		}
	case uint32:
		{
			return strconv.FormatUint(uint64(av), 10)
		}
	case uint64:
		{
			return strconv.FormatUint(uint64(av), 10)
		}
	case float32:
		{
			return strconv.FormatFloat(float64(av), 'f', -1, 32)
		}
	case float64:
		{
			return strconv.FormatFloat(float64(av), 'f', -1, 64)
		}
	case string:
		{
			return av
		}
	case []byte:
		{
			return string(av)
		}
	default:
		{
			return fmt.Sprintf("%v", av)
		}
	}
}

// TableInfo hold column info
type TableInfo struct {
	Schema       string
	Name         string
	Columns      []*ColumnInfo
	IndexColumns []*ColumnInfo
}

// FillColumnsWithValue fill values into columns
func FillColumnsWithValue(ti *TableInfo, values []interface{}) []*ColumnWithValue {
	if len(ti.Columns) != len(values) {
		panic("Table columns count not equal to values count")
	}
	cwvs := make([]*ColumnWithValue, 0, len(values))
	for i := range values {
		cw := &ColumnWithValue{
			Column: ti.Columns[i],
			Value:  values[i],
		}
		cwvs = append(cwvs, cw)
	}
	return cwvs
}

// FindColumnByName returns the column matches column name
func FindColumnByName(cols []*ColumnInfo, cname string) *ColumnInfo {
	for _, v := range cols {
		if v.Name == cname {
			return v
		}
	}
	return nil
}

func convertUnsigned(v interface{}, unsigned bool) interface{} {
	if !unsigned {
		return v
	}
	switch tv := v.(type) {
	case int:
		{
			return uint(tv)
		}
	case int8:
		{
			return uint8(tv)
		}
	case int16:
		{
			return uint16(tv)
		}
	case int32:
		{
			return uint32(tv)
		}
	case int64:
		{
			return uint64(tv)
		}
	}

	return v
}
