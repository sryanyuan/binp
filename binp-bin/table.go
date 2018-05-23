package main

// ColumnInfo hold column base info
type ColumnInfo struct {
	Index         int
	Name          string
	Type          string
	HasIndex      bool
	Nullable      bool
	Default       string
	HasDefault    bool
	Unsigned      bool
	AutoIncrement bool
}

// TableInfo hold column info
type TableInfo struct {
	Schema  string
	Name    string
	Columns []*ColumnInfo
}

func convertUnsigned(v interface{}, unsigned bool) interface{} {
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
