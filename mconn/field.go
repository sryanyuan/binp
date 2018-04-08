package mconn

import (
	"fmt"
	"time"
)

const (
	fieldTypeHeaderNull = 0xfb
	timeFormat          = "2006-01-02 15:04:05.999999"
)

// FieldType is type of mysql field
const (
	FieldTypeDecimal = iota
	FieldTypeTiny
	FieldTypeShort
	FieldTypeLong
	FieldTypeFloat
	FieldTypeNull
	FieldTypeTimestamp
	FieldTypeLongLong
	FieldTypeInt24
	FieldTypeDate
	FieldTypeTime
	FieldTypeDateTime
	FieldTypeYear
	FieldTypeNewDate
	FieldTypeVarChar
	FieldTypeBit
	FieldTypeTimestamp2
	FieldTypeDateTime2
	FieldTypeTime2
)

// FieldType is type of mysql field
const (
	FieldTypeNewDecimal = 0xf6 + iota
	FieldTypeEnum
	FieldTypeSet
	FieldTypeTinyBlob
	FieldTypeMediumBlob
	FieldTypeLongBlob
	FieldTypeBlob
	FieldTypeVarString
	FieldTypeString
	FieldTypeGeometry
)

// Field is a mysql field
type Field struct {
	schemaName   string
	tableName    string
	fieldName    string
	characterSet uint16
	columnLength uint32
	fieldType    byte
	flags        uint16
	demicals     byte
}

func parseDateTime(str string, loc *time.Location) (time.Time, error) {
	var t time.Time
	var err error

	base := "0000-00-00 00:00:00.0000000"
	switch len(str) {
	case 10, 19, 21, 22, 23, 24, 25, 26: // up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
		if str == base[:len(str)] {
			return t, err
		}
		t, err = time.Parse(timeFormat[:len(str)], str)
	default:
		err = fmt.Errorf("invalid time string: %s", str)
		return t, err
	}

	// Adjust location
	if err == nil && loc != time.UTC {
		y, mo, d := t.Date()
		h, mi, s := t.Clock()
		t, err = time.Date(y, mo, d, h, mi, s, t.Nanosecond(), loc), nil
	}

	return t, err
}
