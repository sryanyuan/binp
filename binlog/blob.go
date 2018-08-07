package binlog

import (
	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

func decodeBlob(r *serialize.BinReader, meta uint16) (interface{}, error) {
	var l int

	switch meta {
	case 1:
		{
			// TINYBLOB/TINYTEXT
			v, err := r.ReadUint8()
			if nil != err {
				return nil, errors.Trace(err)
			}
			l = int(v)
		}
	case 2:
		{
			// BLOB/TEXT
			v, err := r.ReadUint16()
			if nil != err {
				return nil, errors.Trace(err)
			}
			l = int(v)
		}
	case 3:
		{
			// MEDIUMBLOB/MEDIUMTEXT
			v, err := r.ReadUint24()
			if nil != err {
				return nil, errors.Trace(err)
			}
			l = int(v)
		}
	case 4:
		{
			// LONGBLOB/LONGTEXT
			v, err := r.ReadUint32()
			if nil != err {
				return nil, errors.Trace(err)
			}
			l = int(v)
		}
	default:
		{
			return nil, errors.Errorf("Unknown blob packetlen=%d", meta)
		}
	}

	return readBlob(r, l)
}

func readBlob(r *serialize.BinReader, l int) (interface{}, error) {
	v, err := r.ReadBytes(l)
	if nil != err {
		return nil, errors.Trace(err)
	}
	return v, nil
}

func decodeVarChar(r *serialize.BinReader, meta uint16) (interface{}, error) {
	var l int

	if meta < 256 {
		v, err := r.ReadUint8()
		if nil != err {
			return nil, errors.Trace(err)
		}
		l = int(v)
	} else {
		// Read 2bytes as length
		v, err := r.ReadUint16()
		if nil != err {
			return nil, errors.Trace(err)
		}
		l = int(v)
	}

	return readBlob(r, l)
}
