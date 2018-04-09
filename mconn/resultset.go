package mconn

import (
	"encoding/binary"
	"io"

	"github.com/juju/errors"
)

// http://dev.mysql.com/doc/internals/en/status-flags.html
const (
	statusInTrans = 1 << iota
	statusInAutocommit
	statusReserved // Not in documentation
	statusMoreResultsExists
	statusNoGoodIndexUsed
	statusNoIndexUsed
	statusCursorExists
	statusLastRowSent
	statusDbDropped
	statusNoBackslashEscapes
	statusMetadataChanged
	statusQueryWasSlow
	statusPsOutParams
	statusInTransReadonly
	statusSessionStateChanged
)

var (
	// ErrNoData no datas in the result set
	ErrNoData = errors.New("No data in the result set")
	// ErrColumnOutOfRange represent the index if out of column count
	ErrColumnOutOfRange = errors.New("Index out of column range")
	// ErrInvalidConn returns when connection is invalid
	ErrInvalidConn = errors.New("Invalid conn")
	// ErrRowNotFree represents the connection has a query and not closed
	ErrRowNotFree = errors.New("Query row not free")
	// ErrWrongFieldType returns when get field data with wrong type
	ErrWrongFieldType = errors.New("Wrong field type")
)

// ResultSet contains all column field
type ResultSet struct {
	conn      *Conn
	columnCnt int
	columns   []Field
	datas     []interface{}
	binary    bool
	parseTime bool
	moreRows  bool
}

// Columns get the column count
func (s *ResultSet) Columns() int {
	return s.columnCnt
}

// GetAt get the value of index i
func (s *ResultSet) GetAt(i int) (interface{}, error) {
	if nil == s.datas {
		return "", ErrNoData
	}
	if i < 0 || i >= s.columnCnt {
		return "", ErrColumnOutOfRange
	}
	return s.datas[i], nil
}

// GetAtString get the value of index i as string
func (s *ResultSet) GetAtString(i int) (string, error) {
	data, err := s.GetAt(i)
	if nil != err {
		return "", errors.Trace(err)
	}
	switch s.columns[i].fieldType {
	case FieldTypeVarChar, FieldTypeVarString, FieldTypeString:
		{
			return data.(string), nil
		}
	}

	return "", ErrWrongFieldType
}

// Close close the result set
func (s *ResultSet) Close() error {
	if s.conn == nil {
		return ErrInvalidConn
	}
	// Read the last eof packet
	if _, err := s.conn.readUntilEOF(); nil != err {
		return errors.Trace(err)
	}
	s.discardResults()

	// All rows need to be skipped
	s.conn = nil

	return nil
}

// Next get the next row data
// If eof, io.ErrEOF will be returned
func (s *ResultSet) Next() error {
	if nil == s.conn {
		return ErrInvalidConn
	}
	if s.binary {
		return s.readRowBinary()
	}
	return s.readRowText()
}

// binary: http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
// text: http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (s *ResultSet) readRowText() error {
	rsp, err := s.conn.ReadPacket()
	if nil != err {
		return errors.Trace(err)
	}

	// Got eof?
	if rsp[0] == packetHeaderEOF && len(rsp) == 5 {
		var peof PacketEOF
		if err = peof.Decode(rsp); nil != err {
			return errors.Trace(err)
		}
		if peof.status&statusMoreResultsExists != 0 {
			s.moreRows = true
			// Multi-resultset is not support
			// https://dev.mysql.com/doc/internals/en/multi-resultset.html
			if err = s.discardResults(); nil != err {
				return errors.Trace(err)
			}
		}
		// Already reach row eof
		s.moreRows = false
		s.conn = nil
		return io.EOF
	}
	if rsp[0] == packetHeaderERR {
		s.moreRows = false
		s.conn = nil
		var perr PacketErr
		if err := perr.Decode(rsp); nil != err {
			return errors.Trace(err)
		}
		return errors.Trace(perr.toError())
	}
	// Reading row
	pos := 0
	s.datas = make([]interface{}, 0, s.columnCnt)
	for i := 0; i < s.columnCnt; i++ {
		// Check null
		if rsp[0] == fieldTypeHeaderNull {
			s.datas = append(s.datas, nil)
			pos++
			continue
		}
		// All field data is string
		var ls LenencString
		offset := ls.FromData(rsp[pos:])
		if ls.EOF {
			return errors.Errorf("reading column %v data meet eof", s.columns[i].fieldName)
		}
		pos += offset
		s.datas = append(s.datas, ls.Value)

		if s.parseTime {
			switch s.columns[i].fieldType {
			case FieldTypeTimestamp, FieldTypeDateTime, FieldTypeDate, FieldTypeNewDate:
				{
					s.datas[i], err = parseDateTime(ls.Value, s.conn.loc)
					if nil != err {
						return errors.Trace(err)
					}
				}
			}
		}
	}

	return nil
}

func (s *ResultSet) readRowBinary() error {
	return nil
}

func (s *ResultSet) discardResults() error {
	for s.moreRows {
		data, err := s.conn.ReadPacket()
		if nil != err {
			return errors.Trace(err)
		}

		switch data[0] {
		case packetHeaderOK:
			{
				_, err := s.conn.readPacketOK(data)
				if nil != err {
					return errors.Trace(err)
				}
				// No more rows
				s.moreRows = false
			}
		case packetHeaderERR:
			{
				perr, err := s.conn.readPacketERR(data)
				if nil != err {
					return errors.Trace(err)
				}
				return errors.New(perr.ErrorMessage)
			}
		case packetHeaderLocalInFile:
			{
				return ErrMalformPacket
			}
		}
		// Reading column count
		var ln LenencInt
		offset := ln.FromData(data[:])
		if offset != len(data) {
			return ErrMalformPacket
		}
		if ln.Value <= 0 {
			break
		}
		// Read columns
		var status int
		if status, err = s.conn.readUntilEOF(); nil != err {
			return errors.Trace(err)
		}
		// Read row
		if status, err = s.conn.readUntilEOF(); nil != err {
			return errors.Trace(err)
		}
		if status&statusMoreResultsExists == 0 {
			s.moreRows = false
		}
	}

	return nil
}

// readUntilEOF returns the eof status if success
func (c *Conn) readUntilEOF() (int, error) {
	for {
		data, err := c.ReadPacket()

		if err != nil {
			return 0, errors.Trace(err)
		}
		if data[0] == packetHeaderERR {
			var perr PacketErr
			if err = perr.Decode(data); nil != err {
				return 0, errors.Trace(err)
			}
			return 0, errors.Trace(perr.toError())
		}
		if data[0] == packetHeaderEOF {
			if len(data) != 5 {
				return 0, errors.Trace(ErrMalformPacket)
			}
			var peof PacketEOF
			if err = peof.Decode(data); nil != err {
				return 0, errors.Trace(err)
			}
			return int(peof.status), nil
		}
		// Continue reading
	}
}

// readResultSet decodes the text result set
// https://dev.mysql.com/doc/internals/en/com-query-response.html
func (c *Conn) readResultSet(data []byte, options *responseOptions) (*PacketOK, error) {
	// Reading column count
	var ln LenencInt
	offset := ln.FromData(data[:])
	if offset != len(data) {
		return nil, ErrMalformPacket
	}

	// Read the column
	rs := &ResultSet{
		columnCnt: int(ln.Value),
		conn:      c,
		binary:    options.binary,
	}
	if err := c.readResultColumns(rs); nil != err {
		return nil, errors.Trace(err)
	}

	return &PacketOK{
		Results: rs,
	}, nil
}

func (c *Conn) readResultColumns(rs *ResultSet) error {
	rs.columns = make([]Field, 0, rs.columnCnt)

	for {
		rsp, err := c.ReadPacket()
		if nil != err {
			return errors.Trace(err)
		}

		if rsp[0] == packetHeaderEOF && len(rsp) == 5 {
			// https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
			// We just support protocol 41, so the eof packet is always 5 bytes length
			if len(rs.columns) == rs.columnCnt {
				return nil
			}
			return errors.Errorf("columns count %v, but receive %v columns", rs.columnCnt, len(rs.columns))
		}

		// Read columns
		// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
		// catalog [lenenc_str]
		var column Field
		var ls LenencString
		var ptr int
		offset := ls.FromData(rsp)
		if ls.EOF {
			return errors.New("eof when parsing category")
		}
		ptr += offset
		// schema [lenenc_str]
		offset = ls.FromData(rsp[ptr:])
		if ls.EOF {
			return errors.New("eof when parsing schema")
		}
		column.schemaName = ls.Value
		ptr += offset
		// table [lenenc_str]
		offset = ls.FromData(rsp[ptr:])
		if ls.EOF {
			return errors.New("eof when parsing table")
		}
		column.tableName = ls.Value
		ptr += offset
		// org_table [lenenc_str]
		offset = ls.FromData(rsp[ptr:])
		if ls.EOF {
			return errors.New("eof when parsing org_table")
		}
		ptr += offset
		// name [lenenc_str]
		offset = ls.FromData(rsp[ptr:])
		if ls.EOF {
			return errors.New("eof when parsing name")
		}
		column.fieldName = ls.Value
		ptr += offset
		// org_name [lenenc_str]
		offset = ls.FromData(rsp[ptr:])
		if ls.EOF {
			return errors.New("eof when parsing org_name")
		}
		ptr += offset
		// fixed-length fields, always 0x0c
		var ln LenencInt
		offset = ln.FromData(rsp[ptr:])
		if ln.EOF {
			return errors.New("eof when parsing length of fixed-length fields")
		}
		if 0x0c != ln.Value {
			return errors.Errorf("invalid length of fixed-length fields %v", ln.Value)
		}
		ptr += offset
		// charset [int2]
		column.characterSet = binary.LittleEndian.Uint16(rsp[ptr:])
		ptr += 2
		// column length
		column.columnLength = binary.LittleEndian.Uint32(rsp[ptr:])
		ptr += 4
		// type
		column.fieldType = rsp[ptr]
		ptr++
		// flags
		column.flags = binary.LittleEndian.Uint16(rsp[ptr:])
		ptr += 2
		// decimals
		column.demicals = rsp[ptr]
		ptr++
		// filler 0x00 0x00
		// skip following data
		// 2              filler [00] [00]
		// if command was COM_FIELD_LIST {
		// lenenc_int     length of default-values
		// string[$len]   default values
		rs.columns = append(rs.columns, column)
	}
}
