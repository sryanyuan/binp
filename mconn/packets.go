package mconn

import (
	"crypto/sha1"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/serialize"
)

// PacketMySQL defines mysql packet interface
type PacketMySQL interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

// PacketHeader is the header of the mysql packet to read the packet length and sequence
type PacketHeader [4]byte

// ToSlice returns a slice reference to the underlying array
func (p *PacketHeader) ToSlice() []byte {
	return (*p)[0:]
}

// GetLength returns the header length
func (p *PacketHeader) GetLength() int {
	data := p.ToSlice()
	return int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)
}

// SetLength set the length into underlying array
func (p *PacketHeader) SetLength(v int) {
	data := p.ToSlice()
	uv := uint32(v)
	data[0] = byte(uv & 0x000000ff)
	data[1] = byte(uv & 0x0000ff00)
	data[2] = byte(uv & 0x00ff0000)
}

// GetSequence returns the header sequence
func (p *PacketHeader) GetSequence() uint8 {
	return uint8(p.ToSlice()[3])
}

// SetSequence sets the packet sequence
func (p *PacketHeader) SetSequence(v uint8) {
	data := p.ToSlice()
	data[3] = byte(v)
}

// PacketHandshake is handshake packet sending from server
type PacketHandshake struct {
	ProtocolVersion    uint8
	ServerVersion      string
	ConnectionID       uint32
	AuthPluginDataPart []byte
	Filter             uint8
	CapabilityFlags    uint32
	CharacterSet       uint8
	StatusFlags        uint16
	AuthPluginName     string
}

// Decode read binary data into handshake packet
// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
func (p *PacketHandshake) Decode(data []byte) error {
	var err error
	r := serialize.NewBinReader(data)

	// Protocol version
	p.ProtocolVersion, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	// Server version
	p.ServerVersion, err = r.ReadStringUntilTerm()
	if nil != err {
		return errors.Trace(err)
	}
	// Connection id
	p.ConnectionID, err = r.ReadUint32()
	if nil != err {
		return errors.Trace(err)
	}
	// Auth plugin data part 1
	p.AuthPluginDataPart = make([]byte, 8, 8+13)
	authPluginDataPart, err := r.ReadBytes(8)
	if nil != err {
		return errors.Trace(err)
	}
	copy(p.AuthPluginDataPart[:], authPluginDataPart)
	// Filter
	p.Filter, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	// capability_flag_1 lower bytes
	cp, err := r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}
	p.CapabilityFlags = uint32(cp)
	// character set
	p.CharacterSet, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	// status flags
	p.StatusFlags, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}
	// capability flags 2 upper bytes
	cp, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}
	p.CapabilityFlags = uint32(cp)<<16 | p.CapabilityFlags
	// auth plugin data len
	authPluginDataLen := uint8(0)
	if (p.CapabilityFlags & clientPluginAuth) != 0 {
		authPluginDataLen, err = r.ReadUint8()
		if nil != err {
			return errors.Trace(err)
		}
	} /*else {
		// Always byte 0
		if rptr >= dl {
			return errors.New("parse auth plugin data len error")
		}
		authPluginDataLen = data[rptr]
		rptr++
	}*/
	// Reserved string(10)
	_, err = r.ReadBytes(10)
	if nil != err {
		return errors.Trace(err)
	}
	// auth plugin data part2
	if (p.CapabilityFlags & clientSecureConnection) != 0 {
		// auth plugin data part2 length is mutable. $len=MAX(13, length of auth-plugin-data - 8)
		p2len := authPluginDataLen - 8
		if p2len > 13 {
			p2len = 13
		}
		// mysql-5.7/sql/auth/sql_authentication.cc line 538, the 13th byte is '\0',
		// so it is a null terminated string.so we read the data as a null terminated string.
		authPluginDataPart, err = r.ReadBytes(int(p2len) - 1)
		if nil != err {
			return errors.Trace(err)
		}
		p.AuthPluginDataPart = append(p.AuthPluginDataPart, authPluginDataPart...)
		// Skip the next '\0' byte
		_, err = r.ReadUint8()
		if nil != err {
			return errors.Trace(err)
		}
	}
	// auth-plugin name
	if (p.CapabilityFlags & clientPluginAuth) != 0 {
		// String eof
		p.AuthPluginName, err = r.ReadStringUntilTerm()
		if nil != err {
			return errors.Trace(err)
		}
	}
	// Check to the terminal
	if !r.Empty() {
		return errors.New("not eof")
	}
	r.End()

	return nil
}

// PacketHandshakeResponse responses the handshake packet to server
// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
type PacketHandshakeResponse struct {
	CapabilityFlags uint32
	MaxPacketSize   uint32
	Charset         uint8
	// Reserved        [23]byte not used
	Username           string // string[null]
	Password           string
	passEncoded        []byte
	AuthResponseLength LenencInt
	Database           string
	// Decode part
	AuthPluginData []byte
}

func (p *PacketHandshakeResponse) esitimateSize() int {
	var sz int
	// 4 bytes of payload length
	sz += 4
	// 4 bytes of capability flags
	sz += 4
	// 4 bytes of max packet size
	sz += 4
	// 1 byte of charset
	sz++
	// 23 bytes of reserved
	sz += 23
	// len(username) + 1 byte of null
	sz += len(p.Username) + 1
	// 1 byte of pass length + len(pass encoded)
	sz += len(p.passEncoded) + 1
	// len(database) + 1 byte of null
	if "" != p.Database {
		sz += len(p.Database) + 1
	}
	// len client plugin auth + 1 byte of null
	sz += len(MySQLNativePasswordPlugin) + 1

	return sz
}

func (p *PacketHandshakeResponse) encodePass(key []byte) []byte {
	if "" == p.Password {
		return nil
	}
	passBytes := []byte(p.Password)

	s1 := sha1.New()
	s1.Write(passBytes)
	s1hash := s1.Sum(nil)

	s1.Reset()
	s1.Write(s1hash)
	shash := s1.Sum(nil)

	s1.Reset()
	s1.Write(key)
	s1.Write(shash)
	phash := s1.Sum(nil)

	for i := range phash {
		phash[i] ^= s1hash[i]
	}
	return phash
}

// Encode serialize the handshake response
func (p *PacketHandshakeResponse) Encode() []byte {
	p.passEncoded = p.encodePass(p.AuthPluginData)

	var err error
	sz := p.esitimateSize()
	data := make([]byte, sz)
	w := serialize.NewBinWriter(data)

	// Skip the payload length, auto fill by WritePacket
	if err = w.WriteUint32(0); nil != err {
		panic(err)
	}

	// capability flags
	cpv := p.CapabilityFlags | clientProtocol41
	if len(p.Database) != 0 {
		cpv |= clientConnectWithDB
	}
	if err = w.WriteUint32(cpv); nil != err {
		panic(err)
	}
	// max packet size, always 0
	if err = w.WriteUint32(0); nil != err {
		panic(err)
	}
	// charset
	if err = w.WriteUint8(p.Charset); nil != err {
		panic(err)
	}
	// TODO: tls/ssl
	// 23bytes reserved
	reserved := [23]byte{}
	if err = w.WriteBytes(reserved[:]); nil != err {
		panic(err)
	}
	// username + null
	if err = w.WriteStringWithTerm(p.Username); nil != err {
		panic(err)
	}
	// len + password
	if nil == p.passEncoded {
		if err = w.WriteUint8(0); nil != err {
			panic(err)
		}
	} else {
		if err = w.WriteLenBytes(p.passEncoded); nil != err {
			panic(err)
		}
	}
	// db name + null
	if err = w.WriteStringWithTerm(p.Database); nil != err {
		panic(err)
	}
	// client auth plugin
	if err = w.WriteBytes([]byte(MySQLNativePasswordPlugin)); nil != err {
		panic(err)
	}

	return w.Bytes()
}

// Deprecated version
/*// Encode serialize the handshake response
func (p *PacketHandshakeResponse) Encode() []byte {
	p.passEncoded = p.encodePass(p.AuthPluginData)

	sz := p.esitimateSize()
	data := make([]byte, sz)
	// Skip the payload length, auto fill by WritePacket
	wptr := 4

	// capability flags
	cpv := p.CapabilityFlags | clientProtocol41
	if len(p.Database) != 0 {
		cpv |= clientConnectWithDB
	}
	binary.LittleEndian.PutUint32(data[wptr:], cpv)
	wptr += 4
	// max packet size, always 0
	binary.LittleEndian.PutUint32(data[wptr:], 0)
	wptr += 4
	// charset
	data[wptr] = p.Charset
	wptr++
	// TODO: tls/ssl
	// 23bytes reserved
	wptr += 23
	// username + null
	copy(data[wptr:], []byte(p.Username))
	wptr += len(p.Username) + 1
	// len + password
	if nil == p.passEncoded {
		data[wptr] = 0
		wptr++
	} else {
		data[wptr] = uint8(len(p.passEncoded))
		wptr++
		copy(data[wptr:], p.passEncoded)
		wptr += len(p.passEncoded)
	}
	// db name + null
	if len(p.Database) != 0 {
		copy(data[wptr:], []byte(p.Database))
		wptr += len(p.Database) + 1
	}
	// client auth plugin
	copy(data[wptr:], []byte(MySQLNativePasswordPlugin))
	wptr += len(MySQLNativePasswordPlugin)

	return data
}*/

// PacketOK parses mysql OK_Packet
// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
type PacketOK struct {
	Header uint8
	// OK packet fields
	AffectedRows uint64
	LastInsertID uint64
	StatusFlags  uint16 // StatusFlags has value : client protocol 41 or client transactions
	Warnings     uint16
	// EOF
	EOF bool
	// Result set
	ColumnCount LenencInt
	Results     *ResultSet
	// Parsing context
	capabilityFlags uint32
}

func (p *PacketOK) isOK() bool {
	return p.Header == PacketHeaderOK
}

// Decode decodes binary data to mysql packet
func (p *PacketOK) Decode(data []byte) error {
	var err error
	r := serialize.NewBinReader(data)

	// Check the ok packet is a eof packet
	if p.Header == PacketHeaderEOF && len(data) < 9 {
		p.EOF = true
		return nil
	}

	p.AffectedRows, err = r.ReadLenencInt()
	if nil != err {
		return errors.Trace(err)
	}
	p.LastInsertID, err = r.ReadLenencInt()
	if nil != err {
		return errors.Trace(err)
	}

	if (p.capabilityFlags & clientProtocol41) != 0 {
		p.StatusFlags, err = r.ReadUint16()
		if nil != err {
			return errors.Trace(err)
		}
		// Reading the next status message
		p.Warnings = 0
	} else if (p.capabilityFlags & clientTransactions) != 0 {
		p.StatusFlags, err = r.ReadUint16()
		if nil != err {
			return errors.Trace(err)
		}
	}

	// If has not ClientSessionTrace, left part is the warning message
	if 0 == (clientSessionTrace & p.capabilityFlags) {

	}

	// TODO: parsing the left fields

	r.End()
	return nil
}

// PacketErr parses mysql OK_Packet
// https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
type PacketErr struct {
	Header uint8
	// ERR packet fields
	ErrorCode    uint16
	State        string
	ErrorMessage string
	// Parsing context
	capabilityFlags uint32
}

func (p *PacketErr) toError() error {
	return errors.New(p.ErrorMessage)
}

// Decode decodes binary data to mysql packet
func (p *PacketErr) Decode(data []byte) error {
	var err error
	r := serialize.NewBinReader(data)

	p.Header, err = r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	if p.Header != PacketHeaderERR {
		return errors.Errorf("Not a packet err, header = %v", p.Header)
	}

	p.ErrorCode, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}

	if 0 != (p.capabilityFlags & clientProtocol41) {
		// Skip marker of the sql state
		_, err = r.ReadUint8()
		if nil != err {
			return errors.Trace(err)
		}
		p.State, err = r.ReadStringWithLen(5)
		if nil != err {
			return errors.Trace(err)
		}
	}
	// Reading the error message until eof
	p.ErrorMessage, err = r.ReadEOFString()
	if nil != err {
		return errors.Trace(err)
	}
	r.End()

	return nil
}

// PacketComStr is used to send the server a text-based query that is executed immediately
type PacketComStr struct {
	command string
}

// Encode encodes the packet to binary data
func (p *PacketComStr) Encode() ([]byte, error) {
	buflen := len(p.command) + 1 + 4
	data := make([]byte, buflen)

	w := serialize.NewBinWriter(data)
	if err := w.WriteUint32(0); nil != err {
		panic(err)
	}
	if err := w.WriteUint8(comQuery); nil != err {
		panic(err)
	}
	if err := w.WriteEOFString(p.command); nil != err {
		panic(err)
	}

	return w.Bytes(), nil
}

// PacketEOF represents the eof of packets
type PacketEOF struct {
	warnings uint16
	status   uint16
}

// Decode decodes binary data to mysql packet
func (p *PacketEOF) Decode(data []byte) error {
	if len(data) != 5 {
		return ErrMalformPacket
	}
	r := serialize.NewBinReader(data)
	header, err := r.ReadUint8()
	if nil != err {
		return errors.Trace(err)
	}
	if header != PacketHeaderEOF {
		return errors.New("not a eof packet")
	}
	p.warnings, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}
	p.status, err = r.ReadUint16()
	if nil != err {
		return errors.Trace(err)
	}

	return nil

	/*if data[0] != PacketHeaderEOF {
		return errors.New("not a eof packet")
	}
	p.warnings = binary.LittleEndian.Uint16(data[1:])
	p.status = binary.LittleEndian.Uint16(data[3:])
	return nil*/
}

// PacketRegisterSlave register slave to master
// https://dev.mysql.com/doc/internals/en/com-register-slave.html
type PacketRegisterSlave struct {
	ServerID uint32
	Hostname string
	User     string
	Password string
	Port     uint16
	Rank     uint32
	MasterID uint32
}

// Encode encodes the packet to binary data
func (p *PacketRegisterSlave) Encode() ([]byte, error) {
	l := 4 + 1 + 4 + 1 + len(p.Hostname) + 1 + len(p.User) + 1 + len(p.Password) + 2 + 4 + 4
	data := make([]byte, l)

	w := serialize.NewBinWriter(data)
	if err := w.WriteUint32(0); nil != err {
		return nil, errors.Trace(err)
	}

	if err := w.WriteUint8(comRegisterSlave); nil != err {
		return nil, errors.Trace(err)
	}
	// the slaves server-id
	if err := w.WriteUint32(p.ServerID); nil != err {
		return nil, errors.Trace(err)
	}
	// see --report-host, usually empty
	if err := w.WriteLenString(p.Hostname); nil != err {
		return nil, errors.Trace(err)
	}
	// see --report-host, usually empty
	if err := w.WriteLenString(p.User); nil != err {
		return nil, errors.Trace(err)
	}
	// see --report-password, usually empty
	if err := w.WriteLenString(p.Password); nil != err {
		return nil, errors.Trace(err)
	}
	// see --report-port, usually empty
	if err := w.WriteUint16(p.Port); nil != err {
		return nil, errors.Trace(err)
	}
	// ignored rank
	if err := w.WriteUint32(p.Rank); nil != err {
		return nil, errors.Trace(err)
	}
	// usually 0. Appears as "master id" in SHOW SLAVE HOSTS on the master. Unknown what else it impacts
	if err := w.WriteUint32(p.MasterID); nil != err {
		return nil, errors.Trace(err)
	}
	return w.Bytes(), nil

	/*wptr := 4
	data[wptr] = comRegisterSlave
	wptr++

	// the slaves server-id
	binary.LittleEndian.PutUint32(data[wptr:], p.ServerID)
	wptr += 4

	// see --report-host, usually empty
	wptr += writeLengthPrefixString(data[wptr:], p.Hostname)
	// see --report-user, usually empty
	wptr += writeLengthPrefixString(data[wptr:], p.User)
	// see --report-password, usually empty
	wptr += writeLengthPrefixString(data[wptr:], p.Password)
	// see --report-port, usually empty
	binary.LittleEndian.PutUint16(data[wptr:], p.Port)
	wptr += 2
	// ignored
	binary.LittleEndian.PutUint32(data[wptr:], p.Rank)
	wptr += 4
	// usually 0. Appears as "master id" in SHOW SLAVE HOSTS on the master. Unknown what else it impacts
	binary.LittleEndian.PutUint32(data[wptr:], p.MasterID)

	return data, nil*/
}

// PacketBinlogDump to enable replication
type PacketBinlogDump struct {
	BinlogPos  uint32
	Flags      uint16
	ServerID   uint32
	BinlogFile string
}

// Encode encodes the packet to binary data
func (p *PacketBinlogDump) Encode() ([]byte, error) {
	l := 4 + 1 + 4 + 2 + 4 + len(p.BinlogFile)
	data := make([]byte, l)
	w := serialize.NewBinWriter(data)

	if err := w.WriteUint32(0); nil != err {
		return nil, errors.Trace(err)
	}
	if err := w.WriteUint8(comBinlogDump); nil != err {
		return nil, errors.Trace(err)
	}
	if err := w.WriteUint32(p.BinlogPos); nil != err {
		return nil, errors.Trace(err)
	}
	if err := w.WriteUint16(p.Flags); nil != err {
		return nil, errors.Trace(err)
	}
	if err := w.WriteUint32(p.ServerID); nil != err {
		return nil, errors.Trace(err)
	}
	if err := w.WriteEOFString(p.BinlogFile); nil != err {
		return nil, errors.Trace(err)
	}

	return w.Bytes(), nil
}

// Deprecated version
/*// Encode encodes the packet to binary data
func (p *PacketBinlogDump) Encode() ([]byte, error) {
	l := 4 + 1 + 4 + 2 + 4 + len(p.BinlogFile)
	data := make([]byte, l)

	wptr := 4
	data[wptr] = comBinlogDump
	wptr++
	binary.LittleEndian.PutUint32(data[wptr:], p.BinlogPos)
	wptr += 4
	binary.LittleEndian.PutUint16(data[wptr:], p.Flags)
	wptr += 2
	binary.LittleEndian.PutUint32(data[wptr:], p.ServerID)
	wptr += 4
	if len(p.BinlogFile) > 0 {
		copy(data[wptr:], p.BinlogFile)
	}
	return data, nil
}*/
