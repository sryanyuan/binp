package slave

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"

	"github.com/juju/errors"
)

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
	AUthPluginName     string
}

// Decode read binary data into handshake packet
// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
func (p *PacketHandshake) Decode(data []byte) error {
	dl := len(data)
	// protocol version
	if dl < 1 {
		return errors.New("parse protocol version error")
	}
	rptr := 0
	p.ProtocolVersion = data[0]
	rptr++
	// server version
	eptr := bytes.IndexByte(data[1:], 0)
	if eptr < 0 {
		return errors.New("parse server version error")
	}
	p.ServerVersion = string(data[1 : eptr+1])
	rptr = eptr + 1 + 1
	// connection id
	if rptr+3 >= dl {
		return errors.New("parse connection id error")
	}
	p.ConnectionID = binary.LittleEndian.Uint32(data[rptr:])
	rptr += 4
	// auth plugin data part 1
	if rptr+7 >= dl {
		return errors.New("parse auth plungin data part 1 error")
	}
	p.AuthPluginDataPart = make([]byte, 0, 8+13)
	copy(p.AuthPluginDataPart[:], data[rptr:rptr+8])
	rptr += 8
	// filter
	if rptr >= dl {
		return errors.New("parse filter error")
	}
	p.Filter = data[rptr]
	rptr++
	// capability_flag_1 lower bytes
	if rptr+1 >= dl {
		return errors.New("parse capability_flag_1 error")
	}
	var capabilityFlagsBytes [4]byte
	capabilityFlagsBytes[0] = data[rptr]
	capabilityFlagsBytes[1] = data[rptr+1]
	rptr += 2
	// character set
	if rptr >= dl {
		// No more data
		p.CapabilityFlags = uint32(binary.LittleEndian.Uint16(capabilityFlagsBytes[2:]))
		return nil
	}
	p.CharacterSet = data[rptr]
	rptr++
	// status flags
	if rptr+1 >= dl {
		return errors.New("parse status flags error")
	}
	p.StatusFlags = binary.LittleEndian.Uint16(data[rptr:])
	rptr += 2
	// capability flags 2 upper bytes
	if rptr+1 >= dl {
		return errors.New("parse capability_flag_2 error")
	}
	capabilityFlagsBytes[2] = data[rptr]
	capabilityFlagsBytes[3] = data[rptr+1]
	p.CapabilityFlags = binary.LittleEndian.Uint32(capabilityFlagsBytes[:])
	rptr += 2
	// auth plugin data len
	authPluginDataLen := uint8(0)
	if (p.CapabilityFlags & ClientPluginAuth) != 0 {
		if rptr >= dl {
			return errors.New("parse auth plugin data len error")
		}
		authPluginDataLen = data[rptr]
		rptr++
	} /*else {
		// Always byte 0
		if rptr >= dl {
			return errors.New("parse auth plugin data len error")
		}
		authPluginDataLen = data[rptr]
		rptr++
	}*/
	// Reserved string(10)
	if rptr+9 >= dl {
		return errors.New("parse reserved error")
	}
	rptr += 10
	// auth plugin data part2
	if (p.CapabilityFlags & ClientSecureConnection) != 0 {
		// auth plugin data part2 length is mutable. $len=MAX(13, length of auth-plugin-data - 8)
		p2len := authPluginDataLen - 8
		if p2len > 13 {
			p2len = 13
		}
		if rptr+int(p2len)-1 >= dl {
			return errors.New("parse auth plugin data 2 error")
		}
		p.AuthPluginDataPart = append(p.AuthPluginDataPart, data[rptr:rptr+int(p2len)]...)
		rptr += int(p2len)
	}
	// auth-plugin name
	if (p.CapabilityFlags & ClientPluginAuth) != 0 {
		// Find null
		eptr = bytes.IndexByte(data[rptr:], 0)
		if eptr < 0 {
			return errors.New("parse auth plugin name failed")
		}
		p.AUthPluginName = string(data[rptr : rptr+eptr])
		rptr = rptr + eptr
	}
	// Check to the terminal
	if rptr+1 != dl {
		return errors.New("invalid eof")
	}

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

	s1 := sha1.New()
	s1.Write(key)
	s1hash := s1.Sum(nil)

	s1.Reset()
	s1.Write(s1hash)
	shash := s1.Sum(nil)

	s1.Reset()
	s1.Write([]byte(p.Password))
	s1.Write(shash)
	phash := s1.Sum(nil)

	for i := range phash {
		phash[i] ^= s1hash[i]
	}
	return phash
}

// Encode serialize the handshake response
func (p *PacketHandshakeResponse) Encode(key []byte) []byte {
	p.passEncoded = p.encodePass(key)

	sz := p.esitimateSize()
	data := make([]byte, sz)
	// Skip the payload length, auto fill by WritePacket
	wptr := 4

	// capability flags
	cpv := p.CapabilityFlags | ClientProtocol41
	if len(p.Database) != 0 {
		cpv |= ClientConnectWithDB
	}
	binary.LittleEndian.PutUint32(data[wptr:], cpv)
	wptr += 4
	// max packet size, always 0
	binary.LittleEndian.PutUint32(data[wptr:], 0)
	wptr += 4
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
}

type PacketOK struct {
	Header       uint8
	AffectedRows LenencInt
	LastInsertID LenencInt
	StatusFlags  uint16 // StatusFlags has value : client protocol 41 or client transactions
	Warnings     uint16
}
