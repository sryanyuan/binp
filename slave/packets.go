package slave

import (
	"bytes"
	"encoding/binary"

	"github.com/juju/errors"
)

const (
	CLIENT_PLUGIN_AUTH = iota
)

// PacketHeader is the header of the mysql packet to read the packet length
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

// GetSequence returns the header sequence
func (p *PacketHeader) GetSequence() uint8 {
	return uint8(p.ToSlice()[3])
}

// PacketHandshake is handshake packet sending from server
type PacketHandshake struct {
	ProtocolVersion    uint8
	ServerVersion      string
	ConnectionID       uint32
	AuthPluginDataPart string
	Filter             uint8
	CapabilityFlags    uint32
	CharacterSet       uint8
	StatusFlags        uint16
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
	rptr = eptr + 1
	// connection id
	if rptr+3 >= dl {
		return errors.New("parse connection id error")
	}
	p.ConnectionID = binary.BigEndian.Uint32(data[rptr:])
	rptr += 4
	// auth plugin data part 1
	if rptr+7 >= dl {
		return errors.New("parse auth plungin data part 1 error")
	}
	p.AuthPluginDataPart = string(data[rptr : rptr+8])
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
	capabilityFlagsBytes[2] = data[rptr]
	capabilityFlagsBytes[3] = data[rptr+1]
	rptr += 2
	// character set
	if rptr >= dl {
		// No more data
		p.CapabilityFlags = uint32(binary.BigEndian.Uint16(capabilityFlagsBytes[2:]))
		return nil
	}
	p.CharacterSet = data[rptr]
	rptr++
	// status flags
	if rptr+1 >= dl {
		return errors.New("parse status flags error")
	}
	p.StatusFlags = binary.BigEndian.Uint16(data[rptr:])
	rptr += 2
	// capability flags 2 upper bytes
	if rptr+1 >= dl {
		return errors.New("pase capability_flag_2 error")
	}
	capabilityFlagsBytes[0] = data[rptr]
	capabilityFlagsBytes[1] = data[rptr+1]
	p.CapabilityFlags = binary.BigEndian.Uint32(capabilityFlagsBytes[:])
	// auth plugin data len

	return nil
}
