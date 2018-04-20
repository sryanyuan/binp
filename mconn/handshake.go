package mconn

import "github.com/juju/errors"

func (c *Conn) handshake(username, password, database string) error {
	handshake, err := c.readHandshake()
	if nil != err {
		return errors.Trace(err)
	}

	if 0 == (handshake.CapabilityFlags & clientProtocol41) {
		return errors.New("protocol version < 4.1 is not supported")
	}
	if 0 == (handshake.CapabilityFlags & clientSecureConnection) {
		return errors.New("protocol only support secure connection")
	}

	// Send handshake response
	var rsp PacketHandshakeResponse
	rsp.Charset = CharsetUtf8GeneralCI
	rsp.Username = username
	rsp.Password = password
	rsp.Database = database
	capability := uint32(0)
	capability |= clientProtocol41
	capability |= clientSecureConnection
	capability |= clientLongPassword
	capability |= clientTransactions
	capability |= clientLongFlag
	capability &= handshake.CapabilityFlags
	//capability = 0x000aa285
	rsp.CapabilityFlags = capability
	rsp.AuthPluginData = handshake.AuthPluginDataPart
	rspData := rsp.Encode()
	if err = c.WritePacket(rspData); nil != err {
		return errors.Trace(err)
	}
	c.capability = capability

	// Read server response
	data, err := c.ReadPacket()
	if nil != err {
		return errors.Trace(err)
	}
	switch data[0] {
	case PacketHeaderOK:
		{
			if _, err = c.readPacketOK(data); nil != err {
				return errors.Trace(err)
			}
			// Handshake done
		}
	case PacketHeaderERR:
		{
			perr, err := c.readPacketERR(data)
			if nil != err {
				return errors.Trace(err)
			}
			return errors.New(perr.ErrorMessage)
		}
	}

	// Update server info
	c.si.ProtoVersion = handshake.ProtocolVersion
	c.si.ServerVersion = handshake.ServerVersion
	c.si.ConnectionID = handshake.ConnectionID

	return nil
}

//
func (c *Conn) readHandshake() (*PacketHandshake, error) {
	payloadData, err := c.ReadPacket()
	if nil != err {
		return nil, errors.Trace(err)
	}

	var packetHandshake PacketHandshake
	if err = packetHandshake.Decode(payloadData); nil != err {
		return nil, errors.Trace(err)
	}
	return &packetHandshake, nil
}
