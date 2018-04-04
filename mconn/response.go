package mconn

import "github.com/juju/errors"

// readResponse parses the response data, all response data is start with the flag
// the query response can be one of <ERR PACKET> <OK PACKET> <LOCAL_INFILE REQUEST> <ResultSet>
// https://dev.mysql.com/doc/internals/en/com-query-response.html
func (c *Conn) readResponse(options *responseOptions) (*PacketOK, error) {
	rspData, err := c.ReadPacket()
	if nil != err {
		return nil, errors.Trace(err)
	}

	switch rspData[0] {
	case packetHeaderOK:
		{
			pok, err := c.readPacketOK(rspData)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return pok, nil
		}
	case packetHeaderERR:
		{
			perr, err := c.readPacketERR(rspData)
			if nil != err {
				return nil, errors.Trace(err)
			}
			return nil, errors.New(perr.ErrorMessage)
		}
	case packetHeaderLocalInFile:
		{
			return nil, ErrMalformPacket
		}
	}

	// Read the following result set
	return c.readResultSet(rspData, options)
}

func (c *Conn) readPacketOK(data []byte) (*PacketOK, error) {
	var pok PacketOK
	pok.capabilityFlags = c.capability
	if err := pok.Decode(data); nil != err {
		return nil, errors.Trace(err)
	}
	return &pok, nil
}

func (c *Conn) readPacketERR(data []byte) (*PacketErr, error) {
	var perr PacketErr
	perr.capabilityFlags = c.capability
	if err := perr.Decode(data); nil != err {
		return nil, errors.Trace(err)
	}
	return &perr, nil
}

func (c *Conn) readOK() (*PacketOK, error) {
	return nil, nil
}
