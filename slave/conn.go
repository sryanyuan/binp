package slave

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/juju/errors"
)

const (
	_ = iota
	connStatusConnected
	connStatusClosed
)

const (
	badConnErrDesc   = "bad connection: "
	maxPayloadLength = 1<<24 - 1
)

var (
	// ErrMalformPacket represents a response with the unknown header
	ErrMalformPacket = errors.New("Malform packet format")
)

type responseOptions struct {
	binary bool
}

type slaveConn struct {
	mu            sync.Mutex
	status        int64
	lastErr       error
	conn          net.Conn
	r             io.Reader
	seq           uint8
	capacityFlags uint32
	mstatus       MasterStatus
	// TODO: support tls
	tlsConfig *tls.Config
}

func (c *slaveConn) GetStatus() int64 {
	c.mu.Lock()
	s := c.status
	c.mu.Unlock()
	return s
}

func (c *slaveConn) SetStatus(v int64) {
	c.mu.Lock()
	c.status = v
	c.mu.Unlock()
}

func (c *slaveConn) Close() {
	c.mu.Lock()
	c.close()
	c.mu.Unlock()
}

func (c *slaveConn) close() {
	if c.status == connStatusConnected &&
		c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.status = connStatusClosed
	}
}

func (c *slaveConn) getMasterStatus() *MasterStatus {
	return &c.mstatus
}

func (c *slaveConn) Connect(host string, port uint16, username, password, database string) error {
	c.mu.Lock()
	if c.status == connStatusConnected {
		c.mu.Unlock()
		return errors.New("already connected")
	}

	// Create connection
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), time.Second*10)
	if nil != err {
		c.mu.Unlock()
		return errors.Trace(err)
	}

	c.conn = conn
	c.r = bufio.NewReader(c.conn)
	c.status = connStatusConnected

	// Receive handshake from server
	if err = c.handshake(username, password, database); nil != err {
		c.close()
		c.mu.Unlock()
		return errors.Trace(err)
	}

	return nil
}

func (c *slaveConn) execute(command string, args ...interface{}) (*PacketOK, error) {
	if len(args) == 0 {
		return c.executeCommand(command)
	}
	return nil, nil
}

func (c *slaveConn) executeCommand(command string) (*PacketOK, error) {
	var pcq PacketComQuery
	pcq.command = command

	data, err := pcq.Encode()
	if nil != err {
		return nil, errors.Trace(err)
	}
	if err = c.WritePacket(data); nil != err {
		return nil, errors.Trace(err)
	}

	return c.readResponse(&responseOptions{binary: false})
}

// readResponse parses the response data, all response data is start with the flag
// the query response can be one of <ERR PACKET> <OK PACKET> <LOCAL_INFILE REQUEST> <ResultSet>
// https://dev.mysql.com/doc/internals/en/com-query-response.html
func (c *slaveConn) readResponse(options *responseOptions) (*PacketOK, error) {
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

// readResultSet decodes the text result set
// https://dev.mysql.com/doc/internals/en/com-query-response.html
func (c *slaveConn) readResultSet(data []byte, options *responseOptions) (*PacketOK, error) {
	// Skip header
	wptr := 1
	// Reading column count
	var ln LenencInt
	offset := ln.FromData(data[wptr:])
	if offset+1 != len(data)+1 {
		return nil, ErrMalformPacket
	}
	return nil, nil
}

func (c *slaveConn) handshake(username, password, database string) error {
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
	capability := handshake.CapabilityFlags
	capability |= clientProtocol41
	capability |= clientSecureConnection
	capability |= clientLongPassword
	capability |= clientTransactions
	capability |= clientLongFlag
	capability &= handshake.CapabilityFlags
	rsp.CapabilityFlags = handshake.CapabilityFlags
	rsp.AuthPluginData = handshake.AuthPluginDataPart
	rspData := rsp.Encode()
	if err = c.WritePacket(rspData); nil != err {
		return errors.Trace(err)
	}
	c.capacityFlags = capability

	// Update master status
	c.mstatus.Version = handshake.ServerVersion

	// Read server response
	data, err := c.ReadPacket()
	if nil != err {
		return errors.Trace(err)
	}
	switch data[0] {
	case packetHeaderOK:
		{
			if _, err = c.readPacketOK(data); nil != err {
				return errors.Trace(err)
			}
			// Handshake done
		}
	case packetHeaderERR:
		{
			perr, err := c.readPacketERR(data)
			if nil != err {
				return errors.Trace(err)
			}
			return errors.New(perr.ErrorMessage)
		}
	}

	return nil
}

func (c *slaveConn) readPacketOK(data []byte) (*PacketOK, error) {
	var pok PacketOK
	pok.capabilityFlags = c.capacityFlags
	if err := pok.Decode(data); nil != err {
		return nil, errors.Trace(err)
	}
	return &pok, nil
}

func (c *slaveConn) readPacketERR(data []byte) (*PacketErr, error) {
	var perr PacketErr
	perr.capabilityFlags = c.capacityFlags
	if err := perr.Decode(data); nil != err {
		return nil, errors.Trace(err)
	}
	return &perr, nil
}

//
func (c *slaveConn) readHandshake() (*PacketHandshake, error) {
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

func (c *slaveConn) readOK() (*PacketOK, error) {
	return nil, nil
}

func (c *slaveConn) WritePacket(data []byte) error {
	var header PacketHeader
	dl := len(data) - 4

	for dl >= maxPayloadLength {
		header.SetLength(0x00ffffff)
		header.SetSequence(c.seq)
		copy(data[:], header.ToSlice())

		if n, err := c.conn.Write(data[:maxPayloadLength+4]); nil != err {
			return errors.Annotate(err, badConnErrDesc)
		} else if n != maxPayloadLength+4 {
			return errors.Errorf("%s%s", badConnErrDesc, "send bytes not equal")
		} else {
			c.seq++
			dl -= maxPayloadLength
			// Next round the position is before the actual payload data
			data = data[maxPayloadLength:]
		}
	}

	header.SetLength(dl)
	header.SetSequence(c.seq)
	copy(data[:], header.ToSlice())
	if n, err := c.conn.Write(data[:]); nil != err {
		return errors.Annotate(err, badConnErrDesc)
	} else if n != dl+4 {
		return errors.Errorf("%s%s", badConnErrDesc, "send bytes not equal")
	} else {
		c.seq++
	}

	return nil
}

func (c *slaveConn) ReadPacket() ([]byte, error) {
	var buf bytes.Buffer
	if err := c.readFullPacket(&buf); nil != err {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func (c *slaveConn) readFullPacket(w io.Writer) error {
	var err error
	var header PacketHeader

	if _, err = io.ReadFull(c.r, header.ToSlice()[:]); nil != err {
		return errors.Annotatef(err, badConnErrDesc)
	}

	length := int64(header.GetLength())
	if length < 1 {
		return errors.Errorf("invalid packet length %v", length)
	}
	seq := header.GetSequence()
	if seq != c.seq {
		return errors.Errorf("connection sequence mismatch, conn = %v, packet = %v", c.seq, seq)
	}

	// Once receive the packet, increase the connection's sequence
	c.seq++

	// Read the payload body into w
	n, err := io.CopyN(w, c.r, length)
	if nil != err {
		return errors.Annotatef(err, badConnErrDesc)
	}
	if n != length {
		return errors.Errorf("%sread pay load length %v, want %v", badConnErrDesc, n, length)
	}
	// If packet length is max payload length, we should wait for the next packet
	if length >= maxPayloadLength {
		// Continue reading
		if err = c.readFullPacket(w); nil != err {
			return errors.Annotate(err, badConnErrDesc)
		}
	}

	return nil
}
