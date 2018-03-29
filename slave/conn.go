package slave

import (
	"bufio"
	"bytes"
	"encoding/hex"
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

type slaveConn struct {
	mu      sync.Mutex
	status  int64
	lastErr error
	conn    net.Conn
	r       io.Reader
	seq     uint8
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
	if c.status == connStatusConnected &&
		c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.status = connStatusClosed
	}
	c.mu.Unlock()
}

func (c *slaveConn) Connect(host string, port uint16, username, password string) error {
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

	// Send handshake
	if err = c.handshake(username, password); nil != err {
		c.mu.Unlock()
		return errors.Trace(err)
	}

	return nil
}

func (c *slaveConn) handshake(username, password string) error {
	_, err := c.readHandshake()
	if nil != err {
		return errors.Trace(err)
	}

	return nil
}

//
func (c *slaveConn) readHandshake() (*PacketHandshake, error) {
	payloadData, err := c.ReadPacket()
	if nil != err {
		return nil, errors.Trace(err)
	}
	fmt.Println(hex.EncodeToString(payloadData))

	var packetHandshake PacketHandshake
	if err = packetHandshake.Decode(payloadData); nil != err {
		return nil, errors.Trace(err)
	}
	return &packetHandshake, nil
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
