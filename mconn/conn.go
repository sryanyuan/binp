package mconn

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

// HandshakeInfo represents the mysql server information returned by handshake
type HandshakeInfo struct {
	ProtoVersion  byte
	ServerVersion string
	ConnectionID  uint32
}

func (i *HandshakeInfo) String() string {
	return fmt.Sprintf("Protocol version:%d Server version:%s Connection id:%d",
		i.ProtoVersion, i.ServerVersion, i.ConnectionID)
}

// Conn is a connection communicate with the mysql server
type Conn struct {
	ds          *DataSource
	mu          sync.Mutex
	status      int64
	lastErr     error
	conn        net.Conn
	r           io.Reader
	seq         uint8
	capability  uint32
	loc         *time.Location
	si          HandshakeInfo
	rc          *ReplicationConfig
	readTimeout time.Duration
	// TODO: support tls
	tlsConfig *tls.Config
}

// GetStatus returns the status of the connection
func (c *Conn) GetStatus() int64 {
	c.mu.Lock()
	s := c.status
	c.mu.Unlock()
	return s
}

// SetKeepalive enables the tcp keepalive
func (c *Conn) SetKeepalive(d time.Duration) bool {
	if tcpconn, ok := c.conn.(*net.TCPConn); ok {
		tcpconn.SetKeepAlive(true)
		tcpconn.SetKeepAlivePeriod(d)
		return true
	}
	return false
}

// GetHandshakeInfo get the server information by handshake
func (c *Conn) GetHandshakeInfo(si *HandshakeInfo) {
	*si = c.si
}

func (c *Conn) setStatus(v int64) {
	c.mu.Lock()
	c.status = v
	c.mu.Unlock()
}

// Close the connection
func (c *Conn) Close() {
	c.mu.Lock()
	c.close()
	c.mu.Unlock()
}

func (c *Conn) close() {
	if c.status == connStatusConnected &&
		c.conn != nil {
		c.conn.Close()
		c.status = connStatusClosed
	}
}

// Connect connects to the mysql server
func (c *Conn) Connect(ds *DataSource, database string) error {
	c.mu.Lock()
	c.ds = ds
	if c.status == connStatusConnected {
		c.mu.Unlock()
		return errors.New("already connected")
	}

	// Create connection
	conn, err := net.DialTimeout("tcp", ds.Address(), time.Second*10)
	if nil != err {
		c.mu.Unlock()
		return errors.Trace(err)
	}

	c.conn = conn
	c.r = bufio.NewReader(c)
	c.status = connStatusConnected
	c.resetSequence()

	// Receive handshake from server
	if err = c.handshake(ds.Username, ds.Password, database); nil != err {
		c.close()
		c.mu.Unlock()
		return errors.Trace(err)
	}

	c.mu.Unlock()
	return nil
}

// SetReadTimeout set the read timeout duration
func (c *Conn) SetReadTimeout(dura time.Duration) {
	c.readTimeout = dura
}

// Read implement io.Reader interface
func (c *Conn) Read(p []byte) (n int, err error) {
	if 0 != c.readTimeout {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	n, err = c.conn.Read(p)
	if 0 != c.readTimeout {
		c.conn.SetReadDeadline(time.Time{})
	}
	return
}

// WritePacket writes the packet to mysql, data must has 4 bytes unused in head
func (c *Conn) WritePacket(data []byte) error {
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

// ReadPacket reads the full packet from mysql
func (c *Conn) ReadPacket() ([]byte, error) {
	var buf bytes.Buffer
	if err := c.readFullPacket(&buf); nil != err {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func (c *Conn) readFullPacket(w io.Writer) error {
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
		//return errors.Errorf("connection sequence mismatch, conn = %v, packet = %v", c.seq, seq)
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

func (c *Conn) resetSequence() {
	c.seq = 0
}
