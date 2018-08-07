package mconn

import (
	"fmt"
	"os"

	"github.com/juju/errors"
)

// DataSource is a mysql instance to pull binlog stream
type DataSource struct {
	Host     string `json:"host" toml:"host"`
	Port     uint16 `json:"port" toml:"port"`
	Username string `json:"username" toml:"username"`
	Password string `json:"password" toml:"password"`
}

// Address returns the address of the data source
func (s *DataSource) Address() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

// DBConfig is the connection information to db server
type DBConfig struct {
	Type     string `json:"type" toml:"type"`
	Host     string `json:"host" toml:"host"`
	Port     uint16 `json:"port" toml:"port"`
	Username string `json:"username" toml:"username"`
	Password string `json:"password" toml:"password"`
	Charset  string `json:"charset" toml:"charset"`
}

// ReplicationConfig specify the master information
type ReplicationConfig struct {
	SlaveID uint32 `json:"slave-id" toml:"slave-id"`
	//Pos             Position `json:"position" toml:"position"`
	EnableGtid      bool `json:"enable-gtid" toml:"enable-gtid"`
	EventBufferSize int  `json:"event-buffer-size" toml:"event-buffer-size"`
	KeepAlivePeriod int  `json:"keepalive-period" toml:"keepalive-period"`
}

// Position represents a binlog replication position, slave can
// start sync with the position
type ReplicationPoint struct {
	Filename string
	Offset   uint32
	Gtid     string
}

// RegisterSlave register the connection as a slave connection
func (c *Conn) RegisterSlave(rc *ReplicationConfig) error {
	c.rc = rc
	// Write register command
	if err := c.sendRegistgerSlaveCommand(); nil != err {
		return err
	}
	// Read reply
	if _, err := c.readOK(); nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (c *Conn) sendRegistgerSlaveCommand() error {
	c.resetSequence()

	var prs PacketRegisterSlave
	prs.Hostname, _ = os.Hostname()

	prs.ServerID = uint32(c.rc.SlaveID)
	prs.User = c.ds.Username
	prs.Password = c.ds.Password

	data, err := prs.Encode()
	if nil != err {
		return errors.Trace(err)
	}

	if err = c.WritePacket(data); nil != err {
		return errors.Trace(err)
	}

	return nil
}

// StartDumpBinlog start dump binlog from master
func (c *Conn) StartDumpBinlog(pos ReplicationPoint) error {
	var err error

	if c.rc.EnableGtid {
		return errors.New("Gtid replication not support now")
	}

	if err = c.sendBinlogDumpCommand(pos); nil != err {
		return errors.Trace(err)
	}

	return nil
}

func (c *Conn) sendBinlogDumpCommand(pos ReplicationPoint) error {
	c.resetSequence()

	var pbd PacketBinlogDump
	pbd.BinlogPos = pos.Offset
	pbd.BinlogFile = pos.Filename
	pbd.ServerID = uint32(c.rc.SlaveID)
	// Just one flag:
	// Description
	// 0x01 BINLOG_DUMP_NON_BLOCK
	// if there is no more event to send send a EOF_Packet instead of blocking the connection
	pbd.Flags = 0
	data, err := pbd.Encode()
	if nil != err {
		return errors.Trace(err)
	}

	if err = c.WritePacket(data); nil != err {
		return errors.Trace(err)
	}

	return nil
}

//func (c *Conn) read
