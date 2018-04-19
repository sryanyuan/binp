package mconn

import (
	"os"

	"github.com/juju/errors"
)

// ReplicationConfig specify the master information
type ReplicationConfig struct {
	Host            string   `json:"host" toml:"host"`
	Port            uint16   `json:"port" toml:"port"`
	Username        string   `json:"username" toml:"username"`
	Password        string   `json:"password" toml:"password"`
	Charset         string   `json:"charset" toml:"charset"`
	SlaveID         uint32   `json:"slave-id" toml:"slave-id"`
	Pos             Position `json:"position" toml:"position"`
	EnableGtid      bool     `json:"enable-gtid" toml:"enable-gtid"`
	EventBufferSize int      `json:"event-buffer-size" toml:"event-buffer-size"`
	KeepAlivePeriod int      `json:"keepalive-period" toml:"keepalive-period"`
}

// Position represents a binlog replication position, slave can
// start sync with the position
type Position struct {
	Filename string
	Offset   uint32
	Gtid     string
}

// RegisterSlave register the connection as a slave connection
func (c *Conn) RegisterSlave(cfg *ReplicationConfig) error {
	c.cfg = cfg
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

	prs.ServerID = uint32(c.cfg.SlaveID)
	prs.User = c.cfg.Username
	prs.Password = c.cfg.Password

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
func (c *Conn) StartDumpBinlog(pos Position) error {
	var err error

	if c.cfg.EnableGtid {
		return errors.New("Gtid replication not support now")
	} else {
		if err = c.sendBinlogDumpCommand(pos); nil != err {
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *Conn) sendBinlogDumpCommand(pos Position) error {
	c.resetSequence()

	var pbd PacketBinlogDump
	pbd.BinlogPos = pos.Offset
	pbd.BinlogFile = pos.Filename
	pbd.ServerID = uint32(c.cfg.SlaveID)
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
