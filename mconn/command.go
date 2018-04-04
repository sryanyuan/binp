package mconn

import "github.com/juju/errors"

// Exec execute the text-query command or bingding data command
func (c *Conn) Exec(command string, args ...interface{}) (*PacketOK, error) {
	if len(args) == 0 {
		return c.execCommandStr(command)
	}
	return nil, nil
}

func (c *Conn) sendCommandStr(str string) error {
	var pcq PacketComStr
	pcq.command = str

	data, err := pcq.Encode()
	if nil != err {
		return errors.Trace(err)
	}
	c.resetSequence()
	if err = c.WritePacket(data); nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (c *Conn) execCommandStr(str string) (*PacketOK, error) {
	if err := c.sendCommandStr(str); nil != err {
		return nil, errors.Trace(err)
	}

	return c.readResponse(&responseOptions{binary: false})
}
