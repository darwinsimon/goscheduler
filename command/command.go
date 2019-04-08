package command

import (
	"encoding/binary"
	"io"
)

var byteSpace = []byte(" ")

// Stream command type
const (
	StreamTypeRequest   byte = 1
	StreamTypeJob       byte = 2
	StreamTypeHeartbeat byte = 3
)

// List of available command string
const (
	CHeartbeat = "HEARTBEAT"
	CRegister  = "REG"
	CRemove    = "REMOVE"
)

type Command struct {
	Name []byte
	Body []byte
	Type byte
}

func (c *Command) WriteTo(w io.Writer) (int, error) {

	var size int32 = 1

	size += int32(len(c.Name))

	if c.Body != nil {
		if c.Name != nil {
			size++
		}

		size += int32(len(c.Body))
	}

	var sizeBuff [4]byte
	buffs := sizeBuff[:]
	binary.BigEndian.PutUint32(buffs, uint32(size))

	// Write size
	n, err := w.Write(buffs)
	total := n
	if err != nil {
		return total, err
	}

	// Write stream type
	n, err = w.Write([]byte{c.Type})
	total += n
	if err != nil {
		return total, err
	}

	if c.Name != nil {

		// Write name
		n, err = w.Write(c.Name)
		total += n
		if err != nil {
			return total, err
		}

	}

	if c.Body != nil {

		// Write separator
		if c.Name != nil {
			n, err = w.Write(byteSpace)
			total += n
			if err != nil {
				return total, err
			}
		}

		// Write body
		n, err = w.Write(c.Body)
		total += n
		if err != nil {
			return total, err
		}
	}

	return total, nil

}

// Heartbeat command
func Heartbeat() *Command {
	return &Command{[]byte(CHeartbeat), nil, StreamTypeHeartbeat}
}

// Register command new channel to consume
func Register(channel string) *Command {
	return &Command{[]byte(CRegister), []byte(channel), StreamTypeRequest}
}

// Remove command remove job based on id and channel
func Remove(channel, id string) *Command {
	return &Command{[]byte(CRemove), []byte(channel + " " + id), StreamTypeRequest}
}

// Job command
func Job(data []byte) *Command {
	return &Command{nil, data, StreamTypeJob}
}
