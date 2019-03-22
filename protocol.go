package goscheduler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/darwinsimon/goscheduler/command"
	"github.com/golang/snappy"
)

type flusher interface {
	Flush() error
}

type Protocol struct {
	addr string

	index int
	c     net.Conn
	r     io.Reader
	w     io.Writer

	delegator Delegator

	readDeadline  time.Duration
	writeDeadline time.Duration

	mtx    sync.Mutex
	logger logger
	logLvl LogLevel

	cmdChan  chan *command.Command
	exitChan chan int
}

type ProtocolConfig struct {
	Index         int
	Conn          net.Conn
	Delegator     Delegator
	ReadDeadline  time.Duration
	WriteDeadline time.Duration
}

func NewProtocol(config ProtocolConfig) *Protocol {
	p := &Protocol{
		addr: config.Conn.LocalAddr().String(),

		index: config.Index,
		c:     config.Conn,
		r:     snappy.NewReader(config.Conn),
		w:     snappy.NewWriter(config.Conn),

		delegator: config.Delegator,

		cmdChan:  make(chan *command.Command),
		exitChan: make(chan int),

		readDeadline:  config.ReadDeadline,
		writeDeadline: config.WriteDeadline,
	}

	// Set default deadline
	if p.readDeadline.Nanoseconds() == 0 {
		p.readDeadline = 5 * time.Second
	}
	if p.writeDeadline.Nanoseconds() == 0 {
		p.writeDeadline = 5 * time.Second
	}

	go p.readLoop()

	return p
}

// Read performs a deadlined read on the underlying TCP connection
func (p *Protocol) Read(d []byte) (int, error) {
	p.c.SetReadDeadline(time.Now().Add(p.readDeadline))
	return p.r.Read(d)
}

// Write performs a deadlined write on the underlying TCP connection
func (p *Protocol) Write(d []byte) (int, error) {
	p.c.SetWriteDeadline(time.Now().Add(p.writeDeadline))
	return p.w.Write(d)
}

// Flush writes all buffered data to the underlying TCP connection
func (p *Protocol) Flush() error {
	if f, ok := p.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

// SetLogger assigns the logger to use as well as a level.
func (p *Protocol) SetLogger(l logger, lvl LogLevel) {
	p.logger = l
	p.logLvl = lvl
}

// WriteCommand is a goroutine safe method to write a Command
// to this connection, and flush.
func (p *Protocol) WriteCommand(cmd *command.Command) error {
	p.mtx.Lock()

	_, err := cmd.WriteTo(p)
	if err != nil {
		goto writeCommandExit
	}
	err = p.Flush()

writeCommandExit:

	p.mtx.Unlock()

	log.Println("write command for", string(cmd.Name), string(cmd.Body))
	if err != nil {
		p.log(LogLevelError, "%s", err)
		p.delegator.OnIOError(p.index)
	}
	return err
}

func (p *Protocol) readLoop() {

	for {
		streamType, data, err := readResponse(p.r)
		if err != nil {
			p.log(LogLevelError, "%v", err)
			p.delegator.OnIOError(p.index)

			goto exit

		}

		// p.log(LogLevelDebug, "Received message %s", string(data))

		switch streamType {
		case command.StreamTypeHeartbeat:
			continue
		case command.StreamTypeRequest:
			p.delegator.OnRequestReceived(p.index, data)
		case command.StreamTypeResponse:

			// c.delegate.OnMessage(c, "msg")

		case command.StreamTypeJob:

			p.delegator.OnJobReceived(data)

		default:

		}
	}

exit:

	p.close()
}

func (p *Protocol) close() {

	close(p.exitChan)
	p.c.Close()

}

func (p *Protocol) log(lvl LogLevel, line string, args ...interface{}) {
	if p.logger == nil {
		return
	}

	if p.logLvl > lvl {
		return
	}

	p.logger.Output(2, fmt.Sprintf("%-4s %s %s", lvl, p.addr, fmt.Sprintf(line, args...)))
}

//    [x][x][x][x][x][x][x][x][x]...
//    |  (int32) || || (binary)
//    |  4-byte  || || N-byte
//                ^^^
//               1-byte
//    ------------------------------------
//        size     type    data
func readResponse(r io.Reader) (byte, []byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return 0, nil, err
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		log.Println(err)
		return 0, nil, err
	}

	if len(buf) < 2 {
		return 0, nil, errors.New("length of response is too small")
	}

	return buf[0], buf[1:], nil

}
