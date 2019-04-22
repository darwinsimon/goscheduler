package goscheduler

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/darwinsimon/goscheduler/command"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
)

type silentDelegator struct {
	countIOError           int
	countOnRequestReceived int
	countOnJobReceived     int
}

func (s *silentDelegator) OnConnClose()        {}
func (s *silentDelegator) OnIOError(index int) { s.countIOError++ }
func (s *silentDelegator) OnRequestReceived(index int, data []byte, addr string) {
	s.countOnRequestReceived++
}
func (s *silentDelegator) OnJobReceived(data []byte) { s.countOnJobReceived++ }

func TestProtocolWriteCommand(t *testing.T) {

	r, w := net.Pipe()

	config := ProtocolConfig{
		Conn:      w,
		Delegator: &silentDelegator{},
	}
	p := newProtocol(config)

	go func() {
		assert.Nil(t, p.WriteCommand(command.Heartbeat()))
		w.Close()
	}()
	result, err := ioutil.ReadAll(snappy.NewReader(r))
	assert.Equal(t, command.CHeartbeat, string(result[5:]))
	assert.Nil(t, err)

	p.Close()
}

func TestProtocolWriteCommandOnClosedConn(t *testing.T) {

	r, w := net.Pipe()

	config := ProtocolConfig{
		Conn:      w,
		Delegator: &silentDelegator{},
	}
	p := newProtocol(config)

	go func() {
		w.Close()
		assert.EqualError(t, p.WriteCommand(command.Heartbeat()), "io: read/write on closed pipe")
	}()

	result, err := ioutil.ReadAll(snappy.NewReader(r))
	assert.Len(t, result, 0)
	assert.Nil(t, err)

	p.Close()
}

func TestProtocolReadResponse(t *testing.T) {

	r, w := net.Pipe()

	go func() {
		sw := snappy.NewWriter(w)

		hb := command.Heartbeat()
		hb.Write(sw)

		sw.Close()
	}()
	streamType, data, err := readResponse(snappy.NewReader(r))
	assert.Equal(t, command.StreamTypeHeartbeat, streamType)
	assert.Equal(t, []byte("HEARTBEAT"), data)
	assert.Nil(t, err)

}

func TestProtocolReadResponseTooSmall(t *testing.T) {

	r, w := net.Pipe()

	go func() {
		sw := snappy.NewWriter(w)

		sw.Write([]byte{0, 0, 0, 0, 1, 1})

		sw.Close()
	}()
	streamType, data, err := readResponse(snappy.NewReader(r))
	assert.Equal(t, uint8(0), streamType)
	assert.Nil(t, data)
	assert.EqualError(t, err, "length of response is too small")

}

func TestProtocolReadLoop(t *testing.T) {

	dg := &silentDelegator{}
	r, w := net.Pipe()

	config := ProtocolConfig{
		Conn:      r,
		Delegator: dg,
	}
	p := newProtocol(config)
	p.SetLogger(log.New(os.Stderr, "", log.LstdFlags), LogLevelError)

	go func() {

		sw := snappy.NewWriter(w)

		for i := 0; i < 1000; i++ {
			foo := command.Register("foo")
			foo.Write(sw)
		}

		for i := 0; i < 10; i++ {
			hb := command.Heartbeat()
			hb.Write(sw)
		}

		for i := 0; i < 1000; i++ {
			job := command.Job([]byte("data"))
			job.Write(sw)
		}

		unknown := command.Heartbeat()
		unknown.Type = byte('A')
		unknown.Write(sw)

		sw.Close()

	}()

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1000, dg.countOnRequestReceived)
	assert.Equal(t, 1000, dg.countOnJobReceived)

	p.Close()

}
