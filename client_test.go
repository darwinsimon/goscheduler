package goscheduler

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/darwinsimon/goscheduler/command"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
)

func TestClientNewTCPError(t *testing.T) {

	config := ClientConfig{
		Address: "foo",
	}

	wk, err := NewClient(config)
	assert.Nil(t, wk)
	assert.Error(t, err)

}

func TestClientRegister(t *testing.T) {

	address := ":12344"

	// Open local connection
	listener, _ := net.Listen("tcp", address)
	defer listener.Close()

	config := ClientConfig{
		Address: address,

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	wk, err := NewClient(config)
	defer wk.Close()
	assert.NotNil(t, wk)
	assert.Nil(t, err)

	assert.Nil(t, wk.Listen("foo", nil))

}

func TestClientOnJobReceived(t *testing.T) {

	address := ":12345"

	encodedFoo, _ := json.Marshal(Job{
		ID:      "1",
		Channel: "foo",
	})
	encodedBar, _ := json.Marshal(Job{
		ID:      "2",
		Channel: "bar",
	})

	tcs := []struct {
		name string
		cmd  *command.Command
	}{
		{
			name: "Unknown JSON",
			cmd:  command.Job([]byte("foo")),
		},
		{
			name: "No channel found",
			cmd:  command.Job(encodedFoo),
		},
		{
			name: "Success",
			cmd:  command.Job(encodedBar),
		},
	}

	// Open local connection
	listener, err := net.Listen("tcp", address)
	defer listener.Close()

	exitChan := make(chan bool)

	go func() {
		conn, _ := listener.Accept()
		w := snappy.NewWriter(conn)

		for _, tc := range tcs {
			tc.cmd.WriteTo(w)
		}

		exitChan <- true
	}()

	config := ClientConfig{
		Address: address,

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	wk, err := NewClient(config)
	defer wk.Close()
	assert.NotNil(t, wk)
	assert.Nil(t, err)

	cb := &callback{}
	assert.EqualError(t, wk.Listen("$", cb.testCallback), ErrorInvalidChannelName)
	assert.Nil(t, wk.Listen("bar", cb.testCallback))

	<-exitChan
	time.Sleep(time.Millisecond)
	assert.Equal(t, "2", cb.lastID)
}

func TestClientAddJob(t *testing.T) {

	address := ":12347"

	// Open local connection
	listener, _ := net.Listen("tcp", address)
	defer listener.Close()

	config := ClientConfig{
		Address: address,

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	p, err := NewClient(config)

	assert.NotNil(t, p)
	assert.Nil(t, err)

	tcs := []struct {
		name    string
		channel string
		runAt   time.Time
		err     error
	}{
		{
			name:  "Empty channel",
			runAt: time.Now().AddDate(0, 0, 1),
			err:   errors.New(ErrorInvalidChannelName),
		},
		{
			name:    "Wrong channel name",
			channel: "*",
			runAt:   time.Now().AddDate(0, 0, 1),
			err:     errors.New(ErrorInvalidChannelName),
		},
		{
			name:    "Expired job",
			channel: "foo21",
			runAt:   time.Now().AddDate(0, 0, -1),
			err:     errors.New(ErrorJobHasExpired),
		},
		{
			name:    "Success",
			channel: "foo_bar",
			runAt:   time.Now().AddDate(0, 0, 1),
			err:     nil,
		},
	}

	for _, tc := range tcs {
		_, err := p.AddJob(tc.channel, tc.runAt, map[string]interface{}{})
		assert.Equal(t, err, tc.err)
	}

	p.Close()
	_, err = p.AddJob("channel", time.Now(), map[string]interface{}{})
	assert.EqualError(t, err, ErrorClosedConnection)
}

func TestClientRemoveJob(t *testing.T) {

	address := ":12340"

	// Open local connection
	listener, _ := net.Listen("tcp", address)
	defer listener.Close()

	config := ClientConfig{
		Address: address,

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	p, err := NewClient(config)

	assert.NotNil(t, p)
	assert.Nil(t, err)

	tcs := []struct {
		name    string
		channel string
		id      string
		runAt   time.Time
		err     error
	}{
		{
			name: "Empty channel",
			err:  errors.New(ErrorInvalidChannelName),
		},
		{
			name:    "Wrong channel name",
			channel: "*",
			err:     errors.New(ErrorInvalidChannelName),
		},
		{
			name:    "Wrong job id",
			channel: "foo_bar",
			err:     errors.New(ErrorInvalidJobID),
		},
		{
			name:    "Wrong job id",
			channel: "foo",
			id:      "bar",
			err:     nil,
		},
	}

	for _, tc := range tcs {
		err := p.RemoveJob(tc.channel, tc.id)
		assert.Equal(t, err, tc.err)
	}

	p.Close()
	assert.EqualError(t, p.RemoveJob("channel", "id"), ErrorClosedConnection)
}

func TestClientAddJobOnClosedScheduler(t *testing.T) {

	address := ":12346"

	// Open local connection
	listener, _ := net.Listen("tcp", address)

	config := ClientConfig{
		Address: address,
	}

	p, err := NewClient(config)
	defer p.Close()

	listener.Close()

	assert.NotNil(t, p)
	assert.Nil(t, err)

	_, err = p.AddJob("foo", time.Now().AddDate(0, 0, 1), map[string]interface{}{})
	assert.Error(t, err)

	p.Close()

}

type callback struct {
	lastID string
}

func (c *callback) testCallback(job *Job) error {
	c.lastID = job.ID
	return nil
}
