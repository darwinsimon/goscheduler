package goscheduler

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/darwinsimon/goscheduler/command"
	"github.com/golang/snappy"

	"github.com/stretchr/testify/assert"
)

func TestWorkerNewTCPError(t *testing.T) {

	config := WorkerConfig{
		Address: "foo",
	}

	wk, err := NewWorker(config)
	assert.Nil(t, wk)
	assert.Error(t, err)

}

func TestWorkerRegister(t *testing.T) {

	// Open local connection
	listener, _ := net.Listen("tcp", ":12345")
	defer listener.Close()

	config := WorkerConfig{
		Address: ":12345",

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	wk, err := NewWorker(config)
	defer wk.Stop()
	assert.NotNil(t, wk)
	assert.Nil(t, err)

	assert.Nil(t, wk.Register("foo", nil))

}

func TestWorkerOnJobReceived(t *testing.T) {

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
	listener, err := net.Listen("tcp", ":12345")
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

	config := WorkerConfig{
		Address: ":12345",

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	wk, err := NewWorker(config)
	defer wk.Stop()
	assert.NotNil(t, wk)
	assert.Nil(t, err)

	cb := &callback{}
	assert.Nil(t, wk.Register("bar", cb.testCallback))

	<-exitChan
	time.Sleep(time.Millisecond)
	assert.Equal(t, "2", cb.lastID)
}

type callback struct {
	lastID string
}

func (c *callback) testCallback(job *Job) error {
	log.Println(job.ID)
	c.lastID = job.ID
	return nil
}
