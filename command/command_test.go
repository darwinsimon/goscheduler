package command_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	. "github.com/darwinsimon/goscheduler/command"
	"github.com/stretchr/testify/assert"
)

type errorWriter struct {
	errorDelay int
	currHit    int
}

func (w *errorWriter) Write(p []byte) (n int, err error) {

	if w.errorDelay == w.currHit {
		return 0, errors.New("dummy error")
	}
	w.currHit++
	return 0, nil
}

func TestWriteTo(t *testing.T) {

	tcs := []struct {
		name   string
		cmd    *Command
		writer io.Writer
		total  int
		err    error
	}{
		{
			name: "Name only",
			cmd: &Command{
				Name: []byte("foo"),
				Type: StreamTypeRequest,
			},
			writer: &bytes.Buffer{},
			total:  8,
			err:    nil,
		},
		{
			name: "Name only with error",
			cmd: &Command{
				Name: []byte("foo"),
				Type: StreamTypeRequest,
			},
			writer: &errorWriter{
				errorDelay: 2,
			},
			total: 0,
			err:   errors.New("dummy error"),
		},
		{
			name: "Name only with error size",
			cmd: &Command{
				Name: []byte("foo"),
				Type: StreamTypeRequest,
			},
			writer: &errorWriter{},
			total:  0,
			err:    errors.New("dummy error"),
		},
		{
			name: "Name only with error type",
			cmd: &Command{
				Name: []byte("foo"),
				Type: StreamTypeRequest,
			},
			writer: &errorWriter{
				errorDelay: 1,
			},
			total: 0,
			err:   errors.New("dummy error"),
		},
		{
			name: "Name with body",
			cmd: &Command{
				Name: []byte("foo"),
				Body: []byte("bar"),
				Type: StreamTypeRequest,
			},
			writer: &bytes.Buffer{},
			total:  12,
			err:    nil,
		},
		{
			name: "Name with body with error",
			cmd: &Command{
				Name: []byte("foo"),
				Body: []byte("bar"),
				Type: StreamTypeRequest,
			},
			writer: &errorWriter{
				errorDelay: 4,
			},
			total: 0,
			err:   errors.New("dummy error"),
		},
		{
			name: "Name with body with error separator",
			cmd: &Command{
				Name: []byte("foo"),
				Body: []byte("bar"),
				Type: StreamTypeRequest,
			},
			writer: &errorWriter{
				errorDelay: 3,
			},
			total: 0,
			err:   errors.New("dummy error"),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			total, err := tc.cmd.WriteTo(tc.writer)

			assert.Equal(t, tc.total, total)
			assert.Equal(t, tc.err, err)
		})
	}

}

func TestHeartbeat(t *testing.T) {

	cmd := Heartbeat()
	assert.Equal(t, []byte(CHeartbeat), cmd.Name)
	assert.Nil(t, cmd.Body)
	assert.Equal(t, StreamTypeHeartbeat, cmd.Type)

}

func TestRegister(t *testing.T) {

	cmd := Register("foo")
	assert.Equal(t, []byte(CRegister), cmd.Name)
	assert.Equal(t, []byte("foo"), cmd.Body)
	assert.Equal(t, StreamTypeRequest, cmd.Type)

}

func TestJob(t *testing.T) {

	cmd := Job([]byte("data"))
	assert.Nil(t, cmd.Name)
	assert.Equal(t, []byte("data"), cmd.Body)
	assert.Equal(t, StreamTypeJob, cmd.Type)

}
