package goscheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/darwinsimon/goscheduler/command"
)

// A Client interface provides all functions for add and consume jobs
type Client interface {
	AddJob(channel string, runAt time.Time, args map[string]interface{}) (string, error)
	Listen(channel string, f ClientCallbackFunc) error
	RemoveJob(channel, id string) error
	Close()
}

type client struct {
	protocol *Protocol

	logger logger
	logLvl LogLevel

	closeFlag int32

	funcs map[string]ClientCallbackFunc
}

// ClientConfig contains every configuration to create a new Client
type ClientConfig struct {
	Address string

	Logger logger
	LogLvl LogLevel

	ReadDeadline  time.Duration
	WriteDeadline time.Duration
}

// ClientCallbackFunc represents callback function for a specific channel
type ClientCallbackFunc func(job *Job) error

// NewClient create new client
func NewClient(config ClientConfig) (Client, error) {

	conn, err := net.Dial("tcp", config.Address)
	if err != nil {
		return nil, err
	}

	c := &client{
		logger: config.Logger,
		logLvl: config.LogLvl,

		funcs: map[string]ClientCallbackFunc{},
	}

	// Set default deadline
	if config.ReadDeadline == 0 {
		config.ReadDeadline = 3 * time.Second
	}
	if config.WriteDeadline == 0 {
		config.WriteDeadline = 3 * time.Second
	}

	c.protocol = newProtocol(ProtocolConfig{
		Conn:          conn,
		Delegator:     c,
		ReadDeadline:  config.ReadDeadline,
		WriteDeadline: config.WriteDeadline,
	})

	c.protocol.SetLogger(config.Logger, config.LogLvl)

	return c, nil
}

// Listen to specific channel
func (c *client) Listen(channel string, f ClientCallbackFunc) error {

	if atomic.LoadInt32(&c.closeFlag) == 1 {
		return errors.New(ErrorClosedConnection)
	}

	if !isAlphaNumeric(channel) {
		return errors.New(ErrorInvalidChannelName)
	}

	// Send Register command to Scheduler
	if err := c.protocol.WriteCommand(command.Register(channel)); err != nil {
		c.log(LogLevelError, "Register channel %v", err)
		return err
	}

	c.funcs[channel] = f

	return nil

}

// AddJob insert new job to scheduler
func (c *client) AddJob(channel string, runAt time.Time, args map[string]interface{}) (string, error) {

	if atomic.LoadInt32(&c.closeFlag) == 1 {
		return "", errors.New(ErrorClosedConnection)
	}

	if !isAlphaNumeric(channel) {
		return "", errors.New(ErrorInvalidChannelName)
	}

	// Remove milliseconds
	runAt = runAt.Truncate(1 * time.Second)

	// Validate expired time
	now := time.Now()
	if now.Sub(runAt).Seconds() > 0 {
		c.log(LogLevelError, "Requested job for %s has expired %s %v", channel, runAt.String(), args)
		return "", errors.New(ErrorJobHasExpired)
	}

	id := generateID(10)
	job := &Job{
		Channel: channel,
		ID:      id,
		Args:    args,
		RunAt:   runAt,
		Status:  JobStatusActive,
	}

	c.log(LogLevelDebug, "New job published %v", job)

	// Push to scheduler
	encoded, _ := json.Marshal(job)
	if err := c.protocol.WriteCommand(command.Job(encoded)); err != nil {
		c.log(LogLevelError, "Publish job %v", err)
		return "", err
	}

	return id, nil

}

// Close the client and connection
func (c *client) Close() {
	if !atomic.CompareAndSwapInt32(&c.closeFlag, 0, 1) {
		return
	}

	c.log(LogLevelInfo, "Closing client...")

	c.protocol.Close()
}

// Remove job based on id and channel name
func (c *client) RemoveJob(channel, id string) error {
	if atomic.LoadInt32(&c.closeFlag) == 1 {
		return errors.New(ErrorClosedConnection)
	}

	if !isAlphaNumeric(channel) {
		return errors.New(ErrorInvalidChannelName)
	}

	if id == "" {
		return errors.New(ErrorInvalidJobID)
	}

	// Send Remove command to Scheduler
	if err := c.protocol.WriteCommand(command.Remove(channel, id)); err != nil {
		c.log(LogLevelError, "Remove job %s %s %v", channel, id, err)
		return err
	}

	return nil
}

func (c *client) log(lvl LogLevel, line string, args ...interface{}) {
	if c.logger == nil {
		return
	}

	if c.logLvl > lvl {
		return
	}

	c.logger.Output(2, fmt.Sprintf("%-4s %s", lvl, fmt.Sprintf(line, args...)))
}

func (c *client) OnRequestReceived(index int, data []byte, addr string) {

}

func (c *client) OnJobReceived(data []byte) {

	strData := string(data)

	job := &Job{}
	if err := json.Unmarshal(data, job); err != nil {
		c.log(LogLevelError, "Read job %s", strData, err)
		return
	}

	f, ok := c.funcs[job.Channel]
	if !ok {
		return
	}

	f(job)

}

func (c *client) OnIOError(index int) {}
