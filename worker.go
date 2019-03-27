package goscheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/darwinsimon/goscheduler/command"
)

// A Worker interface provides all functions for job consumer
type Worker interface {
	Register(channel string, f WorkerFunc) error
	Stop()
}

type worker struct {
	addr     string
	protocol *Protocol

	logger logger
	logLvl LogLevel

	closeFlag int32

	funcs map[string]WorkerFunc
}

// WorkerConfig contains every configuration to create a new Worker
type WorkerConfig struct {
	Address string

	Logger logger
	LogLvl LogLevel

	ReadDeadline  time.Duration
	WriteDeadline time.Duration
}

// WorkerFunc represents callback function for a specific channel
type WorkerFunc func(job *Job) error

// NewWorker create new worker
func NewWorker(config WorkerConfig) (Worker, error) {

	conn, err := net.Dial("tcp", config.Address)
	if err != nil {
		return nil, err
	}

	w := &worker{
		addr: conn.LocalAddr().String(),

		logger: config.Logger,
		logLvl: config.LogLvl,

		funcs: map[string]WorkerFunc{},
	}

	// Set default deadline
	if config.ReadDeadline == 0 {
		config.ReadDeadline = 3 * time.Second
	}
	if config.WriteDeadline == 0 {
		config.WriteDeadline = 3 * time.Second
	}

	w.protocol = newProtocol(ProtocolConfig{
		Conn:          conn,
		Delegator:     w,
		ReadDeadline:  config.ReadDeadline,
		WriteDeadline: config.WriteDeadline,
	})

	w.protocol.SetLogger(config.Logger, config.LogLvl)

	return w, nil
}

// Register new worker function to scheduler
func (w *worker) Register(channel string, f WorkerFunc) error {
	w.funcs[channel] = f

	// Send Register command to Scheduler
	return w.protocol.WriteCommand(command.Register(channel))

}

// Stop worker and close the connection
func (w *worker) Stop() {
	if !atomic.CompareAndSwapInt32(&w.closeFlag, 0, 1) {
		return
	}

	w.protocol.Close()
}

func (w *worker) log(lvl LogLevel, line string, args ...interface{}) {
	if w.logger == nil {
		return
	}

	if w.logLvl > lvl {
		return
	}

	w.logger.Output(2, fmt.Sprintf("%-4s %s %s", lvl, w.addr, fmt.Sprintf(line, args...)))
}

func (w *worker) OnConnClose() {
	w.Stop()
}

func (w *worker) OnRequestReceived(index int, data []byte) {
	log.Println(string(data))
}

func (w *worker) OnJobReceived(data []byte) {

	strData := string(data)

	job := &Job{}
	if err := json.Unmarshal(data, job); err != nil {
		w.log(LogLevelError, "Failed to read job %s", strData)
		return
	}

	f, ok := w.funcs[job.Channel]
	if !ok {
		return
	}

	f(job)

}

func (w *worker) OnIOError(index int) {}
