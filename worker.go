package goscheduler

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/darwinsimon/goscheduler/command"
)

// A Worker interface provides all functions for job consumer
type Worker interface {
	Register(channel string, f WorkerFunc)
	Close()
}

type worker struct {
	addr     string
	storage  Storage
	protocol *Protocol

	logger logger
	logLvl LogLevel

	funcs map[string]WorkerFunc
}

// WorkerConfig contains every configuration to create a new Worker
type WorkerConfig struct {
	Storage Storage

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

		storage: config.Storage,

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

	w.protocol = NewProtocol(ProtocolConfig{
		Conn:          conn,
		Delegator:     w,
		ReadDeadline:  config.ReadDeadline,
		WriteDeadline: config.WriteDeadline,
	})

	w.protocol.SetLogger(config.Logger, config.LogLvl)

	return w, nil
}

// Register new worker function to scheduler
func (w *worker) Register(channel string, f WorkerFunc) {
	w.funcs[channel] = f

	// Send Register command to Scheduler
	w.protocol.WriteCommand(command.Register(channel))
}
func (w *worker) Close() {

}

func (w *worker) close() {

	// Close the protocol
	w.protocol.close()

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

func (w *worker) OnRequestReceived(index int, data []byte) {

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

func (w *worker) OnIOError(index int) { w.close() }
