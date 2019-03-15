package goscheduler

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

type Worker interface {
	Start() error
	Stop() error
	Register(name string, f WorkerFunc)
}

type worker struct {
	name      string
	storage   Storage
	conn      net.Conn
	isRunning bool
	funcs     map[string]WorkerFunc
	reply     chan string
}

// WorkerFunc represents callback function for a job
type WorkerFunc func(job *Job) error

// NewWorker create new worker
func NewWorker(schedulerAddress string) (Worker, error) {

	conn, err := net.Dial("tcp", schedulerAddress)
	if err != nil {
		return nil, err
	}

	w := &worker{
		name:  generateID(5),
		funcs: map[string]WorkerFunc{},
		conn:  conn,
		reply: make(chan string),
	}

	return w, nil
}

// Start listening to new schedule
func (w *worker) Start() error {

	if w.isRunning {
		return nil
	}
	log.Println("Starting worker", w.name)
	// Notify scheduler
	if err := w.sendToScheduler(CommandStartConsumer); err != nil {
		return err
	}

	w.isRunning = true

	go w.listenToScheduler()

	if reply := <-w.reply; reply != CommandStartConsumerReceived {
		return errors.New("Wrong reply from scheduler: " + reply)
	}

	go w.heartbeat()

	log.Println("Started worker", w.name)
	return nil

}

// Stop listening to new schedule
func (w *worker) Stop() error {

	log.Println("Stopping worker", w.name)
	if !w.isRunning {
		return nil
	}

	w.isRunning = false

	// Notify scheduler
	if err := w.sendToScheduler(CommandStopConsumer); err != nil {
		return err
	}

	log.Println("Stopped worker", w.name)
	return nil

}

// Register new worker function
func (w *worker) Register(name string, f WorkerFunc) {
	w.funcs[name] = f
}

func (w *worker) heartbeat() {

	ticker := time.Tick(1 * time.Second)

	for {
		// Send heartbeat check
		if w.isRunning {
			select {
			case <-ticker:
				w.sendToScheduler(CommandHeartbeat)
			}

		} else {
			break
		}
	}
}

func (w *worker) sendToScheduler(message string) error {

	// Timeout in 5s
	w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	_, err := fmt.Fprintf(w.conn, w.name+" "+message+"\n")
	return err
}

func (w *worker) listenToScheduler() {
	log.Println("Start listening to scheduler")

	// Listen to scheduler
	for {
		if w.isRunning {
			receivedData, err := bufio.NewReader(w.conn).ReadString('\n')
			if err != nil {
				break
			}
			w.processCommand(w.conn, strings.Trim(receivedData, "\n"))
		} else {
			break
		}
	}
}

func (w *worker) processCommand(conn net.Conn, receivedData string) {

	log.Println(receivedData)

	// Connection closed
	if receivedData == "" {
		conn.Close()
		return
	}

	request := strings.SplitN(receivedData, " ", 2)

	// Unknown format
	if len(request) == 0 {
		return
	}

	command := request[0]

	switch command {

	case CommandStartConsumerReceived:

		w.reply <- CommandStartConsumerReceived

	}

}
