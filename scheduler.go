package goscheduler

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron"
)

type Scheduler interface {
	InsertJob(name string, startAt time.Time, args map[string]string) (string, error)
	Close() error
}

type scheduler struct {
	storage         Storage
	listener        net.Listener
	cronObj         *cron.Cron
	activeJobs      []*Job
	activeConsumers map[string]*consumer

	mapMutex sync.Mutex
}

type consumer struct {
	name          string
	conn          net.Conn
	channels      []string
	lastHeartbeat time.Time
}

func NewScheduler(storage Storage, port int) (Scheduler, error) {

	c := &scheduler{
		storage:         storage,
		cronObj:         cron.New(),
		mapMutex:        sync.Mutex{},
		activeJobs:      []*Job{},
		activeConsumers: map[string]*consumer{},
	}

	var err error

	// Open connection
	c.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	c.cronObj.Start()

	go c.startAcceptingConsumers()

	return c, nil

}

// InsertJob insert new job to scheduler
func (c *scheduler) InsertJob(name string, startAt time.Time, args map[string]string) (string, error) {

	job := &Job{
		Name:      name,
		ID:        generateID(10),
		Args:      args,
		CreatedAt: time.Now().Unix(),
		StartAt:   startAt.Unix(),
	}

	log.Println("Insert job", name, job.ID, "will run at", startAt.Format("5 4 15 2 1 *"))

	c.insertActiveJobs(job)

	c.cronObj.AddFunc(startAt.Format("5 4 15 2 1 *"), func() {
		c.processJob(name, job.ID)
	})

	return job.ID, c.storage.InsertJob(job)

}

func (c *scheduler) SetUniqueSchedule(name string, startAt time.Time, args map[string]string) (string, error) {

	job := &Job{
		Name:      name,
		ID:        generateID(10),
		Args:      args,
		CreatedAt: time.Now().Unix(),
		StartAt:   startAt.Unix(),
	}

	return job.ID, c.storage.InsertJob(job)

}

func (c *scheduler) Close() error {

	return c.listener.Close()

}

func (c *scheduler) insertActiveJobs(job *Job) {

	c.activeJobs = append(c.activeJobs, job)

}
func (c *scheduler) insertActiveConsumers(name string, conn net.Conn) error {

	log.Println("New consumer", name)

	newConsumer := &consumer{
		conn:          conn,
		channels:      []string{},
		lastHeartbeat: time.Now(),
	}

	if _, ok := c.activeConsumers[name]; ok {
		return errors.New("Consumer already exists")
	}

	c.activeConsumers[name] = newConsumer

	return nil

}

func (c *scheduler) removeActiveConsumers(name string, conn net.Conn) error {

	log.Println("Deleting consumer", name)

	if _, ok := c.activeConsumers[name]; !ok {
		return errors.New("Consumer doesn't exists")
	}

	delete(c.activeConsumers, name)

	return nil

}

func (c *scheduler) processJob(name, id string) {

	log.Println("Run scheduler for", name, id)

}

func (c *scheduler) startAcceptingConsumers() {

	for {

		// Listen for new connection
		conn, _ := c.listener.Accept()
		log.Println("New connection from", conn.RemoteAddr().String())

		go c.listenToConsumer(conn)
	}

}

func (c *scheduler) listenToConsumer(conn net.Conn) {

	for {

		receivedData, err := bufio.NewReader(conn).ReadString('\n')
		if err == nil {
			c.processCommand(conn, strings.Trim(receivedData, "\n"))
		} else {
			log.Println(err)
			break
		}

	}

	// Connection close
	log.Println("Lost connection to", conn.RemoteAddr())

}

func (c *scheduler) refreshHeartbeat(consumerName string) {

	c.mapMutex.Lock()
	defer c.mapMutex.Unlock()

	if c.activeConsumers[consumerName] != nil {
		c.activeConsumers[consumerName].lastHeartbeat = time.Now()
	}

}

func (c *scheduler) processCommand(conn net.Conn, receivedData string) {

	log.Println(receivedData)

	// Connection closed
	if receivedData == "" {
		conn.Close()
		return
	}

	request := strings.SplitN(receivedData, " ", 3)

	// Unknown format
	if len(request) < 2 {
		return
	}

	consumerName := request[0]
	command := request[1]

	switch command {
	case CommandHeartbeat:
		if err := c.sendToConsumer(consumerName, conn, time.Now().String()); err == nil {

		}

		c.refreshHeartbeat(consumerName)

	case CommandStartConsumer:

		if err := c.insertActiveConsumers(consumerName, conn); err == nil {
			c.sendToConsumer(consumerName, conn, CommandStartConsumerReceived)
		}

	case CommandStopConsumer:

		c.removeActiveConsumers(consumerName, conn)
	}

}

func (c *scheduler) sendToConsumer(name string, conn net.Conn, message string) error {
	log.Println("Send to consumer", name, message)

	// Timeout in 5s
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	_, err := conn.Write([]byte(message + "\n"))
	log.Println(err)
	return err
}
