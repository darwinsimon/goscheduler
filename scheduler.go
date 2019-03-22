package goscheduler

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darwinsimon/goscheduler/command"
	"github.com/robfig/cron"
)

type Scheduler interface {
	AddJob(channel string, startAt time.Time, args map[string]string) (string, error)
	Close()
}

type scheduler struct {
	storage Storage

	logger logger
	logLvl LogLevel

	cronObj *cron.Cron

	listener net.Listener

	activeJobs []*Job
	consumers  []*consumer

	channelMap    map[string][]int
	channelMapMtx sync.Mutex

	jobChan chan *Job

	ato int64
}

type consumer struct {
	index    int
	channels []string
	protocol *Protocol

	heartbeatTicker *time.Ticker
}

// SchedulerConfig contains every configuration to create a new Scheduler
type SchedulerConfig struct {
	Storage Storage

	Port int

	Logger logger
	LogLvl LogLevel
}

// NewScheduler create new scheduler
func NewScheduler(config SchedulerConfig) (Scheduler, error) {

	s := &scheduler{
		storage: config.Storage,

		logger: config.Logger,
		logLvl: config.LogLvl,

		cronObj: cron.New(),

		activeJobs: []*Job{},
		consumers:  []*consumer{},

		channelMap:    map[string][]int{},
		channelMapMtx: sync.Mutex{},

		jobChan: make(chan *Job),
	}

	if s.storage != nil {
		s.initializeActiveJobs()
	}

	var err error

	// Open listener connection
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", config.Port))
	if err != nil {
		return nil, err
	}

	s.cronObj.Start()

	go s.startAcceptingConsumers()
	go s.processJob()

	return s, nil

}

// AddJob insert new job to scheduler
func (s *scheduler) AddJob(channel string, startAt time.Time, args map[string]string) (string, error) {

	job := &Job{
		Channel: channel,
		ID:      generateID(10),
		Args:    args,
		StartAt: startAt,
	}

	s.addJobToCron(job)

	return job.ID, s.saveToActiveJobs(job)

}

func (s *scheduler) Close() {

	close(s.jobChan)

	for i := range s.consumers {
		s.consumers[i].heartbeatTicker.Stop()
		s.consumers[i].protocol.close()
	}

	s.listener.Close()
}

func (s *scheduler) initializeActiveJobs() {

	s.activeJobs = s.storage.GetActiveJobs()
	for j := range s.activeJobs {
		s.addJobToCron(s.activeJobs[j])
	}

}

func (s *scheduler) addJobToCron(job *Job) {
	s.cronObj.AddFunc(job.StartAt.Format("5 4 15 2 1 *"), func() {
		s.jobChan <- job
	})
}

func (s *scheduler) closeConsumer(index int) {

	if len(s.consumers) <= index {
		return
	}

	s.consumers[index].protocol.close()

	s.consumers = append(s.consumers[:index], s.consumers[index+1:]...)

}

func (c *consumer) sendHeartbeat() {

	c.heartbeatTicker = time.NewTicker(1 * time.Second)

	for {
		select {
		case <-c.heartbeatTicker.C:

			c.protocol.WriteCommand(command.Heartbeat())
		}
	}

}

func (s *scheduler) saveToActiveJobs(job *Job) error {

	// Insert to storage if available
	if s.storage != nil {
		if err := s.storage.InsertJob(job); err != nil {
			return err
		}
	}

	totaljobs++

	s.activeJobs = append(s.activeJobs, job)
	return nil

}

var ato int64
var totaljobs int

func (s *scheduler) processJob() {

	for {
		select {
		case job := <-s.jobChan:

			atomic.AddInt64(&ato, 1)
			// log.Println("Run scheduler for", job.Channel, job.ID)
			s.channelMapMtx.Lock()
			consumerIdx := s.channelMap[job.Channel]
			//opsFinal := atomic.LoadInt64(&s.ato)
			//
			s.channelMapMtx.Unlock()

			// Not consumer available
			if len(consumerIdx) == 0 {

			}

			log.Println("Start looping consumer", job.Args["c"], ato)

			// Need to implement round robin
			/*	for i := range consumerIdx {

				encoded, _ := json.Marshal(job)
				log.Println("Encoded", totaljobs, ato)
				if err := s.consumers[i].protocol.WriteCommand(command.Job(encoded)); err != nil {
					s.log(LogLevelError, "Failed to push job %s %s", job.Channel, job.ID)
				} else {
					break


			}}*/
			log.Println("Finished processing: ", job.Args["c"])
		}
	}

}

func (s *scheduler) startAcceptingConsumers() {

	for {

		// Listen for new connection
		conn, _ := s.listener.Accept()
		log.Println("New worker from", conn.RemoteAddr().String())

		// Create new consumer
		newConsumer := &consumer{
			index: len(s.consumers),
			protocol: NewProtocol(ProtocolConfig{
				Conn:          conn,
				Index:         len(s.consumers),
				Delegator:     s,
				ReadDeadline:  5 * time.Second,
				WriteDeadline: 5 * time.Second,
			}),
		}

		// Set logger
		newConsumer.protocol.SetLogger(s.logger, s.logLvl)

		// go newConsumer.sendHeartbeat()

		s.consumers = append(s.consumers, newConsumer)

	}

}

// add new channel to map
func (s *scheduler) registerNewChannel(index int, channel string) {

	log.Println("Start registering new channel", channel)
	s.channelMapMtx.Lock()
	defer s.channelMapMtx.Unlock()

	if _, ok := s.channelMap[channel]; !ok {
		s.channelMap[channel] = []int{}
	}

	// Check for duplicate index
	for ch := range s.channelMap[channel] {
		if s.channelMap[channel][ch] == index {
			return
		}
	}

	s.channelMap[channel] = append(s.channelMap[channel], index)
	log.Println("Registered new channel", channel)

}

func (s *scheduler) log(lvl LogLevel, line string, args ...interface{}) {
	if s.logger == nil {
		return
	}

	if s.logLvl > lvl {
		return
	}

	s.logger.Output(2, fmt.Sprintf("%-4s %s", lvl, fmt.Sprintf(line, args...)))
}

func (s *scheduler) OnRequestReceived(index int, data []byte) {

	strData := string(data)
	commands := strings.Split(strData, " ")
	if len(commands) == 0 {
		return
	}
	switch commands[0] {
	case command.CRegister:

		if len(commands) != 2 {
			s.log(LogLevelWarning, "Wrong REG command %s", strData)
			return
		}

		s.registerNewChannel(index, commands[1])

	default:
		s.log(LogLevelWarning, "Unknown command %s", strData)
	}
}
func (s *scheduler) OnJobReceived(data []byte) {}
func (s *scheduler) OnIOError(index int)       { s.closeConsumer(index) }
