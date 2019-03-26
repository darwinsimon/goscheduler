package goscheduler

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darwinsimon/goscheduler/command"
)

type Scheduler interface {
	AddJob(channel string, runAt time.Time, args map[string]interface{}) (string, error)
	Close()
}

type scheduler struct {
	storage Storage

	logger logger
	logLvl LogLevel

	listener net.Listener

	// activeJobs []*Job
	consumers []*consumer

	channelMap    map[string][]int
	channelMapMtx sync.Mutex

	batchKeys   []string
	batchMap    map[string]*JobBatch
	batchMapMtx sync.Mutex

	newJobChan chan time.Time

	closeMtx  sync.Mutex
	closeChan chan bool
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

		// activeJobs: []*Job{},
		consumers: []*consumer{},

		channelMap:    map[string][]int{},
		channelMapMtx: sync.Mutex{},

		batchKeys:   []string{},
		batchMap:    map[string]*JobBatch{},
		batchMapMtx: sync.Mutex{},

		newJobChan: make(chan time.Time, 1),

		closeMtx:  sync.Mutex{},
		closeChan: make(chan bool, 1),
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

	go s.runTimer()
	go s.startAcceptingConsumers()

	return s, nil

}

// AddJob insert new job to scheduler
func (s *scheduler) AddJob(channel string, runAt time.Time, args map[string]interface{}) (string, error) {

	// Remove milliseconds
	runAt = runAt.Truncate(1 * time.Second)

	now := time.Now()

	if now.Sub(runAt).Seconds() > 0 {
		return "", errors.New(ErrorJobHasExpired)
	}

	job := &Job{
		Channel: channel,
		ID:      generateID(10),
		Args:    args,
		RunAt:   runAt,
	}

	// Insert to storage if available
	if s.storage != nil {
		if err := s.storage.InsertJob(job); err != nil {
			return "", err
		}
	}

	if isNewKey := s.insertToBatch(job); isNewKey {
		s.newJobChan <- runAt
	}

	return job.ID, nil

}

func (s *scheduler) Close() {

	for i := range s.consumers {
		s.consumers[i].heartbeatTicker.Stop()
		s.consumers[i].protocol.Close()
	}

	s.listener.Close()
}

func (s *scheduler) initializeActiveJobs() {

	activeJobs := s.storage.GetActiveJobs()
	for j := range activeJobs {
		s.insertToBatch(activeJobs[j])
	}

}

func (s *scheduler) runTimer() {

	for {

		var timer = time.NewTimer(100 * time.Hour)

		now := time.Now()
		nearestKey := now.Format("20060102150405")

		sort.Strings(s.batchKeys)

		if len(s.batchKeys) > 0 {

			// Get nearest and not expired schedule
			for _, bk := range s.batchKeys {

				if nearestKey < bk {

					s.batchMapMtx.Lock()
					job, ok := s.batchMap[bk]
					s.batchMapMtx.Unlock()

					if ok {

						timer.Stop()
						timer = time.NewTimer(job.RunAt.Sub(now))

						break
					}

				} else if nearestKey >= bk {
					break
				}

			}

		}

		select {
		case runTime := <-timer.C:

			currentKey := runTime.Format("20060102150405")

			s.batchMapMtx.Lock()
			batch, ok := s.batchMap[currentKey]
			delete(s.batchMap, currentKey)
			s.batchMapMtx.Unlock()

			if ok {

				go func() {

					// Remove batch key from array
					for k := range s.batchKeys {
						if currentKey == s.batchKeys[k] {
							s.batchKeys = append(s.batchKeys[:k], s.batchKeys[k+1:]...)
							break
						}
					}

				}()

				go func(jobs []*Job) {

					for j := range jobs {
						go s.processJob(jobs[j])
					}

				}(batch.Jobs)
			}

		case <-s.newJobChan:

			timer.Stop()

		}

	}

}

func (s *scheduler) closeConsumer(index int) {

	if len(s.consumers) <= index {
		return
	}

	s.consumers[index].protocol.Close()

	s.consumers = append(s.consumers[:index], s.consumers[index+1:]...)

}

func (c *consumer) sendHeartbeat() {

	c.heartbeatTicker = time.NewTicker(1 * time.Second)

	for range c.heartbeatTicker.C {
		if err := c.protocol.WriteCommand(command.Heartbeat()); err != nil {
			c.heartbeatTicker.Stop()
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

	s.insertToBatch(job)

	return nil

}

func (s *scheduler) insertToBatch(job *Job) bool {

	var isNewKey bool

	batchKey := job.RunAt.Format("20060102150405")

	s.batchMapMtx.Lock()
	defer s.batchMapMtx.Unlock()

	// Initialize new batch
	if _, ok := s.batchMap[batchKey]; !ok {
		s.batchMap[batchKey] = &JobBatch{
			RunAt: job.RunAt,
			Jobs:  []*Job{},
		}
		s.batchKeys = append(s.batchKeys, batchKey)
		isNewKey = true
	}

	s.batchMap[batchKey].Jobs = append(s.batchMap[batchKey].Jobs, job)

	return isNewKey

}

var ato int64

func (s *scheduler) processJob(job *Job) {

	atomic.AddInt64(&ato, 1)

	s.channelMapMtx.Lock()
	consumerIdx := s.channelMap[job.Channel]
	s.channelMapMtx.Unlock()

	// No consumer available
	if len(consumerIdx) == 0 {

	}

	// Need to implement round robin
	for ii := range consumerIdx {

		//encoded, _ := json.Marshal(job)

		if err := s.consumers[ii].protocol.WriteCommand(command.Job([]byte(`{"channel":"dead_channel","id":"463778310c0913d50819","args":{"a":"b"},"run_at":"2019-03-26T14:27:21+07:00"}`))); err != nil {

			s.log(LogLevelError, "Failed to push job %s %s", job.Channel, job.ID)
		} else {
			break
		}
		break
	}

}

func (s *scheduler) startAcceptingConsumers() {

	for {

		// Listen for new connection
		conn, err := s.listener.Accept()
		if err != nil {
			s.log(LogLevelError, "Failed listen to new worker: %v", err)
			break
		}

		s.log(LogLevelInfo, "New worker at %s", conn.RemoteAddr().String())

		// Create new consumer
		newConsumer := &consumer{
			index: len(s.consumers),
			protocol: newProtocol(ProtocolConfig{
				Conn:          conn,
				Index:         len(s.consumers),
				Delegator:     s,
				ReadDeadline:  5 * time.Second,
				WriteDeadline: 5 * time.Second,
			}),
		}

		// Set logger
		newConsumer.protocol.SetLogger(s.logger, s.logLvl)

		go newConsumer.sendHeartbeat()

		s.consumers = append(s.consumers, newConsumer)

	}

}

// add new channel to map
func (s *scheduler) registerNewChannel(index int, channel string) {

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
	s.log(LogLevelInfo, "Registered channel %s for %s", channel, s.consumers[index].protocol.addr)

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

func (s *scheduler) OnConnClose() {
	s.closeChan <- true
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
