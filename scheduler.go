package goscheduler

import (
	"encoding/json"
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

// Scheduler is the core scheduling system that receives and processes jobs
type Scheduler interface {
	Stop()
}

type scheduler struct {
	storage Storage

	logger logger
	logLvl LogLevel

	listener net.Listener

	clients      map[int]*clientConn
	clientsMtx   sync.Mutex
	clientsWG    sync.WaitGroup
	totalClients int32

	channelMap    map[string][]int
	channelMapMtx sync.Mutex

	batchKeys   []string
	batchMap    map[string]*JobBatch
	batchMapMtx sync.Mutex

	resetTimerChan chan bool
	closeTimerChan chan bool

	routineWG sync.WaitGroup
	closeFlag int32
}

type clientConn struct {
	index    int
	channels []string
	protocol *Protocol

	closeChan chan bool
}

// SchedulerConfig contains every configuration to create a new Scheduler
type SchedulerConfig struct {
	Storage Storage

	Address string

	Logger logger
	LogLvl LogLevel
}

// NewScheduler create new scheduler
func NewScheduler(config SchedulerConfig) (Scheduler, error) {

	s := &scheduler{
		storage: config.Storage,

		logger: config.Logger,
		logLvl: config.LogLvl,

		clients:    map[int]*clientConn{},
		clientsMtx: sync.Mutex{},

		channelMap:    map[string][]int{},
		channelMapMtx: sync.Mutex{},

		batchKeys:   []string{},
		batchMap:    map[string]*JobBatch{},
		batchMapMtx: sync.Mutex{},

		resetTimerChan: make(chan bool),
		closeTimerChan: make(chan bool),
	}

	// Get jobs from storage if available
	if s.storage != nil {
		if err := s.initializeActiveJobs(); err != nil {
			return nil, err
		}
	}

	var err error

	// Open listener connection
	s.listener, err = net.Listen("tcp", config.Address)
	if err != nil {
		s.log(LogLevelError, "%v", err)
		return nil, errors.New(ErrorNewConnection)
	}

	go s.runTimer()

	go s.startAcceptingClients()

	return s, nil

}

func (s *scheduler) Stop() {

	if !atomic.CompareAndSwapInt32(&s.closeFlag, 0, 1) {
		return
	}

	s.log(LogLevelInfo, "Stopping scheduler...")

	go func() {
		s.resetTimerChan <- true
	}()

	s.listener.Close()
	s.routineWG.Wait()

	for i := range s.clients {
		go s.closeClientConn(i)
	}

	s.clientsWG.Wait()

}
func (s *scheduler) closeClientConn(i int) {

	s.clientsMtx.Lock()
	defer s.clientsMtx.Unlock()

	if s.clients[i] == nil {
		return
	}

	s.clients[i].protocol.Close()
	s.clients[i].closeChan <- true

	delete(s.clients, i)
	s.clientsWG.Done()

}

func (s *scheduler) initializeActiveJobs() error {

	activeJobs, err := s.storage.GetActiveJobs()
	if err != nil {
		s.log(LogLevelError, "%v", err)
		return errors.New(ErrorJobHasExpired)
	}

	now := time.Now()

	for j := range activeJobs {

		// Validate expired time
		if now.Sub(activeJobs[j].RunAt).Seconds() > 0 {
			continue
		}

		s.insertToBatch(activeJobs[j])
	}

	return nil
}

func (s *scheduler) runTimer() {

	s.routineWG.Add(1)

	for {

		if atomic.LoadInt32(&s.closeFlag) == 1 {
			s.routineWG.Done()
			return
		}

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

					// Process all jobs with goroutine
					for j := range jobs {
						go s.processJob(jobs[j])
					}

				}(batch.Jobs)
			}

		case <-s.resetTimerChan:
			// Stop timer and recalculate with latest job list
			timer.Stop()

		}

	}

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

func (s *scheduler) processJob(job *Job) {

	s.channelMapMtx.Lock()
	workers := s.channelMap[job.Channel]
	s.channelMapMtx.Unlock()

	// No worker available
	if len(workers) == 0 {
	}

	// Need to implement round robin
	for _, ii := range workers {

		encoded, _ := json.Marshal(job)

		if err := s.clients[ii].protocol.WriteCommand(command.Job(encoded)); err != nil {
			s.log(LogLevelError, "Failed to push job %s %s", job.Channel, job.ID)
		} else {
			break
		}

		break
	}

}

func (s *scheduler) startAcceptingClients() {

	s.routineWG.Add(1)

	for {

		// Listen for new connection
		conn, err := s.listener.Accept()
		if err != nil {

			// Error other than closed connection
			if !strings.Contains(err.Error(), "use of closed network connection") {
				s.log(LogLevelError, "Failed to accept new worker: %v", err)
			}

			break
		}

		newIndex := int(atomic.AddInt32(&s.totalClients, 1))

		s.log(LogLevelInfo, "New worker at %s", conn.RemoteAddr().String())

		// Create new client
		newClient := &clientConn{
			index:     newIndex,
			closeChan: make(chan bool),
			protocol: newProtocol(ProtocolConfig{
				Conn:          conn,
				Index:         newIndex,
				Delegator:     s,
				ReadDeadline:  3 * time.Second,
				WriteDeadline: 3 * time.Second,
			}),
		}

		// Set logger
		newClient.protocol.SetLogger(s.logger, s.logLvl)

		go newClient.sendHeartbeat()

		s.clientsMtx.Lock()
		s.clients[newIndex] = newClient
		s.clientsMtx.Unlock()
		s.clientsWG.Add(1)

	}

	s.routineWG.Done()

}

// add new channel to map
func (s *scheduler) registerNewChannel(index int, channel string, addr string) {

	s.channelMapMtx.Lock()
	defer s.channelMapMtx.Unlock()

	if _, ok := s.channelMap[channel]; !ok {
		s.channelMap[channel] = []int{}
	}

	// Check for duplicate index
	for ch := range s.channelMap[channel] {
		if s.channelMap[channel][ch] == index {
			s.log(LogLevelWarning, "Duplicate channel registration %s for %s", channel, addr)
			return
		}
	}

	s.channelMap[channel] = append(s.channelMap[channel], index)
	s.log(LogLevelInfo, "Registered channel %s for %s", channel, s.clients[index].protocol.addr)

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

func (s *scheduler) OnRequestReceived(index int, data []byte, addr string) {

	strData := string(data)
	commands := strings.Split(strData, " ")
	if len(commands) == 0 {
		s.log(LogLevelWarning, "Unknown command %s", strData)
		return
	}

	switch commands[0] {
	case command.CRegister:

		if len(commands) != 2 {
			s.log(LogLevelWarning, "Wrong REG command %s", strData)
			return
		}

		s.registerNewChannel(index, commands[1], addr)

	default:
		s.log(LogLevelWarning, "Unknown command %s", strData)
	}
}

// OnJobReceived new job from publisher
func (s *scheduler) OnJobReceived(data []byte) {

	job := &Job{}
	if err := json.Unmarshal(data, job); err != nil {
		s.log(LogLevelError, "Failed to read job %v", err)
		return
	}

	// Insert to storage if available
	if s.storage != nil {
		if err := s.storage.InsertJob(job); err != nil {
			s.log(LogLevelError, "%v", err)
		}
	}

	if isNewKey := s.insertToBatch(job); isNewKey {
		s.resetTimerChan <- true
	}

}
func (s *scheduler) OnIOError(index int) {

	if atomic.LoadInt32(&s.closeFlag) == 0 {
		s.closeClientConn(index)
	}

}

func (c *clientConn) sendHeartbeat() {

	ticker := time.NewTicker(5 * time.Second)

	for {

		select {
		case <-ticker.C:

			if err := c.protocol.WriteCommand(command.Heartbeat()); err != nil {
				goto exitLoop
			}

		case <-c.closeChan:
			goto exitLoop
		}
	}

exitLoop:

	ticker.Stop()

}
