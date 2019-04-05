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

var batchTimeLayout = "20060102150405MST"

// Scheduler is the core scheduling system that receives and processes jobs
type Scheduler interface {
	Stop()
}

type scheduler struct {
	storage Storage

	logger logger
	logLvl LogLevel

	listener net.Listener

	// Map of connection by client ID's
	clients      map[int]*clientConn
	clientsMtx   sync.Mutex
	clientsWG    sync.WaitGroup
	totalClients int32

	// List of sorted runAt
	runAtKeys []string

	// Flags for runAt key
	batchMap    map[string]bool
	batchMapMtx sync.Mutex

	// Map of jobs by runAt
	jobMap    map[string][]*Job
	jobMapMtx sync.Mutex

	// Map of clients by channel's name
	clientMap    map[string]*roundRobin
	clientMapMtx sync.Mutex

	routineWG sync.WaitGroup
	closeFlag int32

	resetTimerChan chan bool

	readDeadline  time.Duration
	writeDeadline time.Duration
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

	ReadDeadline  time.Duration
	WriteDeadline time.Duration
}

// NewScheduler create new scheduler
func NewScheduler(config SchedulerConfig) (Scheduler, error) {

	s := &scheduler{
		storage: config.Storage,

		logger: config.Logger,
		logLvl: config.LogLvl,

		clients: map[int]*clientConn{},

		runAtKeys: []string{},
		batchMap:  map[string]bool{},
		jobMap:    map[string][]*Job{},
		clientMap: map[string]*roundRobin{},

		resetTimerChan: make(chan bool),

		readDeadline:  config.ReadDeadline,
		writeDeadline: config.WriteDeadline,
	}

	// Get jobs from storage if available
	if s.storage != nil {
		if err := s.initializeStorageJobs(); err != nil {
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

	s.log(LogLevelInfo, "Scheduler has stopped")

}
func (s *scheduler) closeClientConn(i int) {

	s.clientsMtx.Lock()
	defer s.clientsMtx.Unlock()

	if s.clients[i] == nil {
		return
	}

	s.clientMapMtx.Lock()
	for _, channelName := range s.clients[i].channels {
		s.clientMap[channelName].RemoveByValue(i)
	}

	s.clientMapMtx.Unlock()

	s.clients[i].protocol.Close()
	s.clients[i].closeChan <- true

	delete(s.clients, i)
	s.clientsWG.Done()

}

func (s *scheduler) initializeStorageJobs() error {

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

		s.insertToMap(activeJobs[j])
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
		nearestKey := now.Format(batchTimeLayout)

		if len(s.runAtKeys) > 0 {

			// Get nearest and not expired schedule
			for _, bk := range s.runAtKeys {

				if nearestKey < bk {

					earliestTime, _ := time.Parse(batchTimeLayout, bk)

					timer.Stop()
					timer = time.NewTimer(earliestTime.Sub(now))
					break

				}

			}

		}

		select {
		case runTime := <-timer.C:

			currentKey := runTime.Format(batchTimeLayout)
			s.log(LogLevelDebug, "Run for time %s", currentKey)

			s.batchMapMtx.Lock()
			delete(s.batchMap, currentKey)
			s.batchMapMtx.Unlock()

			s.jobMapMtx.Lock()
			jbs, ok := s.jobMap[currentKey]
			delete(s.jobMap, currentKey)
			s.jobMapMtx.Unlock()

			if ok {

				go func() {

					// Remove batch key from array
					for k := range s.runAtKeys {
						if currentKey == s.runAtKeys[k] {
							s.runAtKeys = append(s.runAtKeys[:k], s.runAtKeys[k+1:]...)
							break
						}
					}

				}()

				go func(jobs []*Job) {

					// Process all jobs with goroutine
					for j := range jobs {
						go s.processJob(jobs[j])
					}

				}(jbs)
			}

		case <-s.resetTimerChan:
			// Stop timer and recalculate with latest job list
			timer.Stop()

		}

	}

}

func (s *scheduler) insertToMap(job *Job) (isNewBatch bool) {

	batchKey := job.RunAt.Format(batchTimeLayout)

	// Check for new batch
	s.batchMapMtx.Lock()
	if _, ok := s.batchMap[batchKey]; !ok {
		s.batchMap[batchKey] = true

		// Insert new batch
		s.runAtKeys = append(s.runAtKeys, batchKey)

		// Sort by the nearest schedule
		sort.Strings(s.runAtKeys)

		isNewBatch = true
	}
	s.batchMapMtx.Unlock()

	// Insert to job map
	s.jobMapMtx.Lock()
	if _, ok := s.jobMap[batchKey]; !ok {
		s.jobMap[batchKey] = []*Job{}
	}
	s.jobMap[batchKey] = append(s.jobMap[batchKey], job)
	s.jobMapMtx.Unlock()

	return

}

func (s *scheduler) processJob(job *Job) {

	s.clientMapMtx.Lock()
	rr, ok := s.clientMap[job.Channel]
	s.clientMapMtx.Unlock()

	// No channel is registered
	if !ok {
		return
	}

	// Get client
	pickedIndex, err := rr.Pick()

	// No client is available
	if err != nil {
		s.log(LogLevelError, "Get client index %v", err)
		return
	}

	encoded, _ := json.Marshal(job)

	s.clientsMtx.Lock()
	if err := s.clients[pickedIndex].protocol.WriteCommand(command.Job(encoded)); err != nil {
		s.log(LogLevelError, "Push job %s %s", job.Channel, job.ID, err)
	}
	s.clientsMtx.Unlock()

}

func (s *scheduler) startAcceptingClients() {

	s.routineWG.Add(1)

	for {

		// Listen for new connection
		conn, err := s.listener.Accept()
		if err != nil {

			// Error other than closed connection
			if !strings.Contains(err.Error(), "use of closed network connection") {
				s.log(LogLevelError, "Accept connection: %v", err)
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
				Conn:      conn,
				Index:     newIndex,
				Delegator: s,

				ReadDeadline:  s.readDeadline,
				WriteDeadline: s.writeDeadline,
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

	s.clientMapMtx.Lock()
	defer s.clientMapMtx.Unlock()

	// Initialization for new key
	if _, ok := s.clientMap[channel]; !ok {
		s.clientMap[channel] = newRoundRobin()
	}

	// Prevent duplicate registration
	if s.clientMap[channel].IsDuplicate(index) {
		s.log(LogLevelWarning, "Duplicate channel registration %s for %s", channel, addr)
		return
	}

	s.clientsMtx.Lock()
	s.clients[index].channels = append(s.clients[index].channels, channel)
	s.log(LogLevelInfo, "Registered channel %s for %s", channel, s.clients[index].protocol.addr)
	s.clientsMtx.Unlock()

	s.clientMap[channel].Add(index)

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
		s.log(LogLevelError, "Read job %v", err)
		return
	}

	// Insert to storage if available
	if s.storage != nil {
		if err := s.storage.InsertJob(job); err != nil {
			s.log(LogLevelError, "%v", err)
		}
	}

	if isNewKey := s.insertToMap(job); isNewKey {
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
