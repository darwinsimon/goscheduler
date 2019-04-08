package goscheduler

import (
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testStorage struct{}
type testStorageErr struct{}

func (s *testStorage) GetActiveJobs() ([]*Job, error) {

	expiredJob := &Job{
		RunAt: time.Now().AddDate(-1, 0, 0),
	}
	futureJob := &Job{
		RunAt: time.Now().AddDate(20, 0, 0),
	}

	jobs := []*Job{}
	jobs = append(jobs, expiredJob)
	jobs = append(jobs, futureJob)

	return jobs, nil
}
func (s *testStorage) GetJob(id string) (job *Job, err error) {
	return nil, nil
}
func (s *testStorage) InsertJob(job *Job) error {
	return nil
}
func (s *testStorage) SetJobAsFinished(id string) error {
	return nil
}
func (s *testStorage) RemoveJob(job *Job) error {
	return nil
}

func (s *testStorageErr) GetActiveJobs() ([]*Job, error) {
	return nil, errors.New("foo")
}
func (s *testStorageErr) GetJob(id string) (job *Job, err error) {
	return nil, errors.New("foo")
}
func (s *testStorageErr) InsertJob(job *Job) error {
	return errors.New("foo")
}
func (s *testStorageErr) SetJobAsFinished(id string) error {
	return errors.New("foo")
}
func (s *testStorageErr) RemoveJob(job *Job) error {
	return nil
}

func TestSchedulerNewTCPError(t *testing.T) {

	config := SchedulerConfig{
		Address: "$$",
	}
	sc, err := NewScheduler(config)

	assert.Nil(t, sc)
	assert.Error(t, err)
}

func TestSchedulerInitializeStorageJobsError(t *testing.T) {

	address := ":8888"
	config := SchedulerConfig{
		Address: address,
		Storage: &testStorageErr{},
	}

	sc, err := NewScheduler(config)

	assert.Nil(t, sc)
	assert.Error(t, err)

}

func TestSchedulerDuplicateListener(t *testing.T) {

	address := ":8889"
	config := SchedulerConfig{
		Address: address,
		Storage: &testStorage{},

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	sc, err := NewScheduler(config)
	defer sc.Stop()

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	// Producer
	p, err := NewClient(ClientConfig{
		Address: address,
	})
	defer p.Close()

	assert.NotNil(t, p)
	assert.Nil(t, err)

	// Worker
	w, err := NewClient(ClientConfig{
		Address: address,
	})
	defer w.Close()

	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.Nil(t, w.Listen("foo", func(job *Job) error {
		return nil
	}))

	// Duplicate channel registration
	assert.Nil(t, w.Listen("foo", func(job *Job) error {
		return nil
	}))

}

func TestSchedulerFullFlow(t *testing.T) {

	address := ":8890"
	config := SchedulerConfig{
		Address: address,
		Storage: &testStorage{},

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	sc, err := NewScheduler(config)
	defer sc.Stop()

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	// Producer
	p, err := NewClient(ClientConfig{
		Address: address,
	})
	defer p.Close()

	assert.NotNil(t, p)
	assert.Nil(t, err)

	// Worker
	w, err := NewClient(ClientConfig{
		Address: address,
	})
	defer w.Close()

	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.Nil(t, w.Listen("foo", func(job *Job) error {
		return nil
	}))

	runAt, _ := time.Parse("2006", "2050")

	// Add new job
	_, err = p.AddJob("foo", runAt, map[string]interface{}{
		"satu": 1,
	})
	assert.Nil(t, err)

	// Add new job with the same runAt
	_, err = p.AddJob("foo", runAt, map[string]interface{}{
		"dua": 2,
	})
	assert.Nil(t, err)

	// Add new job to run in 1 second
	_, err = p.AddJob("foo", time.Now().Add(time.Second), map[string]interface{}{
		"tiga": 3,
	})
	assert.Nil(t, err)

	time.Sleep(2 * time.Second)
}

func TestSchedulerDoubleStop(t *testing.T) {

	address := ":8892"
	config := SchedulerConfig{
		Address: address,
		Storage: &testStorage{},

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	sc, err := NewScheduler(config)

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	sc.Stop()
	sc.Stop()

	time.Sleep(time.Millisecond)

}

func TestScheduleRemoveJob(t *testing.T) {

	address := ":8893"
	config := SchedulerConfig{
		Address: address,
		Storage: &testStorage{},

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	sc, err := NewScheduler(config)
	defer sc.Stop()

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	// Producer
	p, err := NewClient(ClientConfig{
		Address: address,
	})
	defer p.Close()

	assert.NotNil(t, p)
	assert.Nil(t, err)

	// Worker
	w, err := NewClient(ClientConfig{
		Address: address,
	})
	defer w.Close()

	assert.NotNil(t, w)
	assert.Nil(t, err)

	assert.Nil(t, w.Listen("foo", func(job *Job) error {
		return nil
	}))

	runAt := time.Now().Add(time.Second)

	// Add new job
	id, _ := p.AddJob("foo", runAt, map[string]interface{}{
		"satu": 1,
	})

	assert.Nil(t, p.RemoveJob("foo", id))
	assert.Nil(t, p.RemoveJob("none", id))
	time.Sleep(500 * time.Millisecond)
}

func TestSchedulerOnRequestReceived(t *testing.T) {
	t.Skip()
	address := ":8894"
	config := SchedulerConfig{
		Address: address,
		Storage: &testStorage{},

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: LogLevelDebug,
	}

	sc, err := NewScheduler(config)
	defer sc.Stop()

	assert.NotNil(t, sc)
	assert.Nil(t, err)

	delegator := sc.(Delegator)

	// Empty command
	delegator.OnRequestReceived(0, nil, "")

	// Unknown command
	delegator.OnRequestReceived(0, []byte("foo"), "")
}
