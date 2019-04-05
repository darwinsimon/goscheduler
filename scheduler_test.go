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

func TestSchedulerNewTCPError(t *testing.T) {

	config := SchedulerConfig{
		Address: "$$",
	}
	sc, err := NewScheduler(config)

	assert.Nil(t, sc)
	assert.Error(t, err)
}

func TestSchedulerInitializeStorageJobsError(t *testing.T) {

	address := ":8891"
	config := SchedulerConfig{
		Address: address,
		Storage: &testStorageErr{},
	}

	sc, err := NewScheduler(config)

	assert.Nil(t, sc)
	assert.Error(t, err)

}

func TestSchedulerDuplicateListener(t *testing.T) {

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
	assert.Nil(t, p.AddJob("foo", runAt, map[string]interface{}{
		"satu": 1,
	}))

	// Add new job with the same runAt
	assert.Nil(t, p.AddJob("foo", runAt, map[string]interface{}{
		"dua": 2,
	}))

	// Add new job to run in 1 second
	assert.Nil(t, p.AddJob("foo", time.Now().Add(time.Second), map[string]interface{}{
		"tiga": 3,
	}))

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
