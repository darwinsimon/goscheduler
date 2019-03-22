package storage

import (
	"github.com/darwinsimon/goscheduler"
)

type local struct{}

func NewLocal() goscheduler.Storage {

	c := local{}
	return &c

}

func (s *local) GetActiveJobs() (jobs []*goscheduler.Job) {
	return nil
}
func (s *local) GetJob(id string) error {
	return nil
}
func (s *local) InsertJob(job *goscheduler.Job) error {
	return nil
}
func (s *local) SetJobAsFinished(id string) error {
	return nil
}
