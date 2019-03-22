package storage

import (
	"github.com/darwinsimon/goscheduler"
)

type local struct{}

func NewLocal() goscheduler.Storage {

	c := local{}

	return &c
}

func (s *local) GetAll() (jobs []*goscheduler.Job) {
	return nil
}
func (s *local) GetJob(id string) error {
	return nil
}
func (s *local) InsertJob(job *goscheduler.Job) error {
	return nil
}
func (s *local) RemoveJOb(id string) error {
	return nil
}
