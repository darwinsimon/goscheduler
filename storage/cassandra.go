package storage

import (
	"github.com/darwinsimon/goscheduler"
)

type cassandra struct{}

func NewCassandra() goscheduler.Storage {

	c := cassandra{}

	return &c
}

func (c *cassandra) GetAll() (jobs []*goscheduler.Job) {
	return nil
}
func (c *cassandra) GetJob(id string) error {
	return nil
}
func (c *cassandra) InsertJob(job *goscheduler.Job) error {
	return nil
}
func (c *cassandra) RemoveJOb(id string) error {
	return nil
}
