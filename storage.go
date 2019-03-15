package goscheduler

type Storage interface {
	GetAll() (jobs []*Job)
	GetJob(id string) error
	InsertJob(job *Job) error
	RemoveJOb(id string) error
}
