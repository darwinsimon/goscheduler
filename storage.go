package goscheduler

// Storage provides initial data for scheduler during initialization
type Storage interface {
	GetActiveJobs() (jobs []*Job, err error)
	InsertJob(job *Job) error
	RemoveJob(channel, id string) error
}
