package goscheduler

// Storage provides initial data for scheduler during initialization
type Storage interface {
	GetActiveJobs() (jobs []*Job, err error)
	GetJob(channel, id string) (job *Job, err error)
	InsertJob(job *Job) error
	SetJobAsFinished(channel, id string) error
	RemoveJob(channel, id string) error
}
