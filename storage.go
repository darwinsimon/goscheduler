package goscheduler

// Storage provides initial data for scheduler during initialization
type Storage interface {
	GetActiveJobs() (jobs []*Job, err error)
	GetJob(id string) (job *Job, err error)
	InsertJob(job *Job) error
	SetJobAsFinished(id string) error
	RemoveJob(job *Job) error
}
