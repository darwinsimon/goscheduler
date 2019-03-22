package goscheduler

// Storage provides initial data for scheduler during initialization
type Storage interface {
	GetActiveJobs() (jobs []*Job)
	GetJob(id string) error
	InsertJob(job *Job) error
	SetJobAsFinished(id string) error
}
