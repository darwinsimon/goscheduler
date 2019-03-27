package goscheduler

const (

	// ErrorJobHasExpired : Requested job has expired and scheduler won't run it
	ErrorJobHasExpired = "Job has expired"

	// ErrorInitFromStorage : Error occured during fetching active jobs from storage
	ErrorInitFromStorage = "Failed to get active jobs from storage"

	// ErrorInsertToStorage : Error occured during inserting new job to storage
	ErrorInsertToStorage = "Failed during insert to storage"

	// ErrorNewConnection : Error occured during net.Listen for new TCP connection
	ErrorNewConnection = "Failed to open TCP connection"

	// ErrorClosedConnection : Error occured because the connection is already closed
	ErrorClosedConnection = "Using closed connection"
)
