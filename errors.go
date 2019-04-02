package goscheduler

const (

	// ErrorJobHasExpired : Requested job has expired and scheduler won't run it
	ErrorJobHasExpired = "Job has expired"

	// ErrorNewConnection : Error occured during net.Listen for new TCP connection
	ErrorNewConnection = "Failed to open TCP connection"

	// ErrorClosedConnection : Error occured because the connection is already closed
	ErrorClosedConnection = "Using closed connection"
)
