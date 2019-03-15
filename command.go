package goscheduler

// List of all available commands
const (

	// Send heartbeat check to ensure the connection is still alive
	CommandHeartbeat string = "HEARTBEAT"

	// Notify scheduler that consumer is ready to receive jobs
	CommandStartConsumer string = "STARTCONSUMER"

	// Notify scheduler that consumer has stopped
	CommandStopConsumer string = "STOPCONSUMER"

	// Notify worker that schedule acknowledged start command
	CommandStartConsumerReceived string = "STARTCONSUMERRECEIVED"
)
