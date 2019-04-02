package goscheduler

// Delegator is an interface of methods that are used as
// callbacks in Protocol
type Delegator interface {
	OnIOError(index int)
	OnRequestReceived(index int, data []byte, addr string)
	OnJobReceived(data []byte)
}
