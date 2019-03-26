package goscheduler

// Delegator is an interface of methods that are used as
// callbacks in Protocol
type Delegator interface {
	OnConnClose()
	OnIOError(index int)
	OnRequestReceived(index int, data []byte)
	OnJobReceived(data []byte)
}
