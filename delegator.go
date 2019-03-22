package goscheduler

// Delegator is an interface of methods that are used as
// callbacks in Protocol
type Delegator interface {
	OnIOError(index int)
	OnRequestReceived(index int, data []byte)
	OnJobReceived(data []byte)
}

/*

func (d *consumerConnDelegate) OnResponse(c *Conn, data []byte)       { d.r.onConnResponse(c, data) }
func (d *consumerConnDelegate) OnError(c *Conn, data []byte)          { d.r.onConnError(c, data) }
func (d *consumerConnDelegate) OnMessage(c *Conn, m *Message)         { d.r.onConnMessage(c, m) }
func (d *consumerConnDelegate) OnMessageFinished(c *Conn, m *Message) { d.r.onConnMessageFinished(c, m) }
func (d *consumerConnDelegate) OnMessageRequeued(c *Conn, m *Message) { d.r.onConnMessageRequeued(c, m) }
func (d *consumerConnDelegate) OnBackoff(c *Conn)                     { d.r.onConnBackoff(c) }
func (d *consumerConnDelegate) OnContinue(c *Conn)                    { d.r.onConnContinue(c) }
func (d *consumerConnDelegate) OnResume(c *Conn)                      { d.r.onConnResume(c) }
func (d *consumerConnDelegate) OnIOError(c *Conn, err error)          { d.r.onConnIOError(c, err) }
func (d *consumerConnDelegate) OnHeartbeat(c *Conn)                   { d.r.onConnHeartbeat(c) }
func (d *consumerConnDelegate) OnClose(c *Conn)                       { d.r.onConnClose(c) }
*/
