package main

import "time"

type SchedulerClient interface {
}

type client struct {
	storage Storage
}

func NewClient(storage Storage) SchedulerClient {

	c := &client{
		storage: storage,
	}
	return c
}

func (c *client) Schedule(startIn time.Time, args map[string]string) error {

	return c.storage.SaveSchedule()
}
