package main

type Storage interface {
	SaveSchedule() error
	GetSchedule() error
}
