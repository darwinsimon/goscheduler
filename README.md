# GoScheduler - Under Development

Inspired by https://github.com/gocraft/work and https://github.com/nsqio/go-nsq, GoScheduler is a simple scheduler for Golang. It uses TCP connection to communicate between scheduler (server) and worker (client). 


## Scheduler
```golang

schedulerConfig := goscheduler.SchedulerConfig{
}

sc, err := goscheduler.NewScheduler(schedulerConfig)
if err != nil {
	log.Fatal("Failed to initialize scheduler")
}
```
## Worker