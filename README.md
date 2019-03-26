# GoScheduler - Under Development

Inspired by https://github.com/gocraft/work and https://github.com/nsqio/go-nsq, GoScheduler is a simple scheduler for Golang. It uses TCP connection to communicate between scheduler (server) and worker (client). 


## Scheduler
```golang

	// Set your logger
	myLogger := log.New(os.Stderr, "", log.LstdFlags|log.Llongfile)

	// Scheduler config
	config := goscheduler.SchedulerConfig{
		Storage: myStorage, // use your own storage

		Port: 7000,

		Logger: myLogger,
		LogLvl: goscheduler.LogLevelDebug,
	}

	// Create scheduler instance
	sc, err := goscheduler.NewScheduler(config)
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()


	// Adding new job
	// will run in 5 seconds
	sc.AddJob("channel_name", time.Now().Add(5*time.Second), map[string]interface{}{
		"name": "foo",
		"age": 28,
	})

```
## Worker
```golang

	// Set your logger
	myLogger := log.New(os.Stderr, "", log.LstdFlags|log.Llongfile)

	// Worker config
	workerConfig := goscheduler.WorkerConfig{
		Address: "localhost:7000",

		Logger: myLogger,
		LogLvl: goscheduler.LogLevelDebug,
	}

	// Create worker instance
	worker, err := goscheduler.NewWorker(workerConfig)
	if err != nil {
		log.Fatalln(err)
	}
	defer worker.Close()

	// Register for new channel listener
	worker.Register("channel_name", callback)


	// Create callback function
	func callback(job *goscheduler.Job) error {
		log.Println("Running job", job.Channel, job.ID, job.Args)
		return nil
	}


```