# GoScheduler - Under Development

Inspired by https://github.com/gocraft/work and https://github.com/nsqio/go-nsq, GoScheduler is a simple scheduler for Golang. It uses TCP connection to communicate between scheduler and client. 


## Scheduler
Scheduler service serves as the main gateway for receiving and pushing jobs. You could add storage to save preserve jobs.

```golang

	// Set your logger
	myLogger := log.New(os.Stderr, "", log.LstdFlags|log.Llongfile)

	// Scheduler config
	config := goscheduler.SchedulerConfig{
		Address: ":7000",

		Logger: myLogger,
		LogLvl: goscheduler.LogLevelDebug,
	}

	// Create scheduler instance
	sc, err := goscheduler.NewScheduler(config)
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Stop()

```

## Client
Client service serves as the job producer or consumer. 

```golang

	// Set your logger
	myLogger := log.New(os.Stderr, "", log.LstdFlags|log.Llongfile)

	// Client config
	config := goscheduler.ClientConfig{
		Address: "localhost:7000",

		Logger: myLogger,
		LogLvl: goscheduler.LogLevelDebug,
	}

	// Create client instance
	client, err := goscheduler.NewClient(config)
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Stop()

	// -- As Producer

	// Adding new job
	// will run in 5 seconds
	client.AddJob("channel_name", time.Now().Add(5*time.Second), map[string]interface{}{
		"name": "foo",
		"age": 28,
	})


	// -- As Consumer

	// Register callback for new channel listener
	client.Listen("channel_name", callback)


	// Create callback function
	func callback(job *goscheduler.Job) error {
		log.Println("Running job", job.Channel, job.ID, job.Args)
		return nil
	}

```

## Storage
-