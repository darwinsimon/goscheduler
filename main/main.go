package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/darwinsimon/goscheduler"
	"github.com/darwinsimon/goscheduler/storage"
)

func main() {

	// Create storage
	st := storage.NewLocal()

	// Scheduler config
	config := goscheduler.SchedulerConfig{
		Storage: st,

		Port: 7000,

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: goscheduler.LogLevelDebug,
	}

	// Create scheduler instance
	sc, err := goscheduler.NewScheduler(config)
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()

	// Insert new job
	for i := 1; i < 1000; i++ {

		// time.Now().Add(time.Duration(1*300))*time.Millisecond)
		sc.AddJob("do_something", time.Now().Add(1*time.Second), map[string]string{
			"c": fmt.Sprintf("%d", i),
		})
	}

	sc.AddJob("dead_channel", time.Now().Add(1*time.Second), map[string]string{
		"a": "b",
	})

	// Create workers
	workerConfig := goscheduler.WorkerConfig{
		Storage: st,

		Address: "localhost:7000",

		Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
		LogLvl: goscheduler.LogLevelDebug,
	}

	worker, err := goscheduler.NewWorker(workerConfig)
	if err != nil {
		log.Println(err)
	}
	worker.Register("do_something", doSomething)
	defer worker.Close()

	for {
	}

}

func doSomething(job *goscheduler.Job) error {

	log.Println("Running job", job.Channel, job.ID, job.Args)

	return nil
}
