package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/darwinsimon/goscheduler"
)

func main() {

	var workerOnly bool
	flag.BoolVar(&workerOnly, "worker", false, "Worker only")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if !workerOnly {

		// Scheduler config
		config := goscheduler.SchedulerConfig{

			Address: ":7000",

			Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
			LogLvl: goscheduler.LogLevelDebug,
		}

		// Create scheduler instance
		sc, err := goscheduler.NewScheduler(config)
		if err != nil {
			log.Fatal(err)
		}
		defer sc.Stop()

		sc.AddJob("dead_channel", time.Now().Add(1*time.Second), map[string]interface{}{
			"a": "b",
		})

		// Insert new job
		for i := 1; i <= 100000; i++ {

			// t, _ := time.Parse("20060102150405 MST", "20190326135345 WIB")

			// time.Now().Add((3*time.Second)+time.Duration(i%10)*time.Second)
			sc.AddJob("do_something", time.Now().Add(3*time.Second), map[string]interface{}{
				"c": fmt.Sprintf("%d", i),
			})

		}
	} else {

		// Create workers
		workerConfig := goscheduler.WorkerConfig{
			Address: "localhost:7000",

			Logger: log.New(os.Stderr, "", log.LstdFlags|log.Llongfile),
			LogLvl: goscheduler.LogLevelDebug,
		}

		worker, err := goscheduler.NewWorker(workerConfig)
		if err != nil {
			log.Println(err)
		}
		log.Println(worker.Register("do_something", doSomething))
		defer worker.Stop()

	}

	exitChan = make(chan int, 1)

	<-exitChan

}

var xx int32
var exitChan chan int

func doSomething(job *goscheduler.Job) error {

	atomic.AddInt32(&xx, 1)

	if xx > 50000 {
		log.Println("Ran", xx, job.Args)
		if xx < -1 {
			exitChan <- 1
		}
	}
	return nil
}
