package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/darwinsimon/goscheduler"
	"github.com/darwinsimon/goscheduler/storage"
)

func main() {

	var workerOnly bool
	flag.BoolVar(&workerOnly, "worker", false, "Worker only")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if !workerOnly {

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
		/*
			// Insert new job
			for i := 1; i == 14000; i++ {

				// t, _ := time.Parse("20060102150405 MST", "20190326095701 WIB")
				// time.Now().Add((3*time.Second)+time.Duration(i%10)*time.Second)
				sc.AddJob("do_something", time.Now().Add((2*time.Second)+time.Duration(i%3)*time.Second), map[string]string{
					"c": fmt.Sprintf("%d", i),
				})

			}
		*/

		log.Println("Finished")
		/*
			go func() {
				time.Sleep(5 * time.Second)

				sc.AddJob("do_something", time.Now().Add(1*time.Second), map[string]string{
					"c": fmt.Sprintf("%d", 20000),
				})
			}()
		*/
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
		defer worker.Close()

	}

	/*// Insert new job
	for i := 1; i <= 2000; i++ {

		// time.Now().Add(time.Duration(1*300))*time.Millisecond)
		sc.AddJob("do_something", time.Now().Add(4*time.Second), map[string]string{
			"c": fmt.Sprintf("%d", i*10),
		})
	}*/

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
