package main

import (
	"log"
	"time"

	"github.com/darwinsimon/goscheduler"
	"github.com/darwinsimon/goscheduler/storage"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)

	st := storage.NewCassandra()

	sc, err := goscheduler.NewScheduler(st, 7000)
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()

	//sc.InsertJob("test", time.Now().Add(2*time.Second), map[string]string{
	//	"c": "d",
	//})

	//sc.InsertJob("test2", time.Now().Add(5*time.Second), map[string]string{
	//	"a": "b",
	//})

	for x := 1; x < 2; x++ {
		worker, err := goscheduler.NewWorker("localhost:7000")
		log.Println(worker, err)
		worker.Register("test", haha)
		log.Println(worker.Start())

		go func() {
			time.Sleep(3 * time.Second)
			log.Println(worker.Stop())
			time.Sleep(3 * time.Second)
			log.Println(worker.Start())
		}()
	}

	for {
	}

}

func haha(schedule *goscheduler.Job) error {
	log.Println("berjalan")

	return nil
}
