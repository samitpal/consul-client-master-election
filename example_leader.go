package main

import (
	"log"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	leader_election "github.com/samitpal/consul-client-master-election/api"
)

type myJob struct{}

func doJobFuncNonHAMode(doneCh chan bool, wg sync.WaitGroup) {
	defer wg.Done()
	log.Println("Do some stuff")
	time.Sleep(5 * time.Minute)
	close(doneCh)
}

func (m myJob) DoJobFunc(stopCh chan bool, doneCh chan bool) {
	var wg sync.WaitGroup
	wg.Add(1)
	go doJobFuncNonHAMode(doneCh, wg)
	select {
	case <-stopCh:
		wg.Wait()
		log.Println("Received stop signal")
		return
	}
}

func main() {

	config := api.DefaultConfig()
	config.Address = "localhost:8500"
	client, err := api.NewClient(config)
	if err != nil {
		log.Fatalf("Fatal error: %v", err)
	}
	j := myJob{}
	leader_election.MaybeAcquireLeadership(client, "example/leader", 20, "example", false, j)
}
