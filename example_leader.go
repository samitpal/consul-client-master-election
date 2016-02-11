package main

import (
	"log"
	"time"

	"github.com/hashicorp/consul/api"
	leader_election "github.com/samitpal/consul-client-master-election/api"
)

type myJob struct{}

/*
This will be goProbe implementation
func (m myJob) DoJobFunc(stopCh <-chan struct{}, doneCh chan bool) {
	wg.Add(1)
	go actualFunc(stopCh, doneCh, wg) // inside actualFunc call 'defer wg.Done()'
	wg.Wait()
}
*/
func doJobFunc(doneCh chan bool) {
	time.Sleep(2 * time.Minute)
	close(doneCh)
}

func (m myJob) DoJobFunc(stopCh <-chan bool, doneCh chan bool) {
	go doJobFunc(doneCh)
	for {
		select {
		case <-stopCh:
			log.Println("Received stop signal")
			return
		default:
			log.Println("Do some stuff")
			time.Sleep(2 * time.Second)
		}
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
	leader_election.MaybeAcquireLeadership(client, "goProbe/leader", 20, "goProbe", true, j)
}
