package main

import (
	"fmt"
	"time"
)

func sendStop(stopCh chan bool) {
	time.Sleep(1 * time.Second)
	close(stopCh)

}

func test1(stopCh chan bool) {
	select {
	case <-stopCh:
		fmt.Println("Rceived stop signal")
		return
	default:
		fmt.Println("Do some stuff")
		time.Sleep(5 * time.Second)
		fmt.Println("Do some stuff")
		time.Sleep(5 * time.Second)
	}
}

func main() {
	fmt.Println("Hello, playground")
	stopCh := make(chan bool)
	go sendStop(stopCh)
	test1(stopCh)
}
