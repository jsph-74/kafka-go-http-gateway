package main

import (
	"log"
	"os"

	"go-http-kafka-consumer/pkg"
)

func main() {
	if err := consumer.RunConsumer(); err != nil {
		log.Printf("Consumer failed: %v", err)
		os.Exit(1)
	}
}
