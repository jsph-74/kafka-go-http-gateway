package main

import (
	"log"
	"os"
	"time"

	"go-http-kafka-producer/pkg"
	"github.com/gin-gonic/gin"
)


func connectToKafkaWithRetry(brokerAddress string) *producer.KafkaProducer {
	maxRetries := 50
	baseDelay := 1 * time.Second
	maxDelay := 30 * time.Second

	log.Printf("Attempting to connect to Kafka broker: %s", brokerAddress)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		kafkaProducer, err := producer.NewKafkaProducer(brokerAddress)
		if err == nil {
			log.Printf("✅ Successfully connected to Kafka broker on attempt %d", attempt)
			return kafkaProducer
		}

		delay := time.Duration(attempt) * baseDelay
		if delay > maxDelay {
			delay = maxDelay
		}

		log.Printf("❌ Failed to connect to Kafka (attempt %d/%d): %v", attempt, maxRetries, err)
		log.Printf("⏳ Retrying in %v...", delay)
		time.Sleep(delay)
	}

	log.Fatalf("💀 Failed to connect to Kafka after %d attempts. Kafka might be unavailable.", maxRetries)
	return nil
}

func main() {

	brokerAddress := os.Getenv("BROKER")
	if brokerAddress == "" {
		brokerAddress = "broker0:29092"
	}

	kafkaProducer := connectToKafkaWithRetry(brokerAddress)
	defer kafkaProducer.Close()

	router := gin.Default()

	router.POST("/produce", kafkaProducer.ProduceMessage)

	router.GET("/health", producer.HealthHandler)

	// Webhook simulator endpoint for E2E testing
	router.POST("/webhook-simulator", producer.WebhookSimulator)

	port := os.Getenv("GO_APP_PORT")
	if port == "" {
		port = ":6969"
	}

	log.Printf("Starting server on %s with Kafka broker: %s", port, brokerAddress)
	if err := router.Run(port); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
