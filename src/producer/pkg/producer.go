package producer

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

// ProduceRequest represents the JSON structure for producing a message to Kafka
type ProduceRequest struct {
	Topic   string                 `json:"topic" binding:"required"`
	Key     *string                `json:"key,omitempty"`
	Payload map[string]interface{} `json:"payload" binding:"required"`
}

// ProduceResponse represents the response structure
type ProduceResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// KafkaProducer wraps the Sarama producer
type KafkaProducer struct {
	producer sarama.SyncProducer
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer(brokerAddress string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer([]string{brokerAddress}, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{producer: producer}, nil
}

// NewKafkaProducerWithClient creates a KafkaProducer with an existing Sarama producer client (useful for testing)
func NewKafkaProducerWithClient(producer sarama.SyncProducer) *KafkaProducer {
	return &KafkaProducer{producer: producer}
}

// ProduceMessage handles HTTP requests to produce messages to Kafka
func (kp *KafkaProducer) ProduceMessage(c *gin.Context) {
	var req ProduceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ProduceResponse{
			Success: false,
			Error:   "Invalid JSON format: " + err.Error(),
		})
		return
	}

	// Convert payload to JSON string
	payloadJSON, err := json.Marshal(req.Payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ProduceResponse{
			Success: false,
			Error:   "Failed to marshal payload: " + err.Error(),
		})
		return
	}

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: req.Topic,
		Value: sarama.StringEncoder(payloadJSON),
	}

	// Add key if provided
	if req.Key != nil {
		msg.Key = sarama.StringEncoder(*req.Key)
	}

	// Send message to Kafka
	partition, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to produce message: %v", err)
		c.JSON(http.StatusInternalServerError, ProduceResponse{
			Success: false,
			Error:   "Failed to produce message: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, ProduceResponse{
		Success: true,
		Message: fmt.Sprintf("Message delivered to topic %s, partition %d, offset %d", req.Topic, partition, offset),
	})
}

// Close closes the Kafka producer
func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}