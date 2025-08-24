//go:build integration

package main_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	producerpkg "go-http-kafka-producer/pkg"
)

func stringPtr(s string) *string {
	return &s
}

// Integration tests that require a running Kafka instance
// Run these with: go test -tags integration
// Note: Test topic is created/deleted by test.sh script

func TestKafkaProducerIntegration(t *testing.T) {
	// Get broker address from environment
	brokerAddr := os.Getenv("BROKER")
	if brokerAddr == "" {
		brokerAddr = "localhost:9092"
	}

	// Get test topic from environment (created by test.sh)
	testTopic := os.Getenv("TEST_TOPIC")
	if testTopic == "" {
		t.Fatal("TEST_TOPIC environment variable not set - topic should be created by test.sh")
	}

	// Wait a moment for topic creation to propagate
	time.Sleep(2 * time.Second)

	// Create real Kafka producer
	producer, err := producerpkg.NewKafkaProducer(brokerAddr)
	if err != nil {
		t.Skipf("Skipping integration test: could not connect to Kafka at %s: %v", brokerAddr, err)
	}
	defer producer.Close()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/produce", producer.ProduceMessage)

	tests := []struct {
		name    string
		payload producerpkg.ProduceRequest
	}{
		{
			name: "produce with key",
			payload: producerpkg.ProduceRequest{
				Topic:   testTopic,
				Key:     stringPtr("integration-test-key"),
				Payload: map[string]interface{}{"test": "integration", "timestamp": time.Now().Unix()},
			},
		},
		{
			name: "produce without key",
			payload: producerpkg.ProduceRequest{
				Topic:   testTopic,
				Payload: map[string]interface{}{"test": "no-key", "data": []int{1, 2, 3}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonPayload, _ := json.Marshal(tt.payload)
			req, _ := http.NewRequest("POST", "/produce", bytes.NewBuffer(jsonPayload))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)

			var response producerpkg.ProduceResponse
			err := json.Unmarshal(w.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.True(t, response.Success)
			assert.Contains(t, response.Message, "Message delivered")
			assert.Contains(t, response.Message, tt.payload.Topic)
		})
	}
}

func TestKafkaConnectionFailure(t *testing.T) {
	// Test with invalid broker address
	producer, err := producerpkg.NewKafkaProducer("invalid-broker:9999")
	if err != nil {
		t.Skipf("Skipping connection failure test: %v", err)
	}
	defer producer.Close()

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/produce", producer.ProduceMessage)

	payload := producerpkg.ProduceRequest{
		Topic:   "test-topic",
		Payload: map[string]interface{}{"test": "connection-failure"},
	}

	jsonPayload, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", "/produce", bytes.NewBuffer(jsonPayload))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should get an error response
	var response producerpkg.ProduceResponse
	err = json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.False(t, response.Success)
	assert.NotEmpty(t, response.Error)
}
