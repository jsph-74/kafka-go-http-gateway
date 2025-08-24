package main_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	producerpkg "go-http-kafka-producer/pkg"
)

func StringPtr(s string) *string {
	return &s
}

func TestProduceMessage_ValidRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		payload        producerpkg.ProduceRequest
		expectedStatus int
		expectSuccess  bool
	}{
		{
			name: "valid request with key",
			payload: producerpkg.ProduceRequest{
				Topic:   "test-topic",
				Key:     StringPtr("user-123"),
				Payload: map[string]interface{}{"message": "hello world", "user_id": 123},
			},
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name: "valid request without key",
			payload: producerpkg.ProduceRequest{
				Topic:   "test-topic",
				Payload: map[string]interface{}{"event": "user_login", "timestamp": 1629454800},
			},
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name: "valid request with array payload",
			payload: producerpkg.ProduceRequest{
				Topic:   "batch-topic",
				Payload: map[string]interface{}{"items": []interface{}{map[string]interface{}{"id": 1}, map[string]interface{}{"id": 2}}},
			},
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create Sarama mock producer
			mockProducer := mocks.NewSyncProducer(t, nil)
			
			// Set expectation for successful message send
			mockProducer.ExpectSendMessageAndSucceed()
			
			// Create KafkaProducer with mock
			kp := producerpkg.NewKafkaProducerWithClient(mockProducer)
			
			// Setup router
			router := gin.New()
			router.POST("/produce", kp.ProduceMessage)

			jsonPayload, _ := json.Marshal(tt.payload)
			req, _ := http.NewRequest("POST", "/produce", bytes.NewBuffer(jsonPayload))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			var response producerpkg.ProduceResponse
			err := json.Unmarshal(w.Body.Bytes(), &response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectSuccess, response.Success)
			
			// Verify all expectations were met
			assert.NoError(t, mockProducer.Close())
		})
	}
}

func TestProduceMessage_InvalidRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		requestBody    string
		expectedStatus int
		expectSuccess  bool
	}{
		{
			name:           "missing topic",
			requestBody:    `{"payload": {"message": "test"}}`,
			expectedStatus: http.StatusBadRequest,
			expectSuccess:  false,
		},
		{
			name:           "missing payload",
			requestBody:    `{"topic": "test-topic"}`,
			expectedStatus: http.StatusBadRequest,
			expectSuccess:  false,
		},
		{
			name:           "empty topic",
			requestBody:    `{"topic": "", "payload": {"message": "test"}}`,
			expectedStatus: http.StatusBadRequest,
			expectSuccess:  false,
		},
		{
			name:           "invalid json",
			requestBody:    `{"topic": "test", "payload": {"message": "test"`,
			expectedStatus: http.StatusBadRequest,
			expectSuccess:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock producer - won't be used for validation errors
			mockProducer := mocks.NewSyncProducer(t, nil)
			kp := producerpkg.NewKafkaProducerWithClient(mockProducer)
			
			router := gin.New()
			router.POST("/produce", kp.ProduceMessage)

			req, _ := http.NewRequest("POST", "/produce", bytes.NewBufferString(tt.requestBody))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			var response producerpkg.ProduceResponse
			err := json.Unmarshal(w.Body.Bytes(), &response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectSuccess, response.Success)
			assert.NotEmpty(t, response.Error)
			
			assert.NoError(t, mockProducer.Close())
		})
	}
}

func TestProduceMessage_KafkaErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name          string
		kafkaError    error
		expectedStatus int
	}{
		{
			name:          "broker not available",
			kafkaError:    sarama.ErrBrokerNotAvailable,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:          "not leader for partition",
			kafkaError:    sarama.ErrNotLeaderForPartition,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:          "message too large",
			kafkaError:    sarama.ErrMessageSizeTooLarge,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProducer := mocks.NewSyncProducer(t, nil)
			
			// Expect the send to fail with specific error
			mockProducer.ExpectSendMessageAndFail(tt.kafkaError)
			
			kp := producerpkg.NewKafkaProducerWithClient(mockProducer)
			
			router := gin.New()
			router.POST("/produce", kp.ProduceMessage)

			payload := producerpkg.ProduceRequest{
				Topic:   "test-topic",
				Payload: map[string]interface{}{"message": "test"},
			}
			jsonPayload, _ := json.Marshal(payload)
			
			req, _ := http.NewRequest("POST", "/produce", bytes.NewBuffer(jsonPayload))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			var response producerpkg.ProduceResponse
			err := json.Unmarshal(w.Body.Bytes(), &response)
			assert.NoError(t, err)
			assert.False(t, response.Success)
			assert.Contains(t, response.Error, "Failed to produce message")
			
			assert.NoError(t, mockProducer.Close())
		})
	}
}
