package consumer_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go-http-kafka-consumer/pkg"
	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	// Test rate limiting logic
	rateLimit := 5.0 // 5 messages per second
	rateLimitInterval := time.Duration(float64(time.Second) / rateLimit)
	
	expected := 200 * time.Millisecond // 1/5 of a second
	assert.Equal(t, expected, rateLimitInterval)
	
	// Test with different rate
	rateLimit = 10.0
	rateLimitInterval = time.Duration(float64(time.Second) / rateLimit)
	expected = 100 * time.Millisecond
	assert.Equal(t, expected, rateLimitInterval)
}

func TestMessagePayload_JSON(t *testing.T) {
	payload := consumer.MessagePayload{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       "test-key",
		Value:     map[string]interface{}{"message": "test"},
		Timestamp: time.Now(),
	}

	// Test JSON marshaling
	jsonData, err := json.Marshal(payload)
	assert.NoError(t, err)
	assert.NotEmpty(t, jsonData)

	// Test that JSON contains expected fields
	assert.Contains(t, string(jsonData), "test-topic")
	assert.Contains(t, string(jsonData), "test-key")
	assert.Contains(t, string(jsonData), "message")
}

func TestHTTPServerStats(t *testing.T) {
	var requestCount int64
	
	// Create a test server that counts requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &http.Client{Timeout: 1 * time.Second}
	
	// Make several requests
	for i := 0; i < 3; i++ {
		resp, err := client.Get(server.URL)
		assert.NoError(t, err)
		if resp != nil {
			resp.Body.Close()
		}
	}
	
	// Verify request count
	assert.Equal(t, int64(3), atomic.LoadInt64(&requestCount))
}