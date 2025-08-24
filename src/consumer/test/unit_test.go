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

func TestCLIConfig_ValidateAndSetDefaults(t *testing.T) {
	tests := []struct {
		name        string
		config      consumer.CLIConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid_config",
			config: consumer.CLIConfig{
				BrokerAddress: "localhost:9092",
				Topic:         "test-topic",
				TargetURL:     "http://example.com",
			},
			expectError: false,
		},
		{
			name: "missing_broker",
			config: consumer.CLIConfig{
				Topic:     "test-topic",
				TargetURL: "http://example.com",
			},
			expectError: true,
			errorMsg:    "broker address is required",
		},
		{
			name: "missing_topic",
			config: consumer.CLIConfig{
				BrokerAddress: "localhost:9092",
				TargetURL:     "http://example.com",
			},
			expectError: true,
			errorMsg:    "topic is required",
		},
		{
			name: "missing_target_url",
			config: consumer.CLIConfig{
				BrokerAddress: "localhost:9092",
				Topic:         "test-topic",
			},
			expectError: true,
			errorMsg:    "target URL is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.ValidateAndSetDefaults()
			
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
				
				// Check that defaults were set
				assert.Equal(t, "http-consumer-default", tt.config.ConsumerGroup)
				assert.Equal(t, 15, tt.config.MaxWorkers)
				assert.Equal(t, 10.0, tt.config.RateLimit)
				assert.Equal(t, 30*time.Second, tt.config.HTTPTimeout)
				assert.Equal(t, 3, tt.config.RetryAttempts)
				assert.Equal(t, 1*time.Second, tt.config.RetryDelay)
			}
		})
	}
}

func TestCLIConfig_ToConfig(t *testing.T) {
	cliConfig := consumer.CLIConfig{
		BrokerAddress: "localhost:9092",
		Topic:         "test-topic",
		ConsumerGroup: "test-group",
		TargetURL:     "http://example.com",
		MaxWorkers:    5,
		RateLimit:     20.0,
		HTTPTimeout:   60 * time.Second,
		RetryAttempts: 2,
		RetryDelay:    2 * time.Second,
	}

	config := cliConfig.ToConfig()

	assert.Equal(t, cliConfig.BrokerAddress, config.BrokerAddress)
	assert.Equal(t, cliConfig.Topic, config.Topic)
	assert.Equal(t, cliConfig.ConsumerGroup, config.ConsumerGroup)
	assert.Equal(t, cliConfig.TargetURL, config.TargetURL)
	assert.Equal(t, cliConfig.MaxWorkers, config.MaxWorkers)
	assert.Equal(t, cliConfig.RateLimit, config.RateLimit)
	assert.Equal(t, cliConfig.HTTPTimeout, config.HTTPTimeout)
	assert.Equal(t, cliConfig.RetryAttempts, config.RetryAttempts)
	assert.Equal(t, cliConfig.RetryDelay, config.RetryDelay)
}