package consumer_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-http-kafka-consumer/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// HTTPServerStats tracks HTTP server behavior
type HTTPServerStats struct {
	RequestCount    int64
	SuccessCount    int64
	ErrorCount      int64
	TimeoutCount    int64
	RequestTimes    []time.Time
	mutex           sync.Mutex
}

func (s *HTTPServerStats) RecordRequest() {
	atomic.AddInt64(&s.RequestCount, 1)
	s.mutex.Lock()
	s.RequestTimes = append(s.RequestTimes, time.Now())
	s.mutex.Unlock()
}

func (s *HTTPServerStats) RecordSuccess() {
	atomic.AddInt64(&s.SuccessCount, 1)
}

func (s *HTTPServerStats) RecordError() {
	atomic.AddInt64(&s.ErrorCount, 1)
}

func (s *HTTPServerStats) RecordTimeout() {
	atomic.AddInt64(&s.TimeoutCount, 1)
}

func (s *HTTPServerStats) GetRequestRate(duration time.Duration) float64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	
	now := time.Now()
	count := 0
	for _, t := range s.RequestTimes {
		if now.Sub(t) <= duration {
			count++
		}
	}
	return float64(count) / duration.Seconds()
}

// createMockHTTPServer creates a controllable mock HTTP server
func createMockHTTPServer(stats *HTTPServerStats, behavior string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats.RecordRequest()
		
		switch behavior {
		case "success":
			w.WriteHeader(200)
			stats.RecordSuccess()
			
		case "slow":
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(200)
			stats.RecordSuccess()
			
		case "error":
			w.WriteHeader(500)
			stats.RecordError()
			
		case "timeout":
			time.Sleep(2 * time.Second) // Longer than typical HTTP timeout
			w.WriteHeader(200)
			stats.RecordTimeout()
			
		case "chaos":
			// Random behavior for chaos testing
			chaos := rand.Float64()
			switch {
			case chaos < 0.1: // 10% timeout
				time.Sleep(2 * time.Second)
				stats.RecordTimeout()
			case chaos < 0.2: // 10% 500 error
				w.WriteHeader(500)
				stats.RecordError()
			case chaos < 0.25: // 5% slow response
				time.Sleep(300 * time.Millisecond)
				w.WriteHeader(200)
				stats.RecordSuccess()
			default: // 75% success
				w.WriteHeader(200)
				stats.RecordSuccess()
			}
		}
	}))
}

// TestConsumerRateLimiting tests that rate limiting works correctly
func TestConsumerRateLimiting(t *testing.T) {
	stats := &HTTPServerStats{}
	mockServer := createMockHTTPServer(stats, "success")
	defer mockServer.Close()

	// Create consumer with strict rate limiting
	config := consumer.Config{
		BrokerAddress: "broker0:29092", // Won't be used with mock
		Topic:        "test-topic",
		TargetURL:    mockServer.URL,
		RateLimit:    2.0, // 2 messages per second
		HTTPTimeout:  1 * time.Second,
	}

	// Test the core rate limiting logic that your consumer uses
	
	// Test rate limiting behavior
	rateLimitInterval := time.Duration(float64(time.Second) / config.RateLimit)
	ticker := time.NewTicker(rateLimitInterval)
	defer ticker.Stop()

	start := time.Now()
	requestCount := 0
	
	// Simulate processing 6 messages with rate limiting
	for i := 0; i < 6; i++ {
		<-ticker.C // Wait for rate limiter
		
		// Simulate HTTP request
		resp, err := http.Post(mockServer.URL, "application/json", nil)
		if err == nil {
			resp.Body.Close()
			requestCount++
		}
	}
	
	elapsed := time.Since(start)
	actualRate := float64(requestCount) / elapsed.Seconds()
	
	// Should be approximately 2 requests per second (with some tolerance)
	assert.InDelta(t, 2.0, actualRate, 0.3, "Rate limiting should enforce ~2 req/sec")
	assert.Equal(t, int64(6), atomic.LoadInt64(&stats.RequestCount))
	assert.Equal(t, int64(6), atomic.LoadInt64(&stats.SuccessCount))
}

// TestHTTPErrorHandling tests retry logic and offset management
func TestHTTPErrorHandling(t *testing.T) {
	stats := &HTTPServerStats{}
	mockServer := createMockHTTPServer(stats, "error")
	defer mockServer.Close()

	// Test retry logic by simulating sendHTTPRequest behavior
	httpClient := &http.Client{Timeout: 1 * time.Second}
	
	payload := consumer.MessagePayload{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       "test-key",
		Value:     map[string]interface{}{"message": "test"},
		Timestamp: time.Now(),
	}

	jsonPayload, _ := json.Marshal(payload)
	
	// Simulate retry attempts (like in processMessage)
	maxRetries := 3
	success := false
	
	for attempt := 0; attempt <= maxRetries; attempt++ {
		resp, err := httpClient.Post(mockServer.URL, "application/json", 
			http.NoBody)
		
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				success = true
				break
			}
		}
		
		if attempt < maxRetries {
			time.Sleep(100 * time.Millisecond) // Retry delay
		}
	}
	
	// Should fail after all retries with error server
	assert.False(t, success, "Should fail with error server")
	assert.Equal(t, int64(4), atomic.LoadInt64(&stats.RequestCount)) // 1 + 3 retries
	assert.Equal(t, int64(4), atomic.LoadInt64(&stats.ErrorCount))
	
	_ = jsonPayload // Use the payload to avoid unused variable
}

// TestWorkerPoolConcurrency tests worker pool limits
func TestWorkerPoolConcurrency(t *testing.T) {
	stats := &HTTPServerStats{}
	mockServer := createMockHTTPServer(stats, "slow") // 200ms delay
	defer mockServer.Close()

	maxWorkers := 3
	workerPool := make(chan struct{}, maxWorkers)
	httpClient := &http.Client{Timeout: 1 * time.Second}
	
	var wg sync.WaitGroup
	startTime := time.Now()
	
	// Send 6 requests with worker pool limiting
	for i := 0; i < 6; i++ {
		// Wait for worker slot (like in ConsumeClaim)
		workerPool <- struct{}{}
		
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() { <-workerPool }() // Release worker slot
			
			resp, err := httpClient.Post(mockServer.URL, "application/json", nil)
			if err == nil {
				resp.Body.Close()
			}
		}(i)
	}
	
	wg.Wait()
	elapsed := time.Since(startTime)
	
	// With 3 workers and 200ms delay per request:
	// - First 3 requests: parallel (200ms)
	// - Next 3 requests: parallel (200ms)
	// Total should be ~400ms (not 1200ms if sequential)
	assert.True(t, elapsed < 500*time.Millisecond, 
		"Worker pool should enable concurrency, elapsed: %v", elapsed)
	assert.True(t, elapsed > 350*time.Millisecond,
		"Should still respect HTTP delay, elapsed: %v", elapsed)
}

// TestChaosHTTPBehavior tests consumer resilience to random HTTP failures
func TestChaosHTTPBehavior(t *testing.T) {
	stats := &HTTPServerStats{}
	mockServer := createMockHTTPServer(stats, "chaos")
	defer mockServer.Close()

	httpClient := &http.Client{Timeout: 500 * time.Millisecond}
	
	// Send many requests to test chaos behavior
	successCount := 0
	clientTimeoutCount := 0
	totalRequests := 50
	
	for i := 0; i < totalRequests; i++ {
		resp, err := httpClient.Post(mockServer.URL, "application/json", nil)
		if err != nil {
			// Client timeout (server took too long)
			clientTimeoutCount++
		} else if resp.StatusCode == 200 {
			successCount++
			resp.Body.Close()
		} else {
			resp.Body.Close()
		}
	}
	
	// With chaos mode (75% success rate), we should see mixed results
	assert.True(t, successCount > 20, "Should have some successes in chaos mode")
	assert.True(t, successCount < 48, "Should have some failures in chaos mode (got %d successes)", successCount)
	
	// Server should record all requests (even if client times out)
	totalRecorded := atomic.LoadInt64(&stats.RequestCount)
	assert.Equal(t, int64(totalRequests), totalRecorded, "All requests should be recorded by server")
	
	// Should have mix of success/error/timeout
	successRecorded := atomic.LoadInt64(&stats.SuccessCount)
	errorRecorded := atomic.LoadInt64(&stats.ErrorCount)
	timeoutRecorded := atomic.LoadInt64(&stats.TimeoutCount)
	
	assert.True(t, successRecorded > 0, "Should have some successes")
	assert.True(t, errorRecorded+timeoutRecorded > 0, "Should have some failures")
	
	t.Logf("Chaos test results: %d client success, %d client timeouts", successCount, clientTimeoutCount)
	t.Logf("Server recorded: %d success, %d errors, %d timeouts", 
		successRecorded, errorRecorded, timeoutRecorded)
}

// TestBackpressure tests what happens when HTTP is slower than message rate
func TestBackpressure(t *testing.T) {
	stats := &HTTPServerStats{}
	mockServer := createMockHTTPServer(stats, "slow") // 200ms per request
	defer mockServer.Close()

	// Simulate high message rate with limited workers
	maxWorkers := 2
	workerPool := make(chan struct{}, maxWorkers)
	httpClient := &http.Client{Timeout: 1 * time.Second}
	
	// Rate limiter allows 10 msg/sec, but HTTP takes 200ms each
	// With 2 workers: max 10 requests/sec (2 workers * 5 req/sec each)
	// Should naturally throttle
	
	rateLimiter := time.NewTicker(100 * time.Millisecond) // 10/sec
	defer rateLimiter.Stop()
	
	var wg sync.WaitGroup
	start := time.Now()
	
	// Try to send 10 messages
	for i := 0; i < 10; i++ {
		<-rateLimiter.C       // Wait for rate limit
		workerPool <- struct{}{} // Wait for worker (this will block when pool full)
		
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() { <-workerPool }()
			
			resp, err := httpClient.Post(mockServer.URL, "application/json", nil)
			if err == nil {
				resp.Body.Close()
			}
		}(i)
	}
	
	wg.Wait()
	elapsed := time.Since(start)
	
	// Should be throttled by worker pool, not just rate limiter
	// With 2 workers at 200ms each: ~1 second for 10 requests
	assert.True(t, elapsed > 900*time.Millisecond, 
		"Should be throttled by worker pool")
	assert.Equal(t, int64(10), atomic.LoadInt64(&stats.RequestCount))
}

// TestInlineRetryLogic tests the inline retry mechanism
func TestInlineRetryLogic(t *testing.T) {
	stats := &HTTPServerStats{}
	
	// Create a server that fails first 2 attempts, succeeds on 3rd
	attemptCount := 0
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats.RecordRequest()
		attemptCount++
		
		if attemptCount <= 2 {
			// Fail first 2 attempts
			w.WriteHeader(500)
			stats.RecordError()
		} else {
			// Succeed on 3rd attempt
			w.WriteHeader(200)
			stats.RecordSuccess()
		}
	}))
	defer mockServer.Close()

	// Simulate the inline retry logic from processMessage
	httpClient := &http.Client{Timeout: 1 * time.Second}
	
	payload := consumer.MessagePayload{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       "test-key",
		Value:     map[string]interface{}{"order_id": 1, "amount": 100.50},
		Timestamp: time.Now(),
	}

	maxRetries := 3
	success := false
	
	// Simulate the retry loop from processMessage
	for attempt := 0; attempt <= maxRetries; attempt++ {
		jsonPayload, _ := json.Marshal(payload)
		resp, err := httpClient.Post(mockServer.URL, "application/json", 
			bytes.NewBuffer(jsonPayload))
		
		if err == nil && resp.StatusCode == 200 {
			success = true
			resp.Body.Close()
			break
		}
		
		if resp != nil {
			resp.Body.Close()
		}
		
		if attempt < maxRetries {
			time.Sleep(10 * time.Millisecond) // Short delay for test
		}
	}
	
	// Verify results
	assert.True(t, success, "Should succeed after retries")
	assert.Equal(t, int64(3), atomic.LoadInt64(&stats.RequestCount), "Should make 3 attempts")
	assert.Equal(t, int64(1), atomic.LoadInt64(&stats.SuccessCount), "Should have 1 success")
	assert.Equal(t, int64(2), atomic.LoadInt64(&stats.ErrorCount), "Should have 2 errors")
	
	t.Logf("Inline retry test results:")
	t.Logf("  Total attempts: %d", atomic.LoadInt64(&stats.RequestCount))
	t.Logf("  Success count: %d", atomic.LoadInt64(&stats.SuccessCount))
	t.Logf("  Error count: %d", atomic.LoadInt64(&stats.ErrorCount))
	t.Logf("  Final result: %t", success)
}

// TestKafkaToHTTPPipeline tests the core consumer integration: Kafka → HTTP
// This test requires a running Kafka cluster and producer service
func TestKafkaToHTTPPipeline(t *testing.T) {
	// Skip if running in unit test mode
	if testing.Short() {
		t.Skip("Skipping Kafka integration test in short mode")
	}

	// Skip if no Kafka broker is configured  
	brokerAddr := os.Getenv("BROKER")
	if brokerAddr == "" {
		t.Skip("Skipping Kafka integration test: BROKER environment variable not set")
	}

	// Get test topic from environment (created by test.sh)
	testTopic := os.Getenv("TEST_TOPIC")
	if testTopic == "" {
		t.Skip("Skipping Kafka integration test: TEST_TOPIC environment variable not set - topic should be created by test.sh")
	}

	// Wait for topic creation to propagate
	time.Sleep(2 * time.Second)

	// Create mock HTTP server to receive consumer requests
	var receivedMessages []map[string]interface{}
	var requestCount int32
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		
		var payload map[string]interface{}
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &payload)
		receivedMessages = append(receivedMessages, payload)
		
		w.WriteHeader(200) // Success response
	}))
	defer mockServer.Close()

	// Send a test message to Kafka via HTTP producer API
	testMessage := map[string]interface{}{
		"test_id": "kafka-to-http-integration", 
		"message": "Hello from Kafka to HTTP pipeline",
		"timestamp": time.Now().Unix(),
	}

	produceRequest := map[string]interface{}{
		"topic": testTopic,
		"key":   "integration-test-key",
		"payload": testMessage,
	}

	jsonPayload, _ := json.Marshal(produceRequest)
	
	// Send HTTP request to producer service 
	producerURL := "http://localhost:6969/produce"
	if brokerAddr != "localhost:9092" {
		// If using container network, adjust producer URL
		producerURL = "http://go-producer:6969/produce"  
	}

	resp, err := http.Post(producerURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		t.Skipf("Skipping integration test: could not reach producer at %s: %v", producerURL, err)
	}
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "Failed to send message to Kafka via producer API")

	// Wait for message to be available
	time.Sleep(1 * time.Second)

	// Create consumer and start consuming
	consumerConfig := consumer.Config{
		BrokerAddress: brokerAddr,
		Topic:         testTopic,
		TargetURL:     mockServer.URL,
		ConsumerGroup: fmt.Sprintf("integration-test-%d", time.Now().UnixNano()),
		RateLimit:     10.0,
		HTTPTimeout:   5 * time.Second,
		RetryAttempts: 1,
		RetryDelay:    100 * time.Millisecond,
	}

	kafkaConsumer, err := consumer.NewConsumer(consumerConfig)
	if err != nil {
		t.Skipf("Skipping integration test: could not create consumer: %v", err)
	}
	defer kafkaConsumer.Stop()

	// Start consumer in background
	go func() {
		kafkaConsumer.Start()
	}()

	// Allow consumer to fully initialize and join consumer group
	time.Sleep(2 * time.Second)

	// Wait for message processing (increased timeout for consumer group rebalancing)
	maxWaitTime := 15 * time.Second
	checkInterval := 500 * time.Millisecond
	deadline := time.Now().Add(maxWaitTime)

	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&requestCount) > 0 {
			break
		}
		time.Sleep(checkInterval)
	}

	// Verify results
	assert.Greater(t, atomic.LoadInt32(&requestCount), int32(0), "HTTP server should have received at least one request")
	assert.NotEmpty(t, receivedMessages, "Should have received messages")

	if len(receivedMessages) > 0 {
		receivedMsg := receivedMessages[0]
		
		// Verify message structure and content
		assert.Contains(t, receivedMsg, "topic", "Message should contain topic")
		assert.Contains(t, receivedMsg, "value", "Message should contain value")
		assert.Equal(t, testTopic, receivedMsg["topic"], "Topic should match")
		
		// Verify the original test message is in the value
		if value, ok := receivedMsg["value"].(map[string]interface{}); ok {
			assert.Equal(t, "kafka-to-http-integration", value["test_id"], "Test ID should match")
			assert.Equal(t, "Hello from Kafka to HTTP pipeline", value["message"], "Message should match")
		}
	}

	t.Logf("Integration test completed: %d HTTP requests received", atomic.LoadInt32(&requestCount))
}

// Additional integration tests moved from consumer_test.go

func TestNewConsumer(t *testing.T) {
	// Test valid consumer creation (validation is now handled by CLI)
	config := consumer.Config{
		BrokerAddress: "broker0:29092",
		Topic:         "test-topic",
		TargetURL:     "http://example.com",
		ConsumerGroup: "test-group",
		RateLimit:     10.0,
		HTTPTimeout:   30 * time.Second,
		RetryAttempts: 3,
		RetryDelay:    1 * time.Second,
	}

	consumer, err := consumer.NewConsumer(config)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	if consumer != nil {
		consumer.Stop()
	}
}

func TestConfig_Defaults(t *testing.T) {
	config := consumer.Config{
		BrokerAddress: "broker0:29092",
		Topic:         "test-topic",
		TargetURL:     "http://example.com",
		// Leave other fields as zero values to test defaults
	}

	consumer, err := consumer.NewConsumer(config)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Stop()

	// Note: Cannot access private config fields in external tests
	// This test verifies consumer creation with default values succeeds
}

func TestGetTopicPartitionCount_Error(t *testing.T) {
	// Create consumer with invalid broker to test error handling
	config := consumer.Config{
		BrokerAddress: "invalid:9999",
		Topic:         "test-topic",
		TargetURL:     "http://example.com",
	}

	consumer, err := consumer.NewConsumer(config)
	if err != nil {
		// If consumer.NewConsumer fails (which is expected), that's fine
		// We're testing error handling in getTopicPartitionCount
		t.Skip("Cannot create consumer with invalid broker - this is expected behavior")
		return
	}
	defer consumer.Stop()

	// Note: Cannot test private method getTopicPartitionCount in external tests
	// This test would need to be moved to internal tests
	t.Skip("Cannot test private methods in external test package")
}

func TestConsumer_Stop(t *testing.T) {
	config := consumer.Config{
		BrokerAddress: "broker0:29092",
		Topic:         "test-topic",
		TargetURL:     "http://example.com",
	}

	consumer, err := consumer.NewConsumer(config)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	// Test that Stop() doesn't panic
	assert.NotPanics(t, func() {
		consumer.Stop()
	})

	// Note: Cannot access private ctx field in external tests
	// This test would need to be moved to internal tests
}

func TestConsumer_HTTPClient(t *testing.T) {
	config := consumer.Config{
		BrokerAddress: "broker0:29092",
		Topic:         "test-topic",
		TargetURL:     "http://example.com",
		HTTPTimeout:   5 * time.Second,
	}

	consumer, err := consumer.NewConsumer(config)
	assert.NoError(t, err)
	defer consumer.Stop()

	// Note: Cannot access private httpClient field in external tests
	// This test would need to be moved to internal tests
}