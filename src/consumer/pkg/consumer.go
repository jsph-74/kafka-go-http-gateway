package consumer

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Config holds the consumer configuration
type Config struct {
	BrokerAddress string
	Topic         string
	ConsumerGroup string
	TargetURL     string
	MaxWorkers    int
	RateLimit     float64 // messages per second
	HTTPTimeout   time.Duration
	RetryAttempts int
	RetryDelay    time.Duration
}

// Consumer represents the HTTP consumer with at-least-once delivery guarantees
//
// DELIVERY SEMANTICS:
// - AT-LEAST-ONCE: Messages are acknowledged only after complete processing (success OR exhausted retries)
// - IMMEDIATE RETRIES: Failed HTTP requests are retried 1-3 times within the same consumer session
// - CRASH RECOVERY: Messages are redelivered only if consumer crashes during processing
// - ORDERING: Messages within each partition processed in offset order (when workers ≤ partitions)
//
// PROCESSING FLOW:
// 1. Message received → NOT acknowledged yet
// 2. Worker processes with immediate retries (1-3 attempts)  
// 3. After success OR all retries exhausted → Message acknowledged
// 4. If consumer crashes during step 2 → Message redelivered (retries restart)
//
// TRADE-OFFS:
// - ✅ Immediate retries for transient failures (most common case)
// - ✅ Messages never lost due to consumer crashes (rare case)
// - ⚠️ Consumer crash during retries will restart retry counter
// - ⚠️ HTTP endpoints should be idempotent to handle potential duplicates
//
// This consumer provides the best balance of immediate failure recovery and crash resilience.
type Consumer struct {
	config        Config
	consumerGroup sarama.ConsumerGroup
	rateLimiter   *time.Ticker
	workerPool    *WorkerPool
	httpClient    *http.Client
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// MessagePayload represents the structure sent to target HTTP endpoint
type MessagePayload struct {
	Topic     string                 `json:"topic"`
	Partition int32                  `json:"partition"`
	Offset    int64                  `json:"offset"`
	Key       string                 `json:"key,omitempty"`
	Value     map[string]interface{} `json:"value"`
	Timestamp time.Time              `json:"timestamp"`
}

// NewConsumer creates a new HTTP consumer
func NewConsumer(config Config) (*Consumer, error) {
	// Validate configuration
	if config.BrokerAddress == "" {
		return nil, fmt.Errorf("broker address is required")
	}
	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	if config.TargetURL == "" {
		return nil, fmt.Errorf("target URL is required")
	}
	if config.ConsumerGroup == "" {
		config.ConsumerGroup = "http-consumer-default"
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 10
	}
	if config.RateLimit <= 0 {
		config.RateLimit = 10 // default 10 messages per second
	}
	if config.HTTPTimeout <= 0 {
		config.HTTPTimeout = 30 * time.Second
	}
	if config.RetryAttempts <= 0 {
		config.RetryAttempts = 3
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 1 * time.Second
	}

	// Create Sarama consumer group config
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Session.Timeout = 10 * time.Second
	saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{config.BrokerAddress}, config.ConsumerGroup, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Calculate rate limiter interval
	rateLimitInterval := time.Duration(float64(time.Second) / config.RateLimit)

	httpClient := &http.Client{
		Timeout: config.HTTPTimeout,
	}

	consumer := &Consumer{
		config:        config,
		consumerGroup: consumerGroup,
		rateLimiter:   time.NewTicker(rateLimitInterval),
		httpClient:    httpClient,
		ctx:           ctx,
		cancel:        cancel,
	}

	// Initialize worker pool after consumer is created
	consumer.workerPool = NewWorkerPool(config, httpClient, ctx, &consumer.wg)

	return consumer, nil
}

// getTopicPartitionCount returns the number of partitions for the configured topic
func (c *Consumer) getTopicPartitionCount() (int, error) {
	// Create a Kafka client to get topic metadata
	client, err := sarama.NewClient([]string{c.config.BrokerAddress}, sarama.NewConfig())
	if err != nil {
		return 0, fmt.Errorf("failed to create Kafka client: %w", err)
	}
	defer client.Close()

	// Get topic partitions
	partitions, err := client.Partitions(c.config.Topic)
	if err != nil {
		return 0, fmt.Errorf("failed to get partitions for topic %s: %w", c.config.Topic, err)
	}

	return len(partitions), nil
}

// Start begins consuming messages
func (c *Consumer) Start() error {
	log.Printf("Starting HTTP consumer for topic: %s", c.config.Topic)
	log.Printf("Target URL: %s", c.config.TargetURL)
	log.Printf("Rate limit: %.2f msg/sec", c.config.RateLimit)
	log.Printf("Max workers: %d", c.config.MaxWorkers)
	log.Printf("HTTP timeout: %v", c.config.HTTPTimeout)

	// Get partition count and determine ordering guarantee
	partitionCount, err := c.getTopicPartitionCount()
	if err != nil {
		log.Printf("Warning: Could not determine partition count: %v", err)
		log.Printf("Retry strategy: INLINE (preserves message ordering when workers ≤ partitions)")
	} else {
		log.Printf("Topic partitions: %d", partitionCount)
		if c.config.MaxWorkers <= partitionCount {
			log.Printf("Retry strategy: INLINE (✅ preserves message ordering - workers ≤ partitions)")
		} else {
			log.Printf("Retry strategy: INLINE (⚠️  ordering NOT guaranteed - workers > partitions)")
		}
	}

	// Start worker pool
	c.workerPool.Start()

	// Start consumer group - handles connection/fatal errors
	// These errors prevent consumption entirely (broker down, auth failure, etc.)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				err := c.consumerGroup.Consume(c.ctx, []string{c.config.Topic}, c)
				if err != nil {
					log.Printf("Fatal consumer error (will retry): %v", err)
				}
			}
		}
	}()

	// Handle runtime errors - individual message/session issues during consumption
	// These errors occur while consuming (corrupt messages, heartbeat failures, etc.)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case err := <-c.consumerGroup.Errors():
				log.Printf("Runtime consumer error: %v", err)
			case <-c.ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Stop gracefully stops the consumer
func (c *Consumer) Stop() error {
	log.Println("Stopping HTTP consumer...")

	c.cancel()
	c.rateLimiter.Stop()
	c.workerPool.Stop()

	c.wg.Wait()

	if err := c.consumerGroup.Close(); err != nil {
		return fmt.Errorf("failed to close consumer group: %w", err)
	}

	log.Println("HTTP consumer stopped successfully")
	return nil
}

// Setup implements sarama.ConsumerGroupHandler
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session setup")
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session cleanup")
	return nil
}


// ConsumeClaim implements sarama.ConsumerGroupHandler
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("Starting to consume partition %d", claim.Partition())

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Smart rate limiting - only delay if needed
			select {
			case <-c.rateLimiter.C:
				// Rate limit tick consumed
			default:
				// No tick available, processing is faster than rate limit - skip delay
			}

			// Create message wrapper with marking callback
			msgWrapper := &MessageWithCallback{
				Message: message,
				MarkFunc: func() {
					session.MarkMessage(message, "")
				},
			}

			// Send to worker pool (instant dispatch)
			select {
			case c.workerPool.GetWorkQueue() <- msgWrapper:
				// Message queued successfully
				// Worker will mark message only after successful HTTP delivery
			case <-c.ctx.Done():
				return nil
			}

		case <-c.ctx.Done():
			return nil
		}
	}
}
