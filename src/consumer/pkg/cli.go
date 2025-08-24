package consumer

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// CLIConfig holds command line configuration
type CLIConfig struct {
	BrokerAddress string
	Topic         string
	ConsumerGroup string
	TargetURL     string
	MaxWorkers    int
	RateLimit     float64
	HTTPTimeout   time.Duration
	RetryAttempts int
	RetryDelay    time.Duration
}

// ParseFlags parses command line flags and returns configuration
func ParseFlags() *CLIConfig {
	config := &CLIConfig{}

	flag.StringVar(&config.BrokerAddress, "broker", "", "Kafka broker address (e.g., localhost:9092)")
	flag.StringVar(&config.Topic, "topic", "", "Kafka topic to consume from")
	flag.StringVar(&config.ConsumerGroup, "group", "http-consumer", "Consumer group ID")
	flag.StringVar(&config.TargetURL, "target-url", "", "Target HTTP URL to send messages to")
	flag.IntVar(&config.MaxWorkers, "workers", 5, "Maximum number of concurrent workers")
	flag.Float64Var(&config.RateLimit, "rate", 10.0, "Rate limit in messages per second")

	var httpTimeoutSeconds int
	flag.IntVar(&httpTimeoutSeconds, "timeout", 30, "HTTP timeout in seconds")

	flag.IntVar(&config.RetryAttempts, "retries", 3, "Number of retry attempts for failed HTTP requests")

	var retryDelaySeconds int
	flag.IntVar(&retryDelaySeconds, "retry-delay", 1, "Delay between retries in seconds")

	var showHelp bool
	flag.BoolVar(&showHelp, "help", false, "Show help message")
	flag.BoolVar(&showHelp, "h", false, "Show help message")

	flag.Parse()

	if showHelp {
		printUsage()
		os.Exit(0)
	}

	// Convert time durations
	config.HTTPTimeout = time.Duration(httpTimeoutSeconds) * time.Second
	config.RetryDelay = time.Duration(retryDelaySeconds) * time.Second

	return config
}

// Validate checks if the configuration is valid
func (c *CLIConfig) Validate() error {
	if c.BrokerAddress == "" {
		return fmt.Errorf("broker address is required (use -broker)")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic is required (use -topic)")
	}
	if c.TargetURL == "" {
		return fmt.Errorf("target URL is required (use -target-url)")
	}
	if c.MaxWorkers <= 0 {
		return fmt.Errorf("workers must be greater than 0")
	}
	if c.RateLimit <= 0 {
		return fmt.Errorf("rate limit must be greater than 0")
	}
	if c.HTTPTimeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}
	if c.RetryAttempts < 0 {
		return fmt.Errorf("retry attempts must be non-negative")
	}
	if c.RetryDelay <= 0 {
		return fmt.Errorf("retry delay must be greater than 0")
	}

	return nil
}

// ToConfig converts CLIConfig to Config
func (c *CLIConfig) ToConfig() Config {
	return Config{
		BrokerAddress: c.BrokerAddress,
		Topic:         c.Topic,
		ConsumerGroup: c.ConsumerGroup,
		TargetURL:     c.TargetURL,
		MaxWorkers:    c.MaxWorkers,
		RateLimit:     c.RateLimit,
		HTTPTimeout:   c.HTTPTimeout,
		RetryAttempts: c.RetryAttempts,
		RetryDelay:    c.RetryDelay,
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Kafka HTTP Consumer

This consumer reads messages from a Kafka topic and forwards them to an HTTP endpoint.
Messages are only acknowledged (committed) if the HTTP endpoint returns a 200 status code.

Usage:
  %s [options]

Required Options:
  -broker string      Kafka broker address (e.g., localhost:9092)
  -topic string       Kafka topic to consume from
  -target-url string  Target HTTP URL to send messages to

Optional Options:
  -group string       Consumer group ID (default: "http-consumer")
  -workers int        Maximum concurrent workers (default: 5)
  -rate float         Rate limit in messages per second (default: 10.0)
  -timeout int        HTTP timeout in seconds (default: 30)
  -retries int        Number of retry attempts (default: 3)
  -retry-delay int    Delay between retries in seconds (default: 1)
  -help, -h          Show this help message

Examples:
  # Basic usage
  %s -broker localhost:9092 -topic orders -target-url http://api.example.com/webhook

  # With custom rate limiting and workers
  %s -broker localhost:9092 -topic events \
    -target-url http://slow-api.com/process \
    -workers 2 -rate 5.0 -timeout 60

  # High throughput setup
  %s -broker kafka1:9092 -topic high-volume \
    -target-url http://fast-api.com/bulk \
    -workers 20 -rate 100.0 -timeout 10

Message Format:
  The consumer sends messages to the target URL as JSON with this structure:
  {
    "topic": "topic-name",
    "partition": 0,
    "offset": 12345,
    "key": "message-key",
    "value": {...},
    "timestamp": "2023-01-01T12:00:00Z"
  }

Behavior:
  - Messages are processed with respect to rate limiting and worker concurrency
  - Only HTTP 200 responses will result in message acknowledgment
  - Failed messages are retried with exponential backoff
  - Consumer gracefully handles Kafka rebalancing
  - Ctrl+C for graceful shutdown

`, os.Args[0], os.Args[0], os.Args[0], os.Args[0])
}

// RunConsumer runs the consumer with CLI configuration
func RunConsumer() error {
	cliConfig := ParseFlags()

	if err := cliConfig.Validate(); err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	config := cliConfig.ToConfig()

	return runConsumer(config)
}

// RunConsumerFromEnv runs the consumer with environment variables
func RunConsumerFromEnv() error {
	config := Config{
		BrokerAddress: getEnv("BROKER", "broker0:29092"),
		Topic:         getEnv("TOPIC", "test-topic"),
		ConsumerGroup: getEnv("CONSUMER_GROUP", "http-consumer"),
		TargetURL:     getEnv("TARGET_URL", "http://localhost:6969/webhook-simulator"),
		MaxWorkers:    getEnvInt("WORKERS", 3),
		RateLimit:     getEnvFloat("RATE_LIMIT", 10.0),
		HTTPTimeout:   time.Duration(getEnvInt("HTTP_TIMEOUT", 30)) * time.Second,
		RetryAttempts: getEnvInt("RETRY_ATTEMPTS", 3),
		RetryDelay:    time.Duration(getEnvInt("RETRY_DELAY", 1)) * time.Second,
	}

	return runConsumer(config)
}

func runConsumer(config Config) error {
	// Create consumer
	consumer, err := NewConsumer(config)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	// Start consumer
	if err := consumer.Start(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Consumer started. Press Ctrl+C to stop...")
	<-sigChan

	// Graceful shutdown
	log.Println("Received interrupt signal, shutting down...")
	if err := consumer.Stop(); err != nil {
		return fmt.Errorf("failed to stop consumer: %w", err)
	}

	return nil
}

// Helper functions for environment variables
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseFloat(value, 64); err == nil {
			return parsed
		}
	}
	return defaultValue
}
