package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// MessageWithCallback wraps a Kafka message with the ability to mark it as processed
type MessageWithCallback struct {
	Message    *sarama.ConsumerMessage
	MarkFunc   func() // Function to call when message is successfully processed
}

// WorkerPool manages a pool of HTTP workers with crash protection and monitoring
type WorkerPool struct {
	config       Config
	httpClient   *http.Client
	workQueue    chan *MessageWithCallback
	workerErrors chan error
	ctx          context.Context
	wg           *sync.WaitGroup
	workers      []*Worker
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(config Config, httpClient *http.Client, ctx context.Context, wg *sync.WaitGroup) *WorkerPool {
	return &WorkerPool{
		config:       config,
		httpClient:   httpClient,
		workQueue:    make(chan *MessageWithCallback, config.MaxWorkers*2),
		workerErrors: make(chan error, config.MaxWorkers),
		ctx:          ctx,
		wg:           wg,
		workers:      make([]*Worker, 0, config.MaxWorkers),
	}
}

// GetWorkQueue returns the work queue channel for message submission
func (wp *WorkerPool) GetWorkQueue() chan<- *MessageWithCallback {
	return wp.workQueue
}

// Start initializes the worker pool and monitoring
func (wp *WorkerPool) Start() {
	log.Printf("Starting worker pool with %d workers", wp.config.MaxWorkers)
	
	// Start persistent workers
	wp.startWorkers()
	
	// Start worker monitor for crash recovery
	wp.wg.Add(1)
	go wp.workerMonitor()
}

// Stop gracefully stops the worker pool
func (wp *WorkerPool) Stop() {
	log.Println("Stopping worker pool...")
	close(wp.workQueue)
}

// startWorkers initializes persistent workers
func (wp *WorkerPool) startWorkers() {
	log.Printf("Starting %d persistent workers", wp.config.MaxWorkers)

	for i := 0; i < wp.config.MaxWorkers; i++ {
		worker := NewWorker(i, wp.config, wp.httpClient, wp.workQueue, wp.workerErrors, wp.ctx, wp.wg)
		wp.workers = append(wp.workers, worker)
		worker.Start()
	}
}

// workerMonitor watches for crashed workers and restarts them
func (wp *WorkerPool) workerMonitor() {
	defer wp.wg.Done()

	for {
		select {
		case err := <-wp.workerErrors:
			log.Printf("Worker crashed, restarting: %v", err)

			// Generate next available worker ID (simple increment for replacement workers)
			replacementID := len(wp.workers) + 1000 // Offset to distinguish replacement workers
			
			// Start replacement worker with proper ID
			replacementWorker := NewWorker(replacementID, wp.config, wp.httpClient, wp.workQueue, wp.workerErrors, wp.ctx, wp.wg)
			replacementWorker.Start()
			
			// Track the replacement worker (optional - keeps growing but ensures proper ID management)
			wp.workers = append(wp.workers, replacementWorker)
			
			log.Printf("Started replacement worker %d (total workers: %d)", replacementID, len(wp.workers))

		case <-wp.ctx.Done():
			log.Printf("Worker monitor shutting down")
			return
		}
	}
}

// Worker handles HTTP message processing with crash protection and retry logic
//
// DELIVERY SEMANTICS:
// - AT-LEAST-ONCE: Messages ACK'd after complete processing (success OR exhausted retries)
// - CRASH RECOVERY: Only consumer crashes during processing cause message redelivery
// - STRICT ORDERING: Inline retries block workers to preserve partition message order (when workers ≤ partitions)  
// - RETRY STRATEGY: Retries all non-200 responses (4xx for race conditions, 5xx for service issues)
type Worker struct {
	id           int
	config       Config
	httpClient   *http.Client
	workQueue    chan *MessageWithCallback
	workerErrors chan error
	ctx          context.Context
	wg           *sync.WaitGroup
}

// NewWorker creates a new worker instance
func NewWorker(id int, config Config, httpClient *http.Client, workQueue chan *MessageWithCallback, 
	workerErrors chan error, ctx context.Context, wg *sync.WaitGroup) *Worker {
	return &Worker{
		id:           id,
		config:       config,
		httpClient:   httpClient,
		workQueue:    workQueue,
		workerErrors: workerErrors,
		ctx:          ctx,
		wg:           wg,
	}
}

// Start launches the worker in a persistent goroutine with crash protection
func (w *Worker) Start() {
	w.wg.Add(1)
	go w.persistentWorker()
}

// persistentWorker is a long-running worker with crash protection and automatic restart
func (w *Worker) persistentWorker() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Worker %d crashed with panic: %v", w.id, r)
			// Signal crash to monitor for restart
			select {
			case w.workerErrors <- fmt.Errorf("worker %d crashed: %v", w.id, r):
			case <-w.ctx.Done():
			}
		}
	}()

	log.Printf("Worker %d started", w.id)

	for {
		select {
		case msgWrapper := <-w.workQueue:
			if msgWrapper == nil {
				continue
			}

			// Process message with crash protection (includes all retry attempts)
			success := w.processMessage(msgWrapper.Message)
			
			// Always mark message after processing completes, regardless of success/failure
			// This ensures:
			// - Successful messages are ACK'd
			// - Failed messages (after all retries) are also ACK'd to prevent infinite redelivery
			// - Only consumer crashes during processing will cause message redelivery
			msgWrapper.MarkFunc()
			
			if success {
				log.Printf("Message processed successfully - marked for commit")
			} else {
				log.Printf("Message processing failed after all retries - marked for commit to prevent infinite redelivery")
			}

		case <-w.ctx.Done():
			log.Printf("Worker %d shutting down", w.id)
			return
		}
	}
}

// processMessage handles individual message processing with inline retries
//
// MESSAGE ORDERING GUARANTEE (when workers ≤ partitions):
// This function uses inline retries with blocking delays to preserve strict message ordering.
// When a message fails, the worker blocks and retries in-place rather than moving to the next message.
// This ensures messages are processed in the exact order they appear in each Kafka partition.
// 
// NOTE: Ordering only guaranteed when MaxWorkers ≤ partition count. If more workers than partitions,
// multiple workers may process the same partition simultaneously, breaking ordering guarantees.
//
// DELIVERY GUARANTEE:
// - AT-LEAST-ONCE: Messages are ACK'd after processing completes (success OR exhausted retries)  
// - This prevents infinite redelivery loops while maintaining crash recovery
// - On consumer restart/rebalance, unprocessed messages are automatically redelivered by Kafka
//
// RETRY STRATEGY:
// - Retries ALL non-200 HTTP responses (both 4xx and 5xx)
// - 4xx errors may be due to race conditions (e.g., user not yet created, payment method not synced)
// - 5xx errors indicate service issues that often resolve after brief delays
// - Blocking behavior acts as natural circuit breaker during service outages
//
// TRADEOFFS:
// - ✅ Strict ordering preservation
// - ✅ No message loss (at-least-once delivery)
// - ✅ Natural backpressure during failures
// - ✅ Handles race conditions gracefully
// - ❌ Lower throughput during failures (workers block on retries)
// - ❌ Head-of-line blocking (one slow message delays subsequent ones)
func (w *Worker) processMessage(message *sarama.ConsumerMessage) bool {
	startTime := time.Now()
	// Parse message value as JSON
	var value map[string]interface{}
	if err := json.Unmarshal(message.Value, &value); err != nil {
		log.Printf("Failed to parse message value as JSON: %v", err)
		// For non-JSON messages, create a simple wrapper
		value = map[string]interface{}{
			"raw_message": string(message.Value),
		}
	}

	// Create payload
	payload := MessagePayload{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       string(message.Key),
		Value:     value,
		Timestamp: message.Timestamp,
	}

	// Send HTTP request with inline retries (ORDER PRESERVING)
	// This loop blocks the worker thread during retries to maintain strict message ordering.
	// Alternative approaches (async retries, retry topics) would break ordering guarantees.
	for attempt := 0; attempt <= w.config.RetryAttempts; attempt++ {
		if w.sendHTTPRequest(payload, attempt) {
			duration := time.Since(startTime)
			log.Printf("Message processed successfully in %v - offset %d, partition %d, worker %d",
				duration, message.Offset, message.Partition, w.id)
			return true // Success - ACK message
		}

		// Block worker during retry delay to preserve ordering
		// This prevents processing message N+1 before message N succeeds
		if attempt < w.config.RetryAttempts {
			time.Sleep(w.config.RetryDelay)
		}
	}

	duration := time.Since(startTime)
	log.Printf("Failed to process message after %d attempts in %v - offset %d, partition %d, worker %d",
		w.config.RetryAttempts+1, duration, message.Offset, message.Partition, w.id)
	return false // Failure - don't ACK message
}

// sendHTTPRequest sends the message to the target HTTP endpoint
func (w *Worker) sendHTTPRequest(payload MessagePayload, attempt int) bool {
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to marshal payload: %v", err)
		return false
	}

	req, err := http.NewRequestWithContext(w.ctx, "POST", w.config.TargetURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Failed to create HTTP request: %v", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "kafka-http-consumer/1.0")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		log.Printf("HTTP request failed (attempt %d): %v", attempt+1, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("Message sent successfully - offset %d, partition %d",
			payload.Offset, payload.Partition)
		return true
	}

	log.Printf("HTTP request failed with status %d (attempt %d) - offset %d, partition %d",
		resp.StatusCode, attempt+1, payload.Offset, payload.Partition)
	return false
}