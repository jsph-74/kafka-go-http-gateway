package producer

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// HealthHandler returns a health check endpoint
func HealthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

// WebhookSimulator simulates a realistic webhook endpoint for E2E testing
func WebhookSimulator(c *gin.Context) {
	// Parse the incoming message
	var payload map[string]interface{}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid JSON payload",
			"details": err.Error(),
		})
		return
	}

	// Extract message info for logging
	topic := "unknown"
	offset := "unknown"
	if topicVal, ok := payload["topic"]; ok {
		topic = fmt.Sprintf("%v", topicVal)
	}
	if offsetVal, ok := payload["offset"]; ok {
		offset = fmt.Sprintf("%v", offsetVal)
	}

	// Simulate realistic processing time (50ms to 2000ms)
	processingTime := time.Duration(50+rand.Intn(1950)) * time.Millisecond
	time.Sleep(processingTime)

	// Simulate realistic error rates
	randomFactor := rand.Float64()

	switch {
	case randomFactor < 0.05: // 5% - Server errors
		log.Printf("🔴 Webhook FAILED (500): topic=%s, offset=%s, processing=%v",
			topic, offset, processingTime)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Internal server error",
			"message": "Simulated downstream service failure",
		})

	case randomFactor < 0.10: // 5% - Bad request errors
		log.Printf("🟡 Webhook FAILED (400): topic=%s, offset=%s, processing=%v",
			topic, offset, processingTime)
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Bad request",
			"message": "Simulated validation failure",
		})

	case randomFactor < 0.12: // 2% - Service unavailable
		log.Printf("🟠 Webhook FAILED (503): topic=%s, offset=%s, processing=%v",
			topic, offset, processingTime)
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "Service temporarily unavailable",
			"message": "Simulated overload condition",
		})

	default: // 88% - Success responses
		log.Printf("✅ Webhook SUCCESS: topic=%s, offset=%s, processing=%v",
			topic, offset, processingTime)
		c.JSON(http.StatusOK, gin.H{
			"status":             "processed",
			"message":            "Message processed successfully",
			"processing_time_ms": processingTime.Milliseconds(),
			"received_at":        time.Now().UTC().Format(time.RFC3339),
		})
	}
}