# Kafka HTTP Gateway

HTTP-to-Kafka bidirectional gateway with Go and Sarama

## 🏗️ Architecture

```
┌─────────────────┐
│   HTTP Client   │
└─────────┬───────┘
          │ POST http://producer:port/produce
          ▼
┌─────────────────┐             
│   Go Gateway    │             ┌─────────────┐
│ ┌─────────────┐ │    push     │             │
│ │ go-producer │ │────────────►│             │
│ │ (http API)  │ │             │    Kafka    │
│ └─────────────┘ │             │   Cluster   │
│ ┌─────────────┐ │    poll     │             │
│ │ go-consumer │ │◄────────────┘             │
│ │ (CLI Tool)  │ │             └─────────────┘
│ └─────────────┘ │
└─────────┬───────┘
          │ POST http://externalhttpservice/url
          ▼
┌─────────────────┐
│   HTTP Server   │
│  target server  │
└─────────────────┘
```

**Components:**
- **Kafka Producer** (`src/producer/`) - Gin web service with `/produce`, `/health`, `/webhook-simulator` (for testing purposes)
- **Kafka Consumer** (`src/consumer/`) - Standalone CLI that forwards Kafka messages to HTTP endpoints  
  - *Delivery Guarantee*: **AT-LEAST-ONCE** (messages ACK'd only after complete processing)
  - *Retry Strategy*: Immediate retries (1-3 attempts) within same session for fast failure recovery
  - *Crash Recovery*: Messages redelivered only if consumer crashes during processing and before ack
  - *Processing Flow*: Message → Worker retries → ACK (regardless of final success/failure)
  - *Trade-off*: Excellent immediate retry + crash safety, but crash during retries restarts retry counter
- **Kafka Cluster** - Test 3-broker fault-tolerant setup with Zookeeper

## 📁 Project Structure

```
kafka-go-http-gateway/
├── bin/
│   ├── reboot.sh                      # Infrastructure reset
│   └── enchilada.sh                   # Complete E2E test pipeline
├── docker-compose.yml                 # Kafka cluster + Go services
├── producer.Dockerfile                # Producer service container
├── consumer.Dockerfile                # Consumer service container
├── CLAUDE.md                          # Development guidelines for Claude Code
└── src/
    ├── producer/                      # HTTP-to-Kafka producer service
    │   ├── main.go, pkg/              # Gin web API implementation
    │   └── test/                      # Container-based unit/integration/E2E tests
    │       ├── test.sh                # Run all tests in containers
    │       ├── producer_e2e_test.sh   # End-to-end HTTP → Kafka pipeline
    │       └── *_test.go              # Unit & integration tests
    └── consumer/                      # Kafka-to-HTTP consumer CLI
        ├── main.go, pkg/              # Consumer & worker implementation
        └── test/                      # Container-based test suite
            ├── test.sh                # Run all tests in containers
            ├── consumer_e2e_test.sh   # End-to-end Kafka → HTTP pipeline
            └── *_test.go              # Unit & integration tests
```

## 🚀 Quick Start

**Prerequisites:** Docker and Docker Compose

### 1. Bootstrap Infrastructure
```bash
# Clean rebuild: down → build → up → logs
# Wait until the go-producer has connected to kafka, indicating Kafka is ready
./bin/reboot.sh 
```

### 2. Run Complete Test Suite  
```bash
# Full E2E pipeline: Producer → Kafka → Consumer
./bin/enchilada.sh 
```

### 3. Individual Testing (Optional)

**Producer Tests** (from `src/producer/`):
```bash
# Unit & integration tests
./test/test.sh 

# E2E HTTP → Kafka
./test/producer_e2e_test.sh localhost:6969 broker0:29092 test-topic 5   
```

**Consumer Tests** (from `src/consumer/`):  
```bash
# Unit & integration tests  
./test/test.sh 

# E2EKafka → HTTP  
./test/consumer_e2e_test.sh broker0:29092 test-topic http://go-producer:6969/webhook-simulator 30 3 2.0
```

**Manual E2E Pipeline:**
```bash
# Step 1: Send 10 messages to Kafka
cd src/producer
./test/producer_e2e_test.sh localhost:6969 broker0:29092 test-topic 10

# Step 2: Consume and forward to HTTP
cd ../consumer  
./test/consumer_e2e_test.sh broker0:29092 test-topic http://go-producer:6969/webhook-simulator 30 3 5.0
```

## 🌐 API Endpoints

**Kafka Producer API:**
```bash
POST http://localhost:6969/produce
{
  "topic": "my-topic", 
  "key": "optional-key",
  "payload": {"message": "Hello Kafka!"}
}

GET http://localhost:6969/health

# Test consumer url, simulating realistic errors and delays
POST http://localhost:6969/webhook-simulator  
```

**Kafka Consumer CLI:**
```bash
docker-compose exec go-consumer go run . \
  -broker broker0:29092 \
  -topic my-topic \  
  -target-url http://api.example.com/webhook \
  -workers 5 -rate 10.0
```

Please note, the webhook simulates failures and delays

## 📊 Configuration

**Kafka:** 3 brokers (localhost:9092-9094), replication factor 3  
**Kafka Producer:** Port 6969, Gin web service  
**Kafka Consumer:** Standalone CLI, rate limiting, retries, concurrent workers

## 👥 Credits

**Project by:** jsph  
**Engineering Support:** Claude (Anthropic AI Assistant)

*Built with Go, Kafka, Docker, and a focus on reliable message delivery patterns.*