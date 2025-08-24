#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
    echo -e "${BLUE}🔧 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 <producer_server> <broker_address> <topic_name> <message_count>"
    echo ""
    echo "Parameters:"
    echo "  producer_server: HTTP server address (e.g., go-producer:6969 for container network, localhost:6969 for host)"
    echo "  broker_address:  Kafka broker address (e.g., broker0:29092 for container network, localhost:9092 for host)"
    echo "  topic_name:      Kafka topic name"
    echo "  message_count:   Number of messages to send (integer)"
    echo ""
    echo "Example (container network):"
    echo "  $0 go-producer:6969 broker0:29092 test-topic 10"
    echo "Example (host network):"
    echo "  $0 localhost:6969 localhost:9092 test-topic 10"
    exit 1
}

# Check parameters
if [ $# -ne 4 ]; then
    print_error "Incorrect number of parameters"
    show_usage
fi

PRODUCER_SERVER="$1"
BROKER_ADDRESS="$2"
TOPIC_NAME="$3"
MESSAGE_COUNT="$4"

# Validate message count is a number
if ! [[ "$MESSAGE_COUNT" =~ ^[0-9]+$ ]]; then
    print_error "Message count must be a positive integer"
    exit 1
fi

print_step "Starting E2E test with parameters:"
echo "  Producer Server: $PRODUCER_SERVER"
echo "  Broker Address:  $BROKER_ADDRESS"
echo "  Topic Name:      $TOPIC_NAME"
echo "  Message Count:   $MESSAGE_COUNT"
echo ""

# Note: Topic cleanup is now handled by consumer_e2e_test.sh

# Step 1: Create topic
print_step "Creating Kafka topic: $TOPIC_NAME"
docker-compose -f ../../docker-compose.yml exec -T cli-tools kafka-topics \
    --bootstrap-server "$BROKER_ADDRESS" \
    --create --topic "$TOPIC_NAME" \
    --partitions 15 --replication-factor 2 \
    --if-not-exists

if [ $? -eq 0 ]; then
    print_success "Topic created successfully"
else
    print_error "Failed to create topic"
    exit 1
fi

# Step 2: Send messages via HTTP producer from host
print_step "Sending $MESSAGE_COUNT messages to producer server from host"

for i in $(seq 1 $MESSAGE_COUNT); do
    # Generate random content
    RANDOM_CONTENT="message_${i}_$(date +%s)_${RANDOM}"
    MESSAGE_KEY="key_$i"
    
    # Send message via curl from host
    RESPONSE=$(curl -s -X POST "http://$PRODUCER_SERVER/produce" \
        -H "Content-Type: application/json" \
        -d "{
            \"topic\": \"$TOPIC_NAME\",
            \"key\": \"$MESSAGE_KEY\",
            \"payload\": {
                \"text\": \"$RANDOM_CONTENT\",
                \"message_number\": $i,
                \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
            }
        }")
    
    # Check if request was successful
    if echo "$RESPONSE" | grep -q '"success":true'; then
        echo "  Message $i sent successfully"
    else
        print_error "Failed to send message $i"
        echo "  Response: $RESPONSE"
        exit 1
    fi
done

print_success "All $MESSAGE_COUNT messages sent successfully"

print_step "Messages are ready in Kafka topic: $TOPIC_NAME"
print_success "Producer E2E test completed successfully"
print_warning "Run consumer_e2e_test.sh next to consume these messages via HTTP consumer"
echo ""
echo "Recommended command:"
echo "cd ../../consumer && ./test/consumer_e2e_test.sh $BROKER_ADDRESS $TOPIC_NAME http://go-producer:6969/webhook-simulator"