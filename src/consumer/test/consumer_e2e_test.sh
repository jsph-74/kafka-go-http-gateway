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
    echo "Usage: $0 <broker_address> <topic_name> <target_url> [duration] [workers] [rate_limit]"
    echo ""
    echo "Parameters:"
    echo "  broker_address: Kafka broker address (e.g., broker0:29092 for container network, localhost:9092 for host)"
    echo "  topic_name:     Kafka topic name to consume from"
    echo "  target_url:     HTTP endpoint to send messages to (e.g., http://go-producer:6969/webhook-simulator)"
    echo "  duration:       How long to run consumer in seconds (default: 60)"
    echo "  workers:        Number of concurrent workers (default: 5)"
    echo "  rate_limit:     Messages per second limit (default: 10.0)"
    echo ""
    echo "Example (container network):"
    echo "  $0 broker0:29092 test-topic http://go-producer:6969/webhook-simulator"
    echo "  $0 broker0:29092 test-topic http://go-producer:6969/webhook-simulator 60 3 5.0"
    echo "Example (host network):"
    echo "  $0 localhost:9092 test-topic http://localhost:6969/webhook-simulator"
    exit 1
}

# Check parameters
if [ $# -lt 3 ]; then
    print_error "Insufficient parameters"
    show_usage
fi

BROKER_ADDRESS="$1"
TOPIC_NAME="$2"
TARGET_URL="$3"
DURATION="${4:-60}"
WORKERS="${5:-3}"
RATE_LIMIT="${6:-25.0}"

# Generate consistent consumer group name that we'll use throughout
CONSUMER_GROUP="e2e-consumer-$(date +%s)"

# Flag to prevent double cleanup
CLEANUP_DONE=false

# Validate numeric parameters
if ! [[ "$DURATION" =~ ^[0-9]+$ ]]; then
    print_error "Duration must be a positive integer"
    exit 1
fi

if ! [[ "$WORKERS" =~ ^[0-9]+$ ]]; then
    print_error "Workers must be a positive integer"
    exit 1
fi

if ! [[ "$RATE_LIMIT" =~ ^[0-9]+\.?[0-9]*$ ]]; then
    print_error "Rate limit must be a positive number"
    exit 1
fi

print_step "Starting Consumer E2E test with parameters:"
echo "  Broker Address:  $BROKER_ADDRESS"
echo "  Topic Name:      $TOPIC_NAME"
echo "  Target URL:      $TARGET_URL"
echo "  Duration:        ${DURATION}s"
echo "  Workers:         $WORKERS"
echo "  Rate Limit:      $RATE_LIMIT msg/sec"
echo "  Consumer Group:  $CONSUMER_GROUP"
echo ""

# Function to cleanup topic and consumer group
cleanup_resources() {
    # Prevent double cleanup
    if [ "$CLEANUP_DONE" = "true" ]; then
        return 0
    fi
    CLEANUP_DONE=true
    
    print_step "Cleaning up resources (restarting consumer to kill any processes)..."
    
    # Restart consumer container to kill any lingering consumer processes
    docker-compose -f ../../docker-compose.yml restart go-consumer >/dev/null 2>&1
    
    # Wait a moment for container to restart and processes to be killed
    sleep 3
    
    print_step "Cleaning up consumer group: $CONSUMER_GROUP"
    # Try to delete the consumer group (should work now that processes are killed)
    OUTPUT=$(docker-compose -f ../../docker-compose.yml exec -T cli-tools kafka-consumer-groups \
        --bootstrap-server "$BROKER_ADDRESS" \
        --delete --group "$CONSUMER_GROUP" 2>&1)
    
    if echo "$OUTPUT" | grep -q "Deletion of some consumer groups failed"; then
        print_warning "Consumer group cleanup skipped (group may still be active - will auto-expire)"
    else
        print_success "Consumer group deleted successfully"
    fi
    
    print_step "Cleaning up topic: $TOPIC_NAME"
    docker-compose -f ../../docker-compose.yml exec -T cli-tools kafka-topics \
        --bootstrap-server "$BROKER_ADDRESS" \
        --delete --topic "$TOPIC_NAME" 2>/dev/null || true
    print_success "Resource cleanup completed"
}

# Trap multiple signals including Ctrl+C (SIGINT) and SIGTERM
trap cleanup_resources EXIT INT TERM

# Check if topic exists
print_step "Checking if topic exists: $TOPIC_NAME"

# First check if we can connect to Kafka at all
if ! docker-compose -f ../../docker-compose.yml exec -T cli-tools kafka-topics \
    --bootstrap-server "$BROKER_ADDRESS" \
    --list > /tmp/kafka_topics_$$ 2> /tmp/kafka_error_$$; then
    
    if grep -q "TimeoutException\|Connection.*refused\|Broker may not be available" /tmp/kafka_error_$$; then
        print_error "Cannot connect to Kafka cluster at $BROKER_ADDRESS"
        print_warning "Check if Kafka cluster is running and accessible"
        print_warning "Try: docker-compose ps (to check broker status)"
    else
        print_error "Failed to list Kafka topics"
        print_warning "Error details:"
        cat /tmp/kafka_error_$$
    fi
    
    rm -f /tmp/kafka_topics_$$ /tmp/kafka_error_$$
    exit 1
fi

# Now check if the specific topic exists
if ! grep -q "^${TOPIC_NAME}$" /tmp/kafka_topics_$$; then
    print_error "Topic '$TOPIC_NAME' does not exist in Kafka cluster"
    print_warning "Available topics:"
    cat /tmp/kafka_topics_$$
    print_warning "Run producer_e2e_test.sh first to create topic and produce messages"
    rm -f /tmp/kafka_topics_$$ /tmp/kafka_error_$$
    exit 1
fi

rm -f /tmp/kafka_topics_$$ /tmp/kafka_error_$$
print_success "Topic '$TOPIC_NAME' exists and Kafka cluster is accessible"

# Run consumer inside consumer container
print_step "Starting consumer inside consumer container for ${DURATION} seconds..."
print_warning "Consumer output:"
echo ""
docker-compose -f ../../docker-compose.yml exec -T go-consumer timeout "$DURATION" go run . \
    -broker "$BROKER_ADDRESS" \
    -topic "$TOPIC_NAME" \
    -target-url "$TARGET_URL" \
    -workers "$WORKERS" \
    -rate "$RATE_LIMIT" \
    -group "$CONSUMER_GROUP" \
    -timeout 10 \
    -retries 2 \
    -retry-delay 1 \
    2>&1 || true

echo ""
print_success "Consumer E2E test completed"

# Show topic status after consumption
print_step "Checking remaining messages in topic..."
REMAINING_MESSAGES=$(docker-compose -f ../../docker-compose.yml exec -T cli-tools kafka-run-class \
    kafka.tools.ConsumerGroupCommand \
    --bootstrap-server "$BROKER_ADDRESS" \
    --group "$CONSUMER_GROUP" \
    --describe 2>/dev/null | tail -n +2 | awk '{sum += $5} END {print sum+0}' || echo "0")

if [ "$REMAINING_MESSAGES" -eq 0 ]; then
    print_success "All messages consumed successfully!"
else
    print_warning "$REMAINING_MESSAGES messages remaining (may indicate processing failures)"
fi

print_success "E2E Consumer test completed successfully!"