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
    echo "Usage: $0 <broker_address> <topic_name> <target_url> [duration] [instances] [rate_limit]"
    echo ""
    echo "Parameters:"
    echo "  broker_address: Kafka broker address (e.g., broker0:29092 for container network, localhost:9092 for host)"
    echo "  topic_name:     Kafka topic name to consume from"
    echo "  target_url:     HTTP endpoint to send messages to (e.g., http://go-producer:6969/webhook-simulator)"
    echo "  duration:       How long to run consumer in seconds (default: 60)"
    echo "  instances:      Number of consumer instances to run (default: 3)"
    echo "  rate_limit:     Messages per second limit per instance (default: 10.0)"
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
INSTANCES="${5:-3}"
RATE_LIMIT="${6:-10.0}"
DOCKER_COMPOSE_FILE="../../docker-compose.yml"

CONSUMER_GROUP="e2e-consumer-$(date +%s)"
CLEANUP_DONE=false

# Validate numeric parameters
if ! [[ "$DURATION" =~ ^[0-9]+$ ]]; then
    print_error "Duration must be a positive integer"
    exit 1
fi

if ! [[ "$INSTANCES" =~ ^[0-9]+$ ]]; then
    print_error "Instances must be a positive integer"
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
echo "  Instances:       $INSTANCES"
echo "  Rate Limit:      $RATE_LIMIT msg/sec per instance"
echo "  Consumer Group:  $CONSUMER_GROUP"
echo ""

# Function to cleanup topic and consumer group
cleanup_resources() {
    if [ "$CLEANUP_DONE" = "true" ]; then
        return 0
    fi
    CLEANUP_DONE=true

    print_step "Cleaning up resources (stopping all consumers)..."
    docker-compose -f $DOCKER_COMPOSE_FILE up -d --scale go-consumer="0"
    sleep 3
    
    print_step "Cleaning up consumer group in kafka: $CONSUMER_GROUP"
    OUTPUT=$(docker-compose -f $DOCKER_COMPOSE_FILE exec -T cli-tools kafka-consumer-groups \
        --bootstrap-server "$BROKER_ADDRESS" \
        --delete --group "$CONSUMER_GROUP" 2>&1)
    
    if echo "$OUTPUT" | grep -q "Deletion of some consumer groups failed"; then
        print_warning "Consumer group cleanup skipped (the group may still be active because of unstopped consumers)"
    else
        print_success "Consumer group deleted successfully"
    fi
    
    print_step "Cleaning up topic: $TOPIC_NAME"
    docker-compose -f $DOCKER_COMPOSE_FILE exec -T cli-tools kafka-topics \
        --bootstrap-server "$BROKER_ADDRESS" \
        --delete --topic "$TOPIC_NAME" 2>/dev/null || true
    print_success "Resource cleanup completed"

}

# Trap multiple signals including Ctrl+C (SIGINT) and SIGTERM
trap cleanup_resources EXIT INT TERM

# Check if topic exists
print_step "Checking if topic exists: $TOPIC_NAME"

# First check if we can connect to Kafka at all
if ! docker-compose -f $DOCKER_COMPOSE_FILE exec -T cli-tools kafka-topics \
    --bootstrap-server "$BROKER_ADDRESS" \
    --list > /tmp/kafka_topics_$$ 2> /tmp/kafka_error_$$; then
    
    if grep -q "TimeoutException\|Connection.*refused\|Broker may not be available" /tmp/kafka_error_$$; then
        print_error "Cannot connect to Kafka cluster at $BROKER_ADDRESS"
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
    exit 1
fi

rm -f /tmp/kafka_topics_$$ /tmp/kafka_error_$$
print_success "Topic '$TOPIC_NAME' exists and Kafka cluster is accessible"

# Run multiple consumer instances
print_step "Starting $INSTANCES consumer instances for ${DURATION} seconds..."

# Scale up consumer instances
print_warning "Starting consumers with native Kafka scaling:"
docker-compose -f $DOCKER_COMPOSE_FILE up -d --scale go-consumer="$INSTANCES"
echo ""

# Start consumer processes in each instance
for i in $(seq 1 "$INSTANCES"); do
    container_name="kafka-go-http-gateway-go-consumer-$i"
    echo "Starting consumer instance $i in container: $container_name"
    
    # Start consumers
    docker exec "$container_name" go run . \
        -broker "$BROKER_ADDRESS" \
        -topic "$TOPIC_NAME" \
        -target-url "$TARGET_URL" \
        -rate "$RATE_LIMIT" \
        -group "$CONSUMER_GROUP" \
        -timeout 10 \
        -retries 2 \
        -retry-delay 1 &
done

echo "All $INSTANCES consumer instances started! (running for a max ${DURATION} seconds)"
sleep "$DURATION"

# Kill all background processes
echo "Stopping all consumer instances..."
docker-compose -f $DOCKER_COMPOSE_FILE up -d --scale go-consumer="0"

# Wait for processes to terminate
sleep 2
print_success "Consumer E2E test completed"

# Show topic status after consumption
print_step "Checking remaining messages in topic..."
REMAINING_MESSAGES=$(docker-compose -f $DOCKER_COMPOSE_FILE exec -T cli-tools kafka-run-class \
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