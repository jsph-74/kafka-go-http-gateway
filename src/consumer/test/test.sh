#!/bin/bash

set -e

echo "🚀 Running Consumer Tests in Container..."

# Go to project root
cd ../../

# Start consumer container in detached mode
docker-compose restart go-consumer

# Run unit tests in consumer container
echo "🧪 Running unit tests in consumer container..."
docker-compose exec -T go-consumer go test -v -run "^Test" -short ./test/

# Generate random topic for integration tests
RANDOM_TOPIC="test-consumer-$(openssl rand -hex 6)"
echo "🔧 Creating test topic for consumer integration tests: $RANDOM_TOPIC"

# Create test topic using cli-tools container (on host). 
# Use replication factor 3 to match the 3-broker cluster
docker-compose exec -T cli-tools kafka-topics --bootstrap-server broker0:29092 --create --topic "$RANDOM_TOPIC" --partitions 1 --replication-factor 3

# Cleanup function
cleanup() {
  echo "🧹 Cleaning up consumer test topic: $RANDOM_TOPIC"
  docker-compose exec -T cli-tools kafka-topics --bootstrap-server broker0:29092 --delete --topic "$RANDOM_TOPIC" || true
}
trap cleanup EXIT

# Run integration tests in consumer container with the test topic
echo "🔌 Running integration tests in consumer container..."
docker-compose exec -T -e BROKER=broker0:29092 -e TEST_TOPIC="$RANDOM_TOPIC" go-consumer go test -v ./test/
echo "🎉 Consumer tests completed successfully!"