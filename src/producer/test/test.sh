#!/bin/bash

set -e

echo "🚀 Running Producer Tests in Container..."

# Go to project root
cd ../../

BROKER="broker0:29092"

# Run unit tests in producer container
echo "🧪 Running unit tests in producer container..."
docker-compose exec -T go-producer \
  go test -v -run "^Test" -short ./test/

# Generate random topic for integration tests
RANDOM_TOPIC="test-$(openssl rand -hex 6)"
echo "🔧 Creating test topic: $RANDOM_TOPIC"

# Create test topic using cli-tools container (on host) 
# Use replication factor 3 to match the 3-broker cluster
docker-compose exec -T cli-tools \
  kafka-topics --bootstrap-server $BROKER \
  --create --topic "$RANDOM_TOPIC" --partitions 1 --replication-factor 3

# Cleanup function
cleanup() {
  echo "🧹 Cleaning up test topic: $RANDOM_TOPIC"
  docker-compose exec -T cli-tools \
    kafka-topics --bootstrap-server $BROKER \
    --delete --topic "$RANDOM_TOPIC" || true
}
trap cleanup EXIT

# Run integration tests in producer container with the test topic
echo "🧪 Running integration tests in producer container..."
docker-compose exec -T -e BROKER=$BROKER -e TEST_TOPIC="$RANDOM_TOPIC" go-producer \
  go test -v -tags integration ./test/

echo "🎉 Producer tests completed successfully!"