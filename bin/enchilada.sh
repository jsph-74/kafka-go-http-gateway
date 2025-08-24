#!/bin/bash +x


TOPIC="test-topic-$(date +%s)"


# Run after reboot.sh when the logs show that the gateway is ready
echo ""
echo ""
echo "Running producer go tests"
echo "-------------------------"
echo ""
(
    cd src/producer
    test/test.sh 
)

echo ""
echo ""
echo "Running consumer go tests"
echo "-------------------------"
echo ""
(
    cd src/consumer
    test/test.sh 
)

echo ""
echo ""
echo "Running producer end 2 end tests"
echo "--------------------------------"
echo ""

(
    cd src/producer
    ./test/producer_e2e_test.sh localhost:6969 broker0:29092 $TOPIC 100
)

echo ""
echo ""
echo "Running consumer end 2 end tests"
echo "--------------------------------"
echo ""
(
    cd src/consumer
    # ./test/consumer_e2e_test.sh <broker_address> <topic_name> <target_url> [duration] [instances] [rate_limit]
    ./test/consumer_e2e_test.sh broker0:29092 $TOPIC http://go-producer:6969/webhook-simulator 60 15 10.0
)