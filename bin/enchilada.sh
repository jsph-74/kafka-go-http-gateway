#!/bin/bash +x

# Run after reboot.sh when the logs show that the gateway is ready
(
    cd src/producer
    test/test.sh 
)
(
    cd src/consumer
    test/test.sh 
)

(
    cd src/producer
    ./test/producer_e2e_test.sh localhost:6969 broker0:29092 test-topic 100
)

(
    cd src/consumer
    # ./test/consumer_e2e_test.sh <broker_address> <topic_name> <target_url> [duration] [workers] [rate_limit]
    ./test/consumer_e2e_test.sh broker0:29092 test-topic http://go-producer:6969/webhook-simulator 60 15 10
)