#!/bin/bash

docker compose down 
docker compose up -d --build
docker compose logs -f 

# once the gateway konnects to kafka, you can run the tests in enchilada.sh