#!/bin/bash

# Check if exactly 2 parameters are provided
if [ "$#" -ne 2 ]; then
  echo "Error: Two parameters are required."
  echo "Usage: $0 topicName groupId"
  exit 1
fi

docker exec -it broker kafka-console-consumer --bootstrap-server localhost:9092 --topic $1 --group $2
