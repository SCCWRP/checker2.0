#!/bin/bash

# Check if the container name is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <container_name>"
    # exit 1
    echo "using default container name for this repo: bight23-checker"
    container_name='bight23-checker'
else 
    container_name=$1
fi

container_id=$(docker ps -a --filter "name=$container_name" --format "{{.ID}}")

# Check if the container exists
if [ -z "$container_id" ]; then
    echo "Error: No container found with name '$container_name'"
    exit 2
fi

# Get the log file path
log_path=$(docker inspect --format='{{.LogPath}}' "$container_id")

# Check if the log file exists
if [ -z "$log_path" ]; then
    echo "Error: No log file found for container '$container_name'"
    exit 3
fi

# Clear the logs
sudo truncate -s 0 "$log_path"

echo "Successfully cleared logs for container '$container_name'"
