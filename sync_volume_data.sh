#!/bin/bash

# Script to manually copy files from Airflow container's volume to local project directory

# Set your project directory path
PROJECT_DIR="$(pwd)"  # This sets it to your current directory - change if needed

# Get the Airflow scheduler container ID
CONTAINER_NAME=$(docker ps | grep airflow-scheduler | awk '{print $1}')

echo "Starting sync operation..."

# Create local directory if it doesn't exist
mkdir -p "$PROJECT_DIR/survey_outputs_local"

if [ -z "$CONTAINER_NAME" ]; then
  echo "Error: Could not find Airflow scheduler container. Is Airflow running?"
  exit 1
fi

# Copy files from container to local directory
echo "Copying files from container $CONTAINER_NAME to $PROJECT_DIR/survey_outputs_local"
docker cp "$CONTAINER_NAME:/usr/local/airflow/survey_outputs/." "$PROJECT_DIR/survey_outputs_local/"

echo "Files copied successfully!"
echo "Contents of survey_outputs_local:"
ls -la "$PROJECT_DIR/survey_outputs_local"

echo "Sync completed"