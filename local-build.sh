#!/bin/bash

# Exit on any error
set -e

IMAGE_NAME="matst80/rust-eventbus-todo:latest"

echo "🔨 Building Docker image: $IMAGE_NAME..."
docker build -t $IMAGE_NAME .

echo "🚀 Pushing Docker image: $IMAGE_NAME..."
docker push $IMAGE_NAME

echo "✅ Done!"

