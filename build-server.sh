#!/bin/bash

# Exit on any error
set -e

IMAGE_NAME="matst80/rust-eventbus:latest"

echo "🔨 Building Docker image: $IMAGE_NAME..."
docker build -t $IMAGE_NAME .

echo "🚀 Pushing Docker image: $IMAGE_NAME..."
docker push $IMAGE_NAME

echo "🔄 Restarting Kubernetes deployment: crawler-web in namespace iamu..."
kubectl rollout restart deployment/crawler-web -n iamu

echo "✅ Done!"

