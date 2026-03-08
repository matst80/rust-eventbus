#!/bin/bash

# Exit on any error
set -e

IMAGE_NAME="matst80/rust-eventbus-todo:latest"

echo "🔨 Building Docker image: $IMAGE_NAME..."
docker build -t $IMAGE_NAME .

echo "🚀 Pushing Docker image: $IMAGE_NAME..."
docker push $IMAGE_NAME

echo "🔄 Restarting Kubernetes deployment: todo-app in namespace dev..."
kubectl rollout restart deployment/todo-app -n dev

echo "✅ Done!"

