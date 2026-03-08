# Deploying Todo App to Kubernetes

This guide explains how to containerize and deploy the `todo_app` example.

## Build the Docker Image

```bash
docker build -t rust-eventbus-todo:latest .
```

## Running Locally with Docker

```bash
docker run -p 3000:3000 \
  -e PORT=3000 \
  -e DATA_DIR=/app/data \
  -v $(pwd)/data:/app/data \
  rust-eventbus-todo:latest
```

## Deploying to Kubernetes

1. Build and push the image to your registry.
2. Update the image name in `kubernetes/deployment.yaml`.
3. Apply the manifests:

```bash
kubectl apply -f kubernetes/deployment.yaml
```

## Configuration

The application handles the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | The port the API server listens on | `3000` |
| `HOST` | The host interface to bind to | `0.0.0.0` |
| `MESH_BIND_HOST` | Host/interface to bind the mesh listener | `HOST` |
| `MESH_ADVERTISE_HOST` | Address advertised for mesh self-filtering | `POD_IP`/`MESH_BIND_HOST` |
| `DATA_DIR` | Directory for event and snapshot storage | `./data` |
| `NODE_ID` | Unique identifier for the node (UUID) | Auto-generated |

> [!NOTE]
> In this example, "distributed" communication between pods is simulated and requires a shared event store (e.g., a shared PVC or a future Redis-based `DistributedPubSub` implementation).
