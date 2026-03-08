# Build Stage
FROM rust:latest AS builder

WORKDIR /usr/src/app

# Install dependencies for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire workspace
COPY . .

# Build the example
RUN cargo build --release --example todo_app

# Runtime Stage - Distroless for security and size
FROM gcr.io/distroless/cc-debian12

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/release/examples/todo_app /app/todo_app

# Environment defaults
ENV PORT=3000
ENV HOST=0.0.0.0
ENV DATA_DIR=/app/data
ENV RUST_LOG=info

# Expose the API port
EXPOSE 3000

# Run the binary
# Note: Data directory should be mounted as a volume in K8s
CMD ["/app/todo_app"]
