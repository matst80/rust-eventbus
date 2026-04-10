# Build Stage
FROM rust:1.82-slim-bookworm AS builder

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Docker caching: Copy only dependency files first
COPY Cargo.toml Cargo.lock ./

# Create dummy files for all targets defined in Cargo.toml to allow dependency fetching
RUN mkdir -p src benches examples \
    && echo "pub fn dummy() {}" > src/lib.rs \
    && touch benches/eventbus_bench.rs \
    && touch benches/store_bench.rs \
    && touch benches/embedding_bench.rs \
    && echo "fn main() {}" > examples/web_server.rs

# Fetch dependencies (this layer will be cached)
RUN cargo fetch

# Now copy the actual source code
COPY . .

# Build the web_server example in release mode
RUN cargo build --release --example web_server

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Install chromium, CA certificates, and runtime libraries for ONNX/SSL
RUN apt-get update && apt-get install -y \
    chromium \
    ca-certificates \
    libssl3 \
    libgomp1 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/release/examples/web_server /app/web_server

# Environment defaults
ENV PORT=3000
ENV HOST=0.0.0.0
ENV RUST_LOG=info,chromiumoxide=error
ENV CHROME_BIN=/usr/bin/chromium

# Models and data storage
RUN mkdir -p /app/data/models
VOLUME /app/data

# Expose the API port
EXPOSE 3000

# Run the binary
CMD ["/app/web_server"]
