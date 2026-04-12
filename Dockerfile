# Stage 1: Build dependencies and application
FROM rustlang/rust:nightly-trixie-slim AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copy source and build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
COPY benches ./benches
RUN cargo build --release --example simplified_web_server

# Stage 2: Runtime
FROM debian:trixie-slim

WORKDIR /app

# Install chromium, CA certificates, and runtime libraries for ONNX/SSL
RUN apt-get update && apt-get install -y \
    chromium \
    ca-certificates \
    libssl3t64 \
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
COPY --from=builder /usr/src/app/target/release/examples/simplified_web_server /app/simplified_web_server

# Environment defaults
ENV PORT=3000
ENV HOST=0.0.0.0
ENV RUST_LOG=info,chromiumoxide=error
ENV CHROME_BIN=/usr/bin/chromium
ENV HOME=/tmp
ENV XDG_CONFIG_HOME=/tmp/.config
ENV XDG_CACHE_HOME=/tmp/.cache

# Create a non-root user to run the application
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --create-home --home-dir /home/appuser appuser

# Models and data storage
RUN mkdir -p /app/data/models /tmp/.config /tmp/.cache /tmp/chrome-profile && \
    chown -R appuser:appgroup /app/data /home/appuser /tmp/.config /tmp/.cache /tmp/chrome-profile
VOLUME /app/data

USER appuser

# Expose the API port
EXPOSE 3000

# Run the binary
CMD ["/app/simplified_web_server"]
