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

# Build the web_server example
RUN cargo build --release --example web_server

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Install chromium and CA certificates for HTTPS crawling
RUN apt-get update && apt-get install -y \
    chromium \
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
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/release/examples/web_server /app/web_server

# Environment defaults
ENV PORT=3000
ENV HOST=0.0.0.0
ENV RUST_LOG=info
# chromiumoxide usually finds it, but we can be explicit if needed
ENV CHROME_BIN=/usr/bin/chromium

# Models and data will be stored here
RUN mkdir -p /app/data/models
VOLUME /app/data

# Expose the API port
EXPOSE 3000

# Run the binary
CMD ["/app/web_server"]
