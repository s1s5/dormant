# ------------- build ----------------
FROM rust:1-slim AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin dormant && \
    cp ./target/release/dormant /dormant


# ------------- server ----------------
FROM debian:bookworm-slim AS dormant

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /dormant /dormant

ENTRYPOINT ["/dormant"]

ENV RUST_LOG=info
