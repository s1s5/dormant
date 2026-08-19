# ------------- build ----------------
FROM s1s5/muslrust:1.95.0-stable-2026-04-22 AS builder

RUN mkdir -p /rust && mkdir -p /cargo
WORKDIR /rust

RUN groupadd -g 999 app && \
    useradd -d /app -s /bin/bash -u 999 -g 999 app

COPY Cargo.toml Cargo.lock /rust/
COPY src /rust/src

RUN --mount=type=cache,target=/opt/cargo/registry \
    --mount=type=cache,target=/opt/cargo/git \
    --mount=type=secret,id=env,target=/root/.env \
    --mount=type=cache,target=/var/cache/sccache \
    --mount=type=cache,id=federation-dormant-target,target=/rust/target \
    . /root/.env >/dev/null 2>&1 || true && \
    cargo build --release --bin dormant && \
    cp ./target/x86_64-unknown-linux-musl/release/dormant /dormant


# ------------- server ----------------
FROM busybox AS dormant

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /dormant /dormant
COPY dormant.yml /etc/dormant/dormant.yml

USER 999
ENTRYPOINT ["/dormant"]
CMD ["-c", "/etc/dormant/dormant.yml"]

ENV RUST_LOG=info

# # dormant ビルドステージ
# FROM rust:1-alpine AS builder
# RUN apk add --no-cache musl-dev
# WORKDIR /app
# COPY Cargo.toml Cargo.lock ./
# COPY src ./src
# RUN cargo build --release
# 
# # 実行ステージ
# FROM alpine:3.20
# RUN apk add --no-cache ca-certificates
# COPY --from=builder /app/target/release/dormant /usr/local/bin/dormant
# COPY dormant.yml /etc/dormant/dormant.yml
# EXPOSE 18000
# ENTRYPOINT ["dormant", "-c", "/etc/dormant/dormant.yml"]

