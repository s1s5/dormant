# syntax=docker/dockerfile:1
# ---- builder ----
# 常にホストアーキテクチャ（BUILDPLATFORM）でネイティブビルドし、
# Zig を C リンカとして使って amd64 / arm64 両方をクロスコンパイルする（QEMU エミュレーション回避）。
# runtime は debian:bookworm-slim (glibc) のため、ターゲットは glibc 版（*-unknown-linux-gnu）。
# dormant は openssl 非依存（bollard 純 Rust / tungstenite）のため libssl-dev は不要。
FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder
ARG TARGETARCH
ARG BUILDPLATFORM
WORKDIR /app

# クロスコンパイルに必要なツールを導入（zig は公式バイナリで導入）
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config curl xz-utils && \
    rm -rf /var/lib/apt/lists/* && \
    cargo install --locked cargo-zigbuild && \
    rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu && \
    ZIG_ARCH=$(case "$BUILDPLATFORM" in linux/amd64) echo x86_64;; linux/arm64) echo aarch64;; esac) && \
    curl -fsSL "https://ziglang.org/download/0.13.0/zig-linux-${ZIG_ARCH}-0.13.0.tar.xz" -o /tmp/zig.tar.xz && \
    mkdir -p /opt/zig && tar -xJf /tmp/zig.tar.xz -C /opt/zig --strip-components=1 && \
    ln -s /opt/zig/zig /usr/local/bin/zig

# 依存クレートのキャッシュを効かせるため、先に Cargo.toml / Cargo.lock だけコピーして
# 依存クレートのビルドを一度通す（ソース変更時は依存の再コンパイルを避ける）
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    mkdir -p src && echo "fn main() {}" > src/main.rs && \
    cargo zigbuild --release \
        --target x86_64-unknown-linux-gnu \
        --target aarch64-unknown-linux-gnu

# 実ソースをコピーしてビルド（COPY は mtime を保持するため、
# cargo が「変更なし」と誤判定しないよう touch で mtime を更新する）
COPY src ./src
RUN --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    touch src/*.rs && \
    cargo zigbuild --release --bin dormant \
        --target x86_64-unknown-linux-gnu \
        --target aarch64-unknown-linux-gnu && \
    mkdir -p /app/linux && \
    cp target/x86_64-unknown-linux-gnu/release/dormant /app/linux/amd64 && \
    cp target/aarch64-unknown-linux-gnu/release/dormant /app/linux/arm64

# ---- runtime ----
FROM debian:bookworm-slim
ARG TARGETPLATFORM
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
# TARGETPLATFORM に応じて正しいバイナリをコピー（例: linux/arm64 → arm64）
COPY --from=builder /app/linux/${TARGETPLATFORM#linux/} /usr/local/bin/dormant
ENTRYPOINT ["dormant"]
ENV RUST_LOG=info
