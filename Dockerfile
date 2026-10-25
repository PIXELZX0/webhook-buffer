# syntax=docker/dockerfile:1

# 모든 아키텍처를 빌드 머신에서 교차 컴파일한다.
# 아키텍처마다 QEMU로 컴파일하는 것보다 훨씬 빠르고, C 코드(SQLite, aws-lc)는 zig가 컴파일한다.
ARG TARGETARCH

FROM --platform=$BUILDPLATFORM rust:1.97-bookworm AS toolchain
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir --break-system-packages ziglang==0.17.0 \
 && cargo install --locked cargo-zigbuild@0.24.0 \
 && mkdir /data

FROM toolchain AS build
ARG TARGETPLATFORM
# musl로 정적 링크해서 어떤 베이스 이미지에서도 돌게 한다.
# s390x는 musl용 Rust 표준 라이브러리가 배포되지 않아서 glibc로 빌드한다.
RUN case "$TARGETPLATFORM" in \
      linux/amd64)   echo x86_64-unknown-linux-musl ;; \
      linux/arm64)   echo aarch64-unknown-linux-musl ;; \
      linux/arm/v7)  echo armv7-unknown-linux-musleabihf ;; \
      linux/arm/v6)  echo arm-unknown-linux-musleabihf ;; \
      linux/386)     echo i686-unknown-linux-musl ;; \
      linux/ppc64le) echo powerpc64le-unknown-linux-musl ;; \
      linux/riscv64) echo riscv64gc-unknown-linux-musl ;; \
      linux/loong64) echo loongarch64-unknown-linux-musl ;; \
      linux/s390x)   echo s390x-unknown-linux-gnu.2.28 ;; \
      *) echo "지원하지 않는 플랫폼: $TARGETPLATFORM" >&2; exit 1 ;; \
    esac > /zig-target \
 && cut -d. -f1 /zig-target > /rust-target \
 && rustup target add "$(cat /rust-target)"
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target,id=target-$TARGETPLATFORM \
    cargo zigbuild --release --locked --target "$(cat /zig-target)" \
 && cp "target/$(cat /rust-target)/release/webhook-buffer" /webhook-buffer

# 정적 바이너리라 아무것도 없는 scratch로 충분하다. s390x만 glibc가 들어 있는 베이스를 쓴다.
FROM scratch AS base-amd64
FROM scratch AS base-arm64
FROM scratch AS base-arm
FROM scratch AS base-386
FROM scratch AS base-ppc64le
FROM scratch AS base-riscv64
FROM scratch AS base-loong64
FROM gcr.io/distroless/cc-debian12 AS base-s390x

FROM base-${TARGETARCH}
COPY --from=toolchain /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=toolchain --chown=65532:65532 /data /data
COPY --from=build /webhook-buffer /usr/local/bin/webhook-buffer
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    LISTEN_ADDR=0.0.0.0:8080 \
    DB_PATH=/data/webhook-buffer.db
USER 65532:65532
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/webhook-buffer"]
