# syntax=docker/dockerfile:1
# Multi-stage build for trust-consumer. Needs cmake + OpenSSL headers in
# the builder stage for rdkafka's `cmake-build`/`ssl` features (librdkafka
# is a C library rdkafka vendors and compiles from source) -- every other
# Rust service in this repo is a pure-Rust dependency tree and doesn't need
# this.
#
# libsasl2-dev: rdkafka's `sasl` feature pulls in the `sasl2-sys` crate,
# whose build script requires a system-installed libsasl2 (headers +
# pkg-config) or the build panics with "Unable to find libsasl2 on your
# system."
#
# libcurl4-openssl-dev: librdkafka's vendored C source
# (rdkafka_conf.c) does an unconditional `#include <curl/curl.h>`
# regardless of any cmake feature flag, so the build fails without curl dev
# headers present -- even though the resulting binary does NOT dynamically
# link libcurl at runtime (verified via `ldd` against a locally compiled
# binary: no libcurl in the linkage, only libsasl2.so.3 and libssl.so.4).
# Headers-only requirement; no matching runtime package is installed below.
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm AS builder
ARG CARGO_PROFILE

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake libssl-dev pkg-config libsasl2-dev libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin trust-consumer; \
    else \
      cargo build --bin trust-consumer; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/trust-consumer /usr/local/bin/trust-consumer

FROM debian:bookworm-slim

# `curl` is added on top of the poller Dockerfiles' pattern solely so
# docker-compose's HEALTHCHECK can probe `GET /healthz` from inside the
# container -- same reasoning as docker/api.Dockerfile's runtime stage. It
# is unrelated to rdkafka: libcurl is a build-time-only (headers) need for
# librdkafka's vendored C source, not a runtime link (see the builder
# stage's comment above), so no libcurl runtime package is installed here.
#
# libsasl2-2 is the runtime counterpart of the builder stage's
# libsasl2-dev: rdkafka's `sasl` feature dynamically links libsasl2.so.3
# at runtime, confirmed via `ldd` against a locally compiled binary.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 libsasl2-2 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin trust-consumer

COPY --from=builder /usr/local/bin/trust-consumer /usr/local/bin/trust-consumer
COPY --chown=trust-consumer:trust-consumer reference-data/ /app/reference-data/

USER trust-consumer

ENTRYPOINT ["/usr/local/bin/trust-consumer"]
