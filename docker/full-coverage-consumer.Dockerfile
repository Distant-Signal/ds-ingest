# syntax=docker/dockerfile:1
# Multi-stage build for full-coverage-consumer. Structurally identical to
# docker/trust-consumer.Dockerfile -- a second, independent Kafka consumer
# against the same RDM Train Movements feed, so it needs the exact same
# cmake/OpenSSL/libsasl2/libcurl4 builder-stage packages for rdkafka's
# `cmake-build`/`ssl`/`sasl` features. See that Dockerfile's own header
# comment for the full rationale (unchanged here).
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
      cargo build --release --bin full-coverage-consumer; \
    else \
      cargo build --bin full-coverage-consumer; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/full-coverage-consumer /usr/local/bin/full-coverage-consumer

FROM debian:bookworm-slim

# `curl` (compose HEALTHCHECK probe of GET /healthz), libssl3, libsasl2-2 --
# see docker/trust-consumer.Dockerfile's own runtime-stage comment for the
# full rationale, unchanged here.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 libsasl2-2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 full-coverage-consumer \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 full-coverage-consumer

COPY --from=builder /usr/local/bin/full-coverage-consumer /usr/local/bin/full-coverage-consumer
# No reference-data COPY step (unlike trust-consumer): this crate's own
# STANOX/CRS table is always live-reloaded via queries::fetch_stanox_crs,
# never a --stanox-crs-file startup default (confirmed against Task 9's
# final config.rs -- it has no such flag at all).
#
# The static line catalogue IS baked in, though -- same
# --lines-dir/LINES_DIR pattern as aggregator/api/schedule-reference (see
# those Dockerfiles' own identical COPY step); this crate needs it to
# build Decision 2c's reverse tiploc->line index.
COPY --chown=full-coverage-consumer:full-coverage-consumer lines/ /app/lines/

# Numeric USER -- see docker/trust-consumer.Dockerfile's own comment for
# why (Kubernetes' runAsNonRoot admission check needs a numeric uid, not a
# name it would have to resolve from /etc/passwd inside the image).
USER 1000:1000

ENTRYPOINT ["/usr/local/bin/full-coverage-consumer"]
