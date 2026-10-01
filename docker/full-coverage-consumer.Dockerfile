# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Multi-stage build for full-coverage-consumer. A plain Rust build, same
# shape as docker/trust-consumer.Dockerfile: the rdkafka dependency went
# with the legacy Kafka backend in Deploy C (PL-15a).
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "${CARGO_PROFILE}" = "release" ]; then \
      cargo build --release --bin full-coverage-consumer; \
    else \
      cargo build --bin full-coverage-consumer; \
    fi \
    && cp "/app/target/${CARGO_PROFILE}/full-coverage-consumer" /usr/local/bin/full-coverage-consumer

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

# `curl` (compose HEALTHCHECK probe of GET /healthz) and libssl3 -- see
# docker/trust-consumer.Dockerfile's own runtime-stage comment.
# hadolint ignore=DL3008 # apt versions unpinned on purpose; see .hadolint.yaml
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 \
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
