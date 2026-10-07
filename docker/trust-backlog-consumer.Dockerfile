# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Multi-stage build for `trust-backlog-consumer`
# (docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md Task 12).
#
# Deliberately NOT structurally identical to docker/trust-consumer.Dockerfile
# or docker/full-coverage-consumer.Dockerfile: this crate has no `rdkafka`
# dependency at all (Task 7's own "Redis-Streams-only, no legacy Kafka
# backend" decision -- confirmed against
# crates/trust-backlog-consumer/Cargo.toml, which carries neither `rdkafka`
# nor any of its cmake/libsasl2/libcurl4 build requirements), so this
# builder stage is a plain Rust build, same shape as docker/aggregator.Dockerfile.
#
# It DOES need both COPY steps the two Kafka-backed consumers split between
# them: `lines/` (like full-coverage-consumer, for the CRS reverse index,
# Task 8) AND `reference-data/` (like trust-consumer, for the
# --stanox-crs-file startup default, Task 7's config.rs).
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "${CARGO_PROFILE}" = "release" ]; then \
      cargo build --release --bin trust-backlog-consumer; \
    else \
      cargo build --bin trust-backlog-consumer; \
    fi \
    && cp "/app/target/${CARGO_PROFILE}/trust-backlog-consumer" /usr/local/bin/trust-backlog-consumer

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

# tini as PID 1 (signal forwarding, zombie reaping): see
# docker/api.Dockerfile's runtime stage for why, and for the version pin.
ARG TINI_VERSION=0.19.0-1

# `curl` (compose HEALTHCHECK probe of GET /healthz), libssl3 for
# reqwest's native-tls feature -- no libsasl2-2 (no rdkafka, see the
# builder stage's own comment).
# hadolint ignore=DL3008 # apt versions unpinned on purpose; see .hadolint.yaml
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 "tini=${TINI_VERSION}*" \
    && rm -rf /var/lib/apt/lists/* \
    && tini --version \
    && groupadd --system --gid 1000 trust-backlog-consumer \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 trust-backlog-consumer

COPY --from=builder /usr/local/bin/trust-backlog-consumer /usr/local/bin/trust-backlog-consumer
COPY --chown=trust-backlog-consumer:trust-backlog-consumer reference-data/ /app/reference-data/
COPY --chown=trust-backlog-consumer:trust-backlog-consumer lines/ /app/lines/

# Numeric USER, not the `trust-backlog-consumer` name useradd created
# above: Kubernetes' runAsNonRoot admission check (this chart's
# podSecurityContext sets runAsNonRoot: true with no explicit runAsUser)
# resolves the image's config purely from its manifest -- it does NOT read
# /etc/passwd inside the image -- so a symbolic USER fails admission with
# "container has runAsNonRoot and image has non-numeric user, cannot
# verify user is non-root". Pinned to the same uid/gid useradd was given
# above so this stays in sync with the group ownership set via COPY
# --chown/groupadd.
USER 1000:1000

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/trust-backlog-consumer"]
