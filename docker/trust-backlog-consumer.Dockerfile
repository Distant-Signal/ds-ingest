# syntax=docker/dockerfile:1
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

FROM rust:1.88-bookworm AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release --bin trust-backlog-consumer; \
    else \
      cargo build --bin trust-backlog-consumer; \
    fi \
    && cp /app/target/${CARGO_PROFILE}/trust-backlog-consumer /usr/local/bin/trust-backlog-consumer

FROM debian:bookworm-slim

# `curl` (compose HEALTHCHECK probe of GET /healthz), libssl3 for
# reqwest's native-tls feature -- no libsasl2-2 (no rdkafka, see the
# builder stage's own comment).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 \
    && rm -rf /var/lib/apt/lists/* \
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

ENTRYPOINT ["/usr/local/bin/trust-backlog-consumer"]
