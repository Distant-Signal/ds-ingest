# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Multi-stage build for movement-relay. The sole real Kafka client against
# RDM's Train Movements product from Deploy B onward -- needs the exact
# same cmake/OpenSSL/libsasl2/libcurl4 builder-stage packages as
# docker/trust-consumer.Dockerfile for rdkafka's `cmake-build`/`ssl`/`sasl`
# features. See that Dockerfile's own header comment for the full
# rationale (unchanged here). This is the crate those packages "belong to"
# from Deploy C onward, once trust-consumer/full-coverage-consumer drop
# rdkafka entirely (see docs/superpowers/plans/2026-09-04-movement-relay-plan.md
# Task 16).
#
# No reference-data/ or lines/ COPY step, unlike trust-consumer/
# full-coverage-consumer: this crate never touches STANOX/CRS translation
# or the line catalogue -- it only classifies envelopes by header.msg_type
# (trust_schema::schema::confirmed_envelope_bodies) and republishes raw
# bytes, nothing else.
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
ARG CARGO_PROFILE

# hadolint ignore=DL3008 # apt versions unpinned on purpose; see .hadolint.yaml
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake libssl-dev pkg-config libsasl2-dev libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "${CARGO_PROFILE}" = "release" ]; then \
      cargo build --release --bin movement-relay; \
    else \
      cargo build --bin movement-relay; \
    fi \
    && cp "/app/target/${CARGO_PROFILE}/movement-relay" /usr/local/bin/movement-relay

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

# `curl` (compose HEALTHCHECK probe of GET /healthz), libssl3, libsasl2-2 --
# see docker/trust-consumer.Dockerfile's own runtime-stage comment for the
# full rationale, unchanged here.
# hadolint ignore=DL3008 # apt versions unpinned on purpose; see .hadolint.yaml
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 libsasl2-2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 movement-relay \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 movement-relay

COPY --from=builder /usr/local/bin/movement-relay /usr/local/bin/movement-relay

# Numeric USER -- see docker/trust-consumer.Dockerfile's own comment for
# why (Kubernetes' runAsNonRoot admission check needs a numeric uid, not a
# name it would have to resolve from /etc/passwd inside the image).
USER 1000:1000

ENTRYPOINT ["/usr/local/bin/movement-relay"]
