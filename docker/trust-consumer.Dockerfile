# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Multi-stage build for trust-consumer. A plain Rust build, same shape as
# docker/trust-backlog-consumer.Dockerfile: the rdkafka dependency (and its
# cmake/libsasl2/libcurl4 build requirements) went with the legacy Kafka
# backend in Deploy C (PL-15a). movement-relay is the only Kafka client.
ARG CARGO_PROFILE=release

FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
ARG CARGO_PROFILE

WORKDIR /app
COPY . .
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cargo-target-1.88,target=/app/target,sharing=locked \
    if [ "${CARGO_PROFILE}" = "release" ]; then \
      cargo build --release --bin trust-consumer; \
    else \
      cargo build --bin trust-consumer; \
    fi \
    && cp "/app/target/${CARGO_PROFILE}/trust-consumer" /usr/local/bin/trust-consumer

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

# `curl` is added on top of the poller Dockerfiles' pattern solely so
# docker-compose's HEALTHCHECK can probe `GET /healthz` from inside the
# container -- same reasoning as docker/api.Dockerfile's runtime stage.
# libssl3 is for reqwest's native-tls feature.
# hadolint ignore=DL3008 # apt versions unpinned on purpose; see .hadolint.yaml
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 trust-consumer \
    && useradd --system --no-create-home --shell /usr/sbin/nologin --uid 1000 --gid 1000 trust-consumer

COPY --from=builder /usr/local/bin/trust-consumer /usr/local/bin/trust-consumer
COPY --chown=trust-consumer:trust-consumer reference-data/ /app/reference-data/

# Numeric USER, not the `trust-consumer` name useradd created above: Kubernetes'
# runAsNonRoot admission check (this chart's podSecurityContext sets
# runAsNonRoot: true with no explicit runAsUser) resolves the image's
# config purely from its manifest -- it does NOT read /etc/passwd inside
# the image -- so a symbolic USER fails admission with "container has
# runAsNonRoot and image has non-numeric user, cannot verify user is
# non-root". Pinned to the same uid/gid useradd was given above so this
# stays in sync with the group ownership set via COPY --chown/groupadd.
USER 1000:1000

ENTRYPOINT ["/usr/local/bin/trust-consumer"]
