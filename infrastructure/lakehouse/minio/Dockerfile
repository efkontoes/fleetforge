# MinIO from source. Upstream archived github.com/minio/minio and stopped publishing
# container images (Docker Hub denies minio/minio; checked 2026-11-13). This builds
# the last source release, which also fixes advisory GHSA-jjjj-jwhf-8rgr that the
# last remaining prebuilt image (bitnamilegacy/minio:2025.7.23) predates. Pinned by
# commit: a tag can be moved, a commit cannot. See ADR 0012 for alternatives.
#
# Build cost: the Go compile needs roughly 1.5-3 GiB of RAM. Build only while the
# Milestone 2 stack is down. GOMAXPROCS bounds parallelism (and so peak memory).
ARG GO_IMAGE=golang:1.24.8-bookworm@sha256:4ed690d6649d63c312b99a6120025ec79ce3b542968a37da53d6236c7c61a848
FROM ${GO_IMAGE} AS build
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
ARG MC_COMMIT=7394ce0dd2a80935aded936b09fa12cbb3cb8096
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOMAXPROCS=2
RUN git clone --filter=blob:none https://github.com/minio/minio /src/minio \
 && git -C /src/minio checkout --detach "$MINIO_COMMIT" \
 && test "$(git -C /src/minio rev-parse HEAD)" = "$MINIO_COMMIT" \
 && cd /src/minio && go build -ldflags "-s -w" -o /out/minio .
RUN git clone --filter=blob:none https://github.com/minio/mc /src/mc \
 && git -C /src/mc checkout --detach "$MC_COMMIT" \
 && test "$(git -C /src/mc rev-parse HEAD)" = "$MC_COMMIT" \
 && cd /src/mc && go build -ldflags "-s -w" -o /out/mc .

# Same base as the application image (already present locally); its Python serves
# the health check, so no extra tooling is installed.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
COPY --from=build /out/minio /out/mc /usr/local/bin/
RUN useradd --uid 10002 --create-home minio && mkdir /data && chown minio /data
USER minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
