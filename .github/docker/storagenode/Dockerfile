# Keep the runtime layout and public environment variables compatible with
# https://github.com/storj/storagenode-docker.
FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS certificates

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM debian:bookworm-slim

ARG TARGETOS
ARG TARGETARCH

COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=binaries /${TARGETOS}_${TARGETARCH}/storagenode /app/bin/storagenode
COPY entrypoint /entrypoint

RUN chmod 0755 /app/bin/storagenode /entrypoint \
    && mkdir -p /app/config /app/identity \
    && chmod a+rw /app/config /app/identity

EXPOSE 28967/tcp
EXPOSE 28967/udp
EXPOSE 14002/tcp

WORKDIR /app
ENTRYPOINT ["/entrypoint"]

ENV ADDRESS="" \
    EMAIL="" \
    WALLET="" \
    STORAGE="2.0TB" \
    SETUP="false" \
    AUTO_UPDATE="false" \
    LOG_LEVEL="" \
    VERSION_SERVER_URL="https://version.storj.io"
