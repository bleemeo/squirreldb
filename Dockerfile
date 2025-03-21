FROM --platform=$BUILDPLATFORM alpine:3.21 AS build

ARG TARGETARCH

COPY dist/squirreldb_linux_amd64_v1/squirreldb /squirreldb.amd64
COPY dist/squirreldb_linux_arm64_v8.0/squirreldb /squirreldb.arm64
COPY dist/squirreldb_linux_arm_6/squirreldb /squirreldb.arm

RUN cp -p /squirreldb.$TARGETARCH /squirreldb

FROM alpine:3.21

LABEL maintainer="Bleemeo Docker Maintainers <packaging-team@bleemeo.com>"

RUN apk update && \
    apk add --no-cache ca-certificates

ENV SQUIRRELDB_LISTEN_ADDRESS=0.0.0.0:9201

ENV SQUIRRELDB_INTERNAL_INSTALLATION_FORMAT="Docker"

COPY --from=build /squirreldb /usr/sbin/squirreldb

USER daemon
CMD ["/usr/sbin/squirreldb"]
