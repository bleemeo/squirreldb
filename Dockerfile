FROM gcr.io/distroless/base

LABEL MAINTAINER="Bleemeo Docker Maintainers <packaging-team@bleemeo.com>"

ENV SQUIRRELDB_REMOTE_STORAGE_LISTEN_ADDRESS=0.0.0.0:9201

ENV SQUIRRELDB_INTERNAL_INSTALLATION_FORMAT="Docker"

ENV SQUIRRELDB_TELEMETRY_ID_PATH="/tmp/telemetry.json"

COPY squirreldb /usr/sbin/squirreldb

CMD ["/usr/sbin/squirreldb"]
