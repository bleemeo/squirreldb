#!/bin/sh

set -e

docker run --rm -u $UID:`getent group docker|cut -d: -f 3` -e HOME=/go/pkg -e CGO_ENABLED=0 \
   -v $(pwd):/src -w /src \
   -v /var/run/docker.sock:/var/run/docker.sock \
   --entrypoint '' \
   goreleaser/goreleaser:v0.147.2 sh -c 'go test ./... && goreleaser --rm-dist --snapshot'
