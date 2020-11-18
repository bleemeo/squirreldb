#!/bin/sh

set -e

UID=$(id -u)

case "$1" in
   "")
      ;;
   "go")
      ONLY_GO=1
      ;;
   *)
      echo "Usage: $0 [go]"
      echo "  go: only compile Go"
      exit 1
esac

if [ -e .build-cache ]; then
   GO_MOUNT_CACHE="-v $(pwd)/.build-cache:/go/pkg"
fi


GORELEASER_VERSION="v0.147.2"

if [ "${ONLY_GO}" = "1" ]; then
   docker run --rm -u $UID:`getent group docker|cut -d: -f 3` -e HOME=/go/pkg -e CGO_ENABLED=0 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      -v /var/run/docker.sock:/var/run/docker.sock \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go build .'
else
   docker run --rm -u $UID:`getent group docker|cut -d: -f 3` -e HOME=/go/pkg -e CGO_ENABLED=0 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      -v /var/run/docker.sock:/var/run/docker.sock \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go test ./... && goreleaser --rm-dist --snapshot --parallelism 2'
fi
