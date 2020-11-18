#!/bin/sh

set -e

UID=$(id -u)

case "$1" in
   "")
      ;;
   "go")
      ONLY_GO=1
      ;;
   "race")
      ONLY_GO=1
      WITH_RACE=1
      ;;
   *)
      echo "Usage: $0 [go|race]"
      echo "  go: only compile Go"
      echo "race: only compile Go with race detector"
      exit 1
esac

if [ -e .build-cache ]; then
   GO_MOUNT_CACHE="-v $(pwd)/.build-cache:/go/pkg"
fi


GORELEASER_VERSION="v0.147.2"

if [ "${ONLY_GO}" = "1" -a "${WITH_RACE}" != "1" ]; then
   docker run --rm -u $UID:`getent group docker|cut -d: -f 3` -e HOME=/go/pkg -e CGO_ENABLED=0 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      -v /var/run/docker.sock:/var/run/docker.sock \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go build .'
elif [ "${ONLY_GO}" = "1" -a "${WITH_RACE}" = "1"  ]; then
   docker run --rm -u $UID:`getent group docker|cut -d: -f 3` -e HOME=/go/pkg -e CGO_ENABLED=1 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      -v /var/run/docker.sock:/var/run/docker.sock \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION}-cgo sh -c 'go build -ldflags="-linkmode external -extldflags=-static" -race .'
else
   docker run --rm -u $UID:`getent group docker|cut -d: -f 3` -e HOME=/go/pkg -e CGO_ENABLED=0 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      -v /var/run/docker.sock:/var/run/docker.sock \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go test ./... && goreleaser --rm-dist --snapshot --parallelism 2'
fi
