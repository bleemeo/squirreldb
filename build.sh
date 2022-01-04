#!/bin/sh

set -e

USER_UID=$(id -u)

# Should be the same as run-tests.sh
GORELEASER_VERSION="v1.2.5"

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

if docker volume ls | grep -q squirreldb-buildcache; then
   GO_MOUNT_CACHE="-v squirreldb-buildcache:/go/pkg"
fi

if [ "${ONLY_GO}" = "1" -a "${WITH_RACE}" != "1" ]; then
   docker run --rm -e HOME=/go/pkg -e CGO_ENABLED=0 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION} sh -c "go build . && chown $USER_UID squirreldb"
elif [ "${ONLY_GO}" = "1" -a "${WITH_RACE}" = "1"  ]; then
   docker run --rm -e HOME=/go/pkg -e CGO_ENABLED=1 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION} sh -c "go build -ldflags='-linkmode external -extldflags=-static' -race . && chown $USER_UID squirreldb"
else
   docker run --rm -e HOME=/go/pkg -e CGO_ENABLED=0 \
      -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
      -v /var/run/docker.sock:/var/run/docker.sock \
      --entrypoint '' \
      goreleaser/goreleaser:${GORELEASER_VERSION} sh -c "(goreleaser check && go test ./... && goreleaser --rm-dist --snapshot --parallelism 2);result=\$?;chown -R $USER_UID dist; exit \$result"
fi
