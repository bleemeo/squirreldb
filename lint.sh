#!/bin/sh

set -e

LINTER_VERSION=v1.57.2

USER_UID=$(id -u)

case "$1" in
   "")
      ;;
   "coverage")
      COVERAGE=1
      ;;
   "lint")
      LINT=1
      ;;
   *)
      echo "Usage: $0 [coverage|lint]"
      echo "  coverage: run test coverage"
      echo "  lint: run linter only, skip tests"
      exit 1
esac

if docker volume ls | grep -q squirreldb-buildcache; then
   GO_MOUNT_CACHE="-v squirreldb-buildcache:/go/pkg"
fi

if [ "${COVERAGE}" = "1" ]; then
   docker run --rm -v "$(pwd)":/app ${GO_MOUNT_CACHE} -e HOME=/go/pkg \
      -w /app golangci/golangci-lint:${LINTER_VERSION} \
      sh -exc "
      go test ./... --coverprofile=coverage.out
      go tool cover -html=coverage.out -o coverage.html
      chown $USER_UID coverage.out coverage.html
      "

   exit
fi

if [ "${LINT}" != "1" ]; then
   docker run --rm -v "$(pwd)":/app ${GO_MOUNT_CACHE} -e HOME=/go/pkg \
      -w /app golangci/golangci-lint:${LINTER_VERSION} \
      sh -exc "
      go test ./...
      go test -race ./... -short
      "
fi

docker run --rm -v "$(pwd)":/app ${GO_MOUNT_CACHE} -e HOME=/go/pkg \
   -e GOOS=linux -e GOARCH=amd64 -w /app golangci/golangci-lint:${LINTER_VERSION} \
   golangci-lint run

echo "Success"
