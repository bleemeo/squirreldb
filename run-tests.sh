#!/bin/sh

set -e

UID=$(id -u)

# Should be the same as build.sh
GORELEASER_VERSION="v0.157.0"

if [ -e .build-cache ]; then
   GO_MOUNT_CACHE="-v $(pwd)/.build-cache:/go/pkg"
fi

echo
echo "== Starting Cassandra"
docker run --name squirreldb-test-cassandra -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra:3.11.9 && sleep 20 || true
docker exec squirreldb-test-cassandra cqlsh -e 'DROP KEYSPACE squirreldb_test' || true
docker exec squirreldb-test-cassandra nodetool clearsnapshot || true

export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-cassandra  -f '{{ .NetworkSettings.IPAddress }}'):9042
export GORACE=halt_on_error=1

echo
echo "== Running squirreldb-cassandra-lock-bench"
docker run --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/squirreldb-cassandra-lock-bench/ -run-time=10s'

echo
echo "== Running squirreldb-cassandra-index-bench"
docker run --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/squirreldb-cassandra-index-bench/ -verify -bench.shard-size 100 -bench.query 100'

echo
echo "== Running remote-storage-test"
docker run --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e GORACE \
    -e SQUIRRELDB_CASSANDRA_KEYSPACE=squirreldb_test \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/remote-storage-test/ --scale 5 --threads 2 --start-bultin-squirreldb'

echo
echo "== Success =="
