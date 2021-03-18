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

echo
echo "== Starting Redis"
docker run --name squirreldb-test-redis -d redis:6.0.9 && sleep 5 || true
docker exec squirreldb-test-redis redis-cli flushdb


export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-cassandra  -f '{{ .NetworkSettings.IPAddress }}'):9042
export SQUIRRELDB_REDIS_ADDRESSES=$(docker inspect squirreldb-test-redis  -f '{{ .NetworkSettings.IPAddress }}'):6379
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
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e GORACE -e SQUIRRELDB_REDIS_ADDRESSES \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/remote-storage-test/ --scale 5 --threads 2 --start-bultin-squirreldb'

echo
echo "== Running squirreldb-cluster-redis"
docker run --rm -u $UID -e HOME=/go/pkg \
    -e GORACE -e SQUIRRELDB_REDIS_ADDRESSES \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/squirreldb-cluster-redis/'


echo
echo "== Success =="
