#!/bin/sh

set -e

UID=$(id -u)

# Should be the same as build.sh
GORELEASER_VERSION="v0.157.0"

if [ -e .build-cache ]; then
   GO_MOUNT_CACHE="-v $(pwd)/.build-cache:/go/pkg"
fi

echo
echo "== Starting cluster component using examples/squirreldb_ha/"
(cd examples/squirreldb_ha/; docker-compose up -d cassandra1 cassandra2 cassandra3 redis1 redis2 redis3 redis4 redis5 redis6 redis_init)
export GORACE=halt_on_error=1

echo "== waiting stores"
docker run --network squirreldb_ha_default --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES=cassandra1:9042,cassandra2:9042 -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=3 \
    -e SQUIRRELDB_REDIS_ADDRESSES=redis1:6379,redis2:6379 \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/wait-stores'


echo
echo "== Running squirreldb-cassandra-lock-bench"
docker run --network squirreldb_ha_default --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES=cassandra1:9042,cassandra2:9042 -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=3 \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/squirreldb-cassandra-lock-bench/ --worker-processes 3 --run-time 90s'

echo
echo "== Running squirreldb-cassandra-index-bench"
docker run --network squirreldb_ha_default --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES=cassandra1:9042,cassandra2:9042 -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=3 \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/squirreldb-cassandra-index-bench/ --verify --bench.query 500 --bench.shard-end 10 --bench.shard-size 2000 --bench.worker-max-threads 5 --bench.worker-processes 2'

echo
echo "== Running remote-storage-test"
docker run --network squirreldb_ha_default --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES=cassandra1:9042,cassandra2:9042 -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=3 \
    -e SQUIRRELDB_REDIS_ADDRESSES=redis1:6379,redis2:6379 \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/remote-storage-test/ --threads 3 --scale 10'

echo
echo "== Running squirreldb-cluster-redis"
docker run --network squirreldb_ha_default --rm -u $UID -e HOME=/go/pkg \
    -e SQUIRRELDB_REDIS_ADDRESSES=redis1:6379,redis2:6379 \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} sh -c 'go run -race ./tests/squirreldb-cluster-redis/ --test.run-time=1m'


echo
echo "== Success =="
