#!/bin/sh

set -e

# Should be the same as build.sh
GORELEASER_VERSION="v2.8.0"

while [ ! -z "$1" ]; do
    case "$1" in
    "cluster")
        WITH_CLUSTER=1
        ;;
    "race")
        WITH_RACE=1
        ;;
    "long")
        WITH_LONG=1
        ;;
    "nostop")
        WITH_NOSTOP=1
        ;;
    "shell")
        WITH_SHELL=1
        ;;
    "scylladb")
        WITH_SCYLLADB=1
        ;;
    *)
        echo "Usage: $0 [cluster|race|long|nostop|scylladb]"
        echo " cluster: Run test with clustered Redis & Cassandra"
        echo "    long: Run longer test"
        echo "    race: Run test with -race"
        echo "  nostop: Do not stop Cassandra & Redis at the end"
        echo "   shell: Open a shell instead of running tests. It will start Redis & Cassandra."
        echo "          It also forward the port 9200 to a SquirrelDB running inside this shell"
        echo "          The shell container will be named \"squirreldb-test-shell\""
        echo "scylladb: Use ScyllaDB instead of Cassandra"

        exit 1
    esac
    shift
done

if docker volume ls | grep -q squirreldb-buildcache; then
   GO_MOUNT_CACHE="-v squirreldb-buildcache:/go/pkg"
fi

if [ "${WITH_CLUSTER}" = "1" ]; then
    if [ "${WITH_SCYLLADB}" = "1" ]; then
        echo "ScyllaDB is not yet supported in cluster with run-tests.sh"
        exit 1
    fi

    echo
    echo "== Starting cluster component using examples/squirreldb_ha/"
    (cd examples/squirreldb_ha/; docker compose up -d cassandra1 cassandra2 cassandra3 redis1 redis2 redis3 redis4 redis5 redis6 redis_init)
    docker_network="--network squirreldb_ha_default"
    export SQUIRRELDB_CASSANDRA_ADDRESSES=cassandra1:9042,cassandra2:9042
    export SQUIRRELDB_REDIS_ADDRESSES=redis1:6379,redis2:6379
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=3
elif [ "${WITH_SCYLLADB}" = "1" ]; then
    echo
    echo "== Starting ScyllaDB & Redis"
    docker run --name squirreldb-test-scylla -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M scylladb/scylla || true
    docker run --name squirreldb-test-redis -d redis || true

    docker_network=""
    export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-scylla  -f '{{ .NetworkSettings.IPAddress }}'):9042
    export SQUIRRELDB_REDIS_ADDRESSES=$(docker inspect squirreldb-test-redis  -f '{{ .NetworkSettings.IPAddress }}'):6379
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=1
else
    echo
    echo "== Starting Cassandra & Redis"
    docker run --name squirreldb-test-cassandra -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra || true
    docker run --name squirreldb-test-redis -d redis || true

    docker_network=""
    export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-cassandra  -f '{{ .NetworkSettings.IPAddress }}'):9042
    export SQUIRRELDB_REDIS_ADDRESSES=$(docker inspect squirreldb-test-redis  -f '{{ .NetworkSettings.IPAddress }}'):6379
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=1
fi

if [ "${WITH_RACE}" = "1" ]; then
    race_opt="-ldflags='-linkmode external -extldflags=-static' -race"
else
    race_opt=""
fi

lock_opt="--worker-processes 2 --worker-threads 25 --work-duration 100ms --worker-delay 10ms"
lock2_opt="--worker-processes 5 --worker-threads 1 --work-duration 250ms --worker-delay 500ms"
lock3_opt="--worker-processes 5 --worker-threads 1 --work-duration 250ms --worker-delay 500ms --with-block"
index_bench2_opt="--bench.scale-agent 10 --bench.scale-tenant 5"

if [ "${WITH_LONG}" = "1" ]; then
    lock_opt="--worker-processes 3 --run-time 90s"
    lock2_opt="${lock2_opt} --run-time 90s"
    lock3_opt="${lock3_opt} --run-time 90s"
    index_bench_opt="--bench.query 500 --bench.shard-end 10 --bench.shard-size 2000 --bench.worker-max-threads 5 --bench.worker-processes 2"
    index_bench2_opt="${index_bench2_opt} --bench.scale-agent 25 --bench.scale-tenant 10"
    remote_store_opt="--threads 3 --scale 10"
    remote_store2_opt="--test.processes 2 --test.run-duration 1m"
    redis_opt="--test.run-time=1m"
fi




export GORACE=halt_on_error=1

if [ "${WITH_SHELL}" = "1" ]; then

docker run $docker_network -p 127.0.0.1:9200:9201 --rm -ti -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES \
    -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e SQUIRRELDB_REDIS_ADDRESSES \
    -e GORACE \
    -e SQUIRRELDB_TELEMETRY_ENABLED=false -e SQUIRRELDB_LISTEN_ADDRESS=0.0.0.0:9201 \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --name squirreldb-test-shell \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} bash || true
else

echo "== waiting stores"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES \
    -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e SQUIRRELDB_REDIS_ADDRESSES \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/wait-stores"

echo
echo "== Running squirreldb-cassandra-lock-bench (threaded)"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/squirreldb-cassandra-lock-bench/ $lock_opt"

echo
echo "== Running squirreldb-cassandra-lock-bench (single thread, more processes)"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/squirreldb-cassandra-lock-bench/ $lock2_opt"

echo "== Running squirreldb-cassandra-lock-bench (with blocking/unblocking from time to time)"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/squirreldb-cassandra-lock-bench/ $lock3_opt"

echo
echo "== Running squirreldb-cassandra-index-bench"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/squirreldb-cassandra-index-bench/ --verify $index_bench_opt"

echo
echo "== Running squirreldb-cassandra-index-bench2"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/squirreldb-cassandra-index-bench2/ $index_bench2_opt"

echo
echo "== Running squirreldb-promql-aggregate"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e SQUIRRELDB_REDIS_ADDRESSES -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/squirreldb-promql-aggregate/"

echo
echo "== Running remote-storage-test"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e SQUIRRELDB_REDIS_ADDRESSES -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/remote-storage-test/ $remote_store_opt"

echo
echo "== Running remote-storage-test2"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e SQUIRRELDB_REDIS_ADDRESSES -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/remote-storage-test2/ $remote_store2_opt"

echo
echo "== Running squirreldb-cluster-redis"
docker run $docker_network --rm -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e GORACE -e SQUIRRELDB_REDIS_ADDRESSES \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} \
    sh -exc "go run $race_opt ./tests/squirreldb-cluster-redis/ $redis_opt"


echo
echo "== Success =="

fi

if [ ! "${WITH_NOSTOP}" = "1" ]; then
    if [ "${WITH_CLUSTER}" = "1" ]; then
        echo
        echo "== Stopping cluster component using examples/squirreldb_ha/"
        (cd examples/squirreldb_ha/; docker compose down -v)
    else
        echo
        echo "== Stopping Cassandra & Redis"

        if [ "${WITH_SCYLLADB}" = "1" ]; then
            docker rm -f squirreldb-test-scylla
        else
            docker rm -f squirreldb-test-cassandra
        fi

        docker rm -f squirreldb-test-redis
    fi
fi
