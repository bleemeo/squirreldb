#!/bin/sh

set -eu

# Should be the same as build.sh
GORELEASER_VERSION="v2.15.4"

WITH_CLUSTER=0
WITH_RACE=0
WITH_COVER=0
WITH_NOSTOP=0
WITH_SHELL=0
WITH_SCYLLADB=0
WITH_CLICKHOUSE=0
WITH_LONG=""

USER_UID=$(id -u)

while [ "$#" -gt 0 ]; do
    case "$1" in
    "cluster")
        WITH_CLUSTER=1
        ;;
    "race")
        WITH_RACE=1
        ;;
    "cover")
        WITH_COVER=1
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
    "clickhouse")
        WITH_CLICKHOUSE=1
        ;;
    *)
        cat << EOF
Usage: $0 [cluster|race|long|nostop|scylladb|clickhouse]
   cluster: Run test with clustered Redis & Cassandra
      long: Run longer test
      race: Run test with -race
     cover: Run test with -cover and produce coverage-run-tests/report.html report
            You might want to clean "coverage-run-tests" folder first
    nostop: Do not stop Cassandra, Redis & ClickHouse at the end
     shell: Open a shell instead of running tests. It will start Redis & Cassandra.
            It also forward the port 9200 to a SquirrelDB running inside this shell
            The shell container will be named "squirreldb-test-shell"
  scylladb: Use ScyllaDB instead of Cassandra
clickhouse: Use the ClickHouse TSDB backend (alongside Cassandra for locks/states).
            Skips the Cassandra-index benches; the other tests run against ClickHouse.
EOF

        exit 1
    esac
    shift
done

if [ "${WITH_CLICKHOUSE}" = "1" ] && [ "${WITH_CLUSTER}" = "1" ]; then
    echo "ClickHouse is not yet supported in cluster mode with run-tests.sh"
    exit 1
fi

GO_MOUNT_CACHE=""
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
    export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-scylla  -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):9042
    export SQUIRRELDB_REDIS_ADDRESSES=$(docker inspect squirreldb-test-redis  -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):6379
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=1
else
    echo
    echo "== Starting Cassandra & Redis"
    docker run --name squirreldb-test-cassandra -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra || true
    docker run --name squirreldb-test-redis -d redis || true

    docker_network=""
    export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-cassandra  -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):9042
    export SQUIRRELDB_REDIS_ADDRESSES=$(docker inspect squirreldb-test-redis  -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):6379
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=1
fi

# ClickHouse is orthogonal to the store above (locks/states stay on Cassandra):
# it only provides the TSDB backend. The database is created by the ClickHouse
# image (CLICKHOUSE_DB); SquirrelDB only creates the tables inside it.
if [ "${WITH_CLICKHOUSE}" = "1" ]; then
    echo
    echo "== Starting ClickHouse (TSDB backend)"
    docker run --name squirreldb-test-clickhouse -d -e CLICKHOUSE_DB=squirreldb_test -e CLICKHOUSE_PASSWORD=secretForTest clickhouse/clickhouse-server || true

    export SQUIRRELDB_CLICKHOUSE_ADDRESSES=$(docker inspect squirreldb-test-clickhouse -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):9000
    export SQUIRRELDB_INTERNAL_TSDB=clickhouse
    export SQUIRRELDB_CLICKHOUSE_PASSWORD=secretForTest
fi

build_opt=""
if [ "${WITH_COVER}" = "1" ]; then
    mkdir -p coverage-run-tests/covdata
    build_opt="$build_opt -cover"
fi

if [ "${WITH_RACE}" = "1" ]; then
    build_opt="$build_opt -ldflags='-linkmode external -extldflags=-static' -race"
fi

lock_opt="--worker-processes 2 --worker-threads 25 --work-duration 100ms --worker-delay 10ms"
lock2_opt="--worker-processes 5 --worker-threads 1 --work-duration 250ms --worker-delay 500ms"
lock3_opt="--worker-processes 5 --worker-threads 1 --work-duration 250ms --worker-delay 500ms --with-block"
index_bench_opt=""
index_bench2_opt="--bench.scale-agent 10 --bench.scale-tenant 5"
remote_store_opt=""
remote_store2_opt=""
redis_opt=""

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
export GOCOVERDIR=coverage-run-tests/covdata

# run_sqdb runs a command inside the goreleaser build container, with the store
# discovery environment forwarded (Cassandra, Redis and, when set, ClickHouse).
# The ClickHouse variables are only exported in ClickHouse mode; otherwise they
# are simply unset in the container.
run_sqdb() {
    docker run $docker_network --rm -e HOME=/go/pkg \
        -e SQUIRRELDB_LOG_DISABLE_COLOR \
        -e SQUIRRELDB_CASSANDRA_ADDRESSES \
        -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
        -e SQUIRRELDB_REDIS_ADDRESSES \
        -e SQUIRRELDB_CLICKHOUSE_ADDRESSES \
        -e SQUIRRELDB_INTERNAL_TSDB \
        -e SQUIRRELDB_CLICKHOUSE_PASSWORD \
        -e SQUIRRELDB_TELEMETRY_ENABLED=false \
        -e GOCOVERDIR -e GORACE \
        -v "$(pwd)":/src -w /src ${GO_MOUNT_CACHE} \
        --entrypoint '' \
        goreleaser/goreleaser:${GORELEASER_VERSION} \
        sh -euxc "$1"
}

if [ "${WITH_SHELL}" = "1" ]; then

docker run $docker_network -p 127.0.0.1:9200:9201 --rm -ti -e HOME=/go/pkg \
    -e SQUIRRELDB_LOG_DISABLE_COLOR \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES \
    -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
    -e SQUIRRELDB_REDIS_ADDRESSES \
    -e SQUIRRELDB_CLICKHOUSE_ADDRESSES \
    -e SQUIRRELDB_INTERNAL_TSDB \
    -e SQUIRRELDB_CLICKHOUSE_PASSWORD \
    -e SQUIRRELDB_TELEMETRY_ENABLED=false \
    -e SQUIRRELDB_LISTEN_ADDRESS=0.0.0.0:9201 \
    -e GOCOVERDIR -e GORACE \
    -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
    --name squirreldb-test-shell \
    --entrypoint '' \
    goreleaser/goreleaser:${GORELEASER_VERSION} bash || true
else

echo "== waiting stores"
run_sqdb "go run $build_opt ./tests/wait-stores"

echo
echo "== Running squirreldb-cassandra-lock-bench (threaded)"
run_sqdb "go run $build_opt ./tests/squirreldb-cassandra-lock-bench/ $lock_opt"

echo
echo "== Running squirreldb-cassandra-lock-bench (single thread, more processes)"
run_sqdb "go run $build_opt ./tests/squirreldb-cassandra-lock-bench/ $lock2_opt"

echo "== Running squirreldb-cassandra-lock-bench (with blocking/unblocking from time to time)"
run_sqdb "go run $build_opt ./tests/squirreldb-cassandra-lock-bench/ $lock3_opt"

# The index benches exercise the Cassandra index, which the ClickHouse TSDB
# backend does not use (series are indexed in tsdb_tags), so skip them there.
if [ "${WITH_CLICKHOUSE}" = "1" ]; then
    echo
    echo "== Skipping squirreldb-cassandra-index-bench & index-bench2 (Cassandra index unused with ClickHouse TSDB)"
else
    echo
    echo "== Running squirreldb-cassandra-index-bench"
    run_sqdb "go run $build_opt ./tests/squirreldb-cassandra-index-bench/ --verify $index_bench_opt"

    echo
    echo "== Running squirreldb-cassandra-index-bench2"
    run_sqdb "go run $build_opt ./tests/squirreldb-cassandra-index-bench2/ $index_bench2_opt"
fi

echo
echo "== Running squirreldb-promql-aggregate"
run_sqdb "go run $build_opt ./tests/squirreldb-promql-aggregate/"

echo
echo "== Running remote-storage-test"
run_sqdb "go run $build_opt ./tests/remote-storage-test/ $remote_store_opt"

echo
echo "== Running remote-storage-test2"
run_sqdb "go run $build_opt ./tests/remote-storage-test2/ $remote_store2_opt"

echo
echo "== Running squirreldb-cluster-redis"
run_sqdb "go run $build_opt ./tests/squirreldb-cluster-redis/ $redis_opt"

echo
echo "== Running run-test-cases"
run_sqdb "go run $build_opt ./tests/run-test-cases/"



echo
echo "== Success =="

fi

if [ "${WITH_COVER}" = "1" ]; then
    docker run $docker_network --rm -e HOME=/go/pkg \
        -e SQUIRRELDB_LOG_DISABLE_COLOR \
        -e SQUIRRELDB_CASSANDRA_ADDRESSES \
        -e SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR \
        -e SQUIRRELDB_REDIS_ADDRESSES \
        -e SQUIRRELDB_CLICKHOUSE_ADDRESSES \
        -e SQUIRRELDB_INTERNAL_TSDB \
        -e SQUIRRELDB_TELEMETRY_ENABLED=false \
        -e GOCOVERDIR -e GORACE \
        -v $(pwd):/src -w /src ${GO_MOUNT_CACHE} \
        --entrypoint '' \
        goreleaser/goreleaser:${GORELEASER_VERSION} \
        sh -euc "
go tool covdata textfmt -i $GOCOVERDIR -o coverage-run-tests/covdata.out
go tool cover -html=coverage-run-tests/covdata.out -o coverage-run-tests/report.html
chown -R $USER_UID coverage-run-tests/
"
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

    if [ "${WITH_CLICKHOUSE}" = "1" ]; then
        echo
        echo "== Stopping ClickHouse"
        docker rm -f squirreldb-test-clickhouse
    fi
fi
