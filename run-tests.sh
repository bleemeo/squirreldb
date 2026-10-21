#!/bin/sh

set -eu

# Should be the same as build.sh
GORELEASER_VERSION="v2.18.0"

# ClickHouse server image of the single-node "clickhouse" mode (the cluster mode
# uses the one of examples/squirreldb_ha_clickhouse). Pinned to a tested release
# rather than latest, so a new ClickHouse release does not silently change what
# the tests run against; override it to test another one, e.g.
# CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:26.3.10.60 ./run-tests.sh clickhouse
# The tested versions are listed in docs/clickhouse-cluster.md.
CLICKHOUSE_IMAGE="${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server:26.8.15.10}"

WITH_CLUSTER=0
WITH_RACE=0
WITH_COVER=0
WITH_NOSTOP=0
WITH_SHELL=0
START_STORE_SCYLLADB=0
START_STORE_CASSANDRA=0
START_STORE_CLICKHOUSE=0
BACKEND=
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
        START_STORE_SCYLLADB=1
        BACKEND=cassandra  # ScyllaDB is a Cassandra-like
        ;;
    "cassandra")
        START_STORE_CASSANDRA=1
        BACKEND=cassandra
        ;;
    "clickhouse")
        START_STORE_CLICKHOUSE=1
        BACKEND=clickhouse
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
 cassandra: Use Cassandra as store [default]
  scylladb: Use ScyllaDB instead of Cassandra
clickhouse: Use the ClickHouse as store.
            The server image is \$CLICKHOUSE_IMAGE (default ${CLICKHOUSE_IMAGE}).

If multiple store are specified, they are all started (could be useful in shell session),
but last one is used as backend. Multiple store is only supported in non-cluster environment.
EOF

        exit 1
    esac
    shift
done

GO_MOUNT_CACHE=""
if docker volume ls | grep -q squirreldb-buildcache; then
   GO_MOUNT_CACHE="-v squirreldb-buildcache:/go/pkg"
fi

# Ensure that Cassandra is the default backend and started by default
if [ "${BACKEND}" = "" ]; then
    BACKEND="cassandra"
    START_STORE_CASSANDRA=1
fi

_store_started=0

if [ "${WITH_CLUSTER}" = "1" ] && [ "${START_STORE_CLICKHOUSE}" = 1 ]; then
    if [ "${_store_started}" = "1" ]; then
        echo "Multiple store is not supported in Cluster mode"
        exit 1
    fi
    _store_started=1

    echo
    echo "== Starting cluster component using examples/squirreldb_ha_clickhouse/"
    (cd examples/squirreldb_ha_clickhouse/; docker compose up -d clickhouse-keeper-0 clickhouse-keeper-1 clickhouse-keeper-2 clickhouse-server-0 clickhouse-server-1 redis1 redis2 redis3 redis4 redis5 redis6 redis_init)
    docker_network="--network squirreldb_ha_clickhouse_default"
    export SQUIRRELDB_CLICKHOUSE_ADDRESSES=clickhouse-server-0:9000,clickhouse-server-1:9000
    export SQUIRRELDB_REDIS_ADDRESSES=redis1:6379,redis2:6379
    export SQUIRRELDB_CLICKHOUSE_CLUSTER_NAME=main_cluster
    export SQUIRRELDB_CLICKHOUSE_PASSWORD=secretForTest
    export SQUIRRELDB_CLICKHOUSE_USERNAME=squirreldb_user
fi

if [ "${WITH_CLUSTER}" = "1" ] && [ "${START_STORE_SCYLLADB}" = "1" ]; then
    if [ "${_store_started}" = "1" ]; then
        echo "Multiple store is not supported in Cluster mode"
        exit 1
    fi
    _store_started=1

    echo "ScyllaDB is not yet supported in cluster with run-tests.sh"
    exit 1
fi

if [ "${WITH_CLUSTER}" = "1" ] && [ "${START_STORE_CASSANDRA}" = 1 ]; then
    if [ "${_store_started}" = "1" ]; then
        echo "Multiple store is not supported in Cluster mode"
        exit 1
    fi
    _store_started=1

    echo
    echo "== Starting cluster component using examples/squirreldb_ha/"
    (cd examples/squirreldb_ha/; docker compose up -d cassandra1 cassandra2 cassandra3 redis1 redis2 redis3 redis4 redis5 redis6 redis_init)
    docker_network="--network squirreldb_ha_default"
    export SQUIRRELDB_CASSANDRA_ADDRESSES=cassandra1:9042,cassandra2:9042
    export SQUIRRELDB_REDIS_ADDRESSES=redis1:6379,redis2:6379
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=3
fi

if [ "${WITH_CLUSTER}" = "0" ]; then
    echo
    echo "== Starting common component"
    docker run --name squirreldb-test-redis -d redis || true

    export SQUIRRELDB_REDIS_ADDRESSES=$(docker inspect squirreldb-test-redis  -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):6379
fi

if [ "${WITH_CLUSTER}" = "0" ] && [ "${START_STORE_SCYLLADB}" = "1" ]; then
    echo
    echo "== Starting ScyllaDB & Redis"
    docker run --name squirreldb-test-scylla -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M scylladb/scylla || true

    docker_network=""
    export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-scylla  -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):9042
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=1
fi

if [ "${WITH_CLUSTER}" = "0" ] && [ "${START_STORE_CLICKHOUSE}" = "1" ]; then
    # The database is created by the ClickHouse image (CLICKHOUSE_DB); SquirrelDB only creates the tables inside it.
    echo
    echo "== Starting ClickHouse (${CLICKHOUSE_IMAGE}) & Redis"
    docker run --name squirreldb-test-clickhouse -d -e CLICKHOUSE_DB=squirreldb_test -e CLICKHOUSE_USER=squirreldb_user -e CLICKHOUSE_PASSWORD=secretForTest "${CLICKHOUSE_IMAGE}" || true
    # A container left by an earlier run (nostop) is reused whatever its image: say which one.
    echo "ClickHouse container image: $(docker inspect squirreldb-test-clickhouse -f '{{ .Config.Image }}')"

    docker_network=""
    export SQUIRRELDB_CLICKHOUSE_ADDRESSES=$(docker inspect squirreldb-test-clickhouse -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):9000
    export SQUIRRELDB_CLICKHOUSE_CLUSTER_NAME=""
    export SQUIRRELDB_CLICKHOUSE_PASSWORD=secretForTest
    export SQUIRRELDB_CLICKHOUSE_USERNAME=squirreldb_user
fi

if [ "${WITH_CLUSTER}" = "0" ] && [ "${START_STORE_CASSANDRA}" = "1" ]; then
    echo
    echo "== Starting Cassandra & Redis"
    docker run --name squirreldb-test-cassandra -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra || true

    docker_network=""
    export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-cassandra  -f '{{ .NetworkSettings.Networks.bridge.IPAddress }}'):9042
    export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=1
fi

export SQUIRRELDB_BACKEND=${BACKEND}

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
telemetry_opt=""

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
        -e SQUIRRELDB_BACKEND \
        -e SQUIRRELDB_CLICKHOUSE_USERNAME \
        -e SQUIRRELDB_CLICKHOUSE_PASSWORD \
        -e SQUIRRELDB_CLICKHOUSE_CLUSTER_NAME \
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
    -e SQUIRRELDB_BACKEND \
    -e SQUIRRELDB_CLICKHOUSE_USERNAME \
    -e SQUIRRELDB_CLICKHOUSE_PASSWORD \
    -e SQUIRRELDB_CLICKHOUSE_CLUSTER_NAME \
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

# The index benches exercise the Cassandra index, which the ClickHouse backend
# does not use (series are indexed in tsdb_tags). They run unconditionally: with
# a non-Cassandra backend they skip themselves (daemon.ErrSkipBackend).
echo
echo "== Running squirreldb-cassandra-index-bench"
run_sqdb "go run $build_opt ./tests/squirreldb-cassandra-index-bench/ --verify $index_bench_opt"

echo
echo "== Running squirreldb-cassandra-index-bench2"
run_sqdb "go run $build_opt ./tests/squirreldb-cassandra-index-bench2/ $index_bench2_opt"

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
echo "== Running squirreldb-telemetry"
run_sqdb "go run $build_opt ./tests/squirreldb-telemetry/ $telemetry_opt"

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
        -e SQUIRRELDB_BACKEND \
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
    if [ "${WITH_CLUSTER}" = "1" ] && [ "${START_STORE_CLICKHOUSE}" = 1 ]; then
        echo
        echo "== Stopping cluster component using examples/squirreldb_ha_clickhouse/"
        (cd examples/squirreldb_ha_clickhouse/; docker compose down -v)
    fi
    if [ "${WITH_CLUSTER}" = "1" ] && [ "${START_STORE_CASSANDRA}" = 1 ]; then
        echo
        echo "== Stopping cluster component using examples/squirreldb_ha/"
        (cd examples/squirreldb_ha/; docker compose down -v)
    fi

    if [ "${WITH_CLUSTER}" = "0" ]; then
        echo "== Stop common component (Redis)"
        docker rm -f squirreldb-test-redis
    fi

    if [ "${WITH_CLUSTER}" = "0" ] && [ "${START_STORE_CLICKHOUSE}" = "1" ]; then
        echo
        echo "== Stopping ClickHouse"
        docker rm -f squirreldb-test-clickhouse
    fi

    if [ "${WITH_CLUSTER}" = "0" ] && [ "${START_STORE_CASSANDRA}" = 1 ]; then
        echo
        echo "== Stopping Cassandra"

        docker rm -f squirreldb-test-cassandra
    fi

    if [ "${WITH_CLUSTER}" = "0" ] && [ "${START_STORE_SCYLLADB}" = 1 ]; then
        echo
        echo "== Stopping ScyllaDB"

        docker rm -f squirreldb-test-scylla
    fi
fi
