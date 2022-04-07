# Tests

This folder contains tests that are not unit-test. Mostly it contains tests
that require an external component (Cassandra or Redis).

Tests:

* squirreldb-cassandra-index-bench: Tests & benchmark Cassandra Index.
* squirreldb-cassandra-lock-bench: Ensure validity of locks using Cassandra and
  benchmark performance achived with them.
* squirreldb-cluster-redis: Test the cluster pub/sub.
* squirreldb-promql-bench: Benchmark PromQL queries.
* remote-storage-test: simple write then read using the Prometheus remote storage.
  It ensure data wrote can be re-read.



To run test, you will need a Cassandra (and in some test Redis):

```
docker run --name squirreldb-test-cassandra -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
docker run --name squirreldb-test-redis -d redis
```

And tell test programs to use this Cassandra and Redis:
```
export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-test-cassandra  -f '{{ .NetworkSettings.IPAddress }}'):9042
export SQUIRRELDB_REDIS_ADDRESSES=$(docker inspect squirreldb-test-redis  -f '{{ .NetworkSettings.IPAddress }}'):6379
```

Note: some test will clear store (Cassandra or Redis) before processing.
* For Cassandra, it's the keyspace "squirreldb_test" which is dropped
* For Redis, it's all key starting with "test:"

If you want to run usin goreleaser to use same Go version as the one used for release, run all "go run"
inside a container started with:
```
docker run --rm -ti -e HOME=/go/pkg \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES \
    -e SQUIRRELDB_REDIS_ADDRESSES \
    -v $(pwd):/src -w /src -v squirreldb-buildcache:/go/pkg \
    --entrypoint '' \
    goreleaser/goreleaser:v1.6.3 bash
```

# squirreldb-cassandra-lock-bench

This test validate the Cassandra distributed locks are correct: that is two nodes
can not hold the same lock are the same time.
It also allow to known how fast lock can be took/released.

It test this by:
* Simulating multiple SquirrelDB nodes in one system process (multiple instance
  of the CassandraLock objects).
* Since running in the same process, it use Golang mutex to check that Cassandra
  lock are correct.
* The simulation took lock and do some works for few milliseconds (sleeping), while
  other node try to acquire this lock.
  Be aware that because we simulare work by sleeping, the maximum number of lock
  acquired/s could be limited by this delay.

Run it with:
```
go run ./tests/squirreldb-cassandra-lock-bench/
go run ./tests/squirreldb-cassandra-lock-bench/ --try-lock-duration 1ms
```

# squirreldb-cassandra-index-bench

This test does few validation test and the will create a "shard" with a fixed number of metrics.
Think "shard" a one tenant, queries will only ask metric from one shard.

Run it with:
```
go run ./tests/squirreldb-cassandra-index-bench/
```

To test concurrent insertion in the Index, run with (this try to mimick 2 SquirrelDB being a nginx load-balancer):
```
go run ./tests/squirreldb-cassandra-index-bench/ --bench.worker-max-threads 20 --bench.worker-processes 2 --bench.worker-client 2 --bench.query 0 --bench.batch-size 100 --bench.shard-size 1000 --force-fair-lb
```

To see impact of other shard (total number of total metrics) you may want to re-run the command increasing the shard-start/shard-end

```
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --no-drop --bench.shard-start 6 --bench.shard-end 10
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --no-drop --bench.shard-start 11 --bench.shard-end 20
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --no-drop --bench.shard-start 21 --bench.shard-end 50
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --no-drop --bench.shard-start 51 --bench.shard-end 100
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --no-drop --bench.shard-start 101 --bench.shard-end 200
[...]
```

With larger index, query may excess the deadline (5s by default), you may want to run with larger deadline:

```
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --no-drop --bench.shard-start 201 --bench.shard-end 400 --bench.max-time 30s
```

Finally if you want to test a bit the expiration of metric you may want to run the following two command:
```
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --bench.query 0 --expired-fraction 1 --bench.expiration=false --bench.shard-end 30
go run ./tests/squirreldb-cassandra-index-bench/ --bench.shard-size 1000 --bench.query 0 --expired-fraction 2 --bench.expiration=true --bench.shard-end 30 --no-drop
```

The first command will:

* Create 30k metrics that all are set to expire, but don't run the expiration
* The second will update metric, and keep only 1/2 to expire. The other will get the expiration date updated.
  Then run the expiration which should only delete 15k metrics

# squirreldb-promql-bench

This test benchmarks a PromQL query against SquirrelDB. You can change the number of concurrent queries with the `-parallel` flag.

Run it with:
```
go run ./tests/squirreldb-promql-bench/ -url http://localhost:9201 -run-time 10s -parallel 10 -query node_load5
```

# remote-storage-test

This program do some testing on remote write & remote read. It verify that what was
written could be re-read.

You may use this program against any Prometheus remote read/write storage. You may also
request this test to start SquirrelDB for you:

```
# Using built-in SquirrelDB
go run ./tests/remote-storage-test

# Using existing remote storage
go run ./tests/remote-storage-test --read-url http://localhost:9201/read --write-url http://localhost:9201/write
```

# remote-storage-test2

This is another testing program that rely on remote write & remote read. The difference with previous:
* It does not check metric values.
* It does read and write concurrently
* New metrics are created (churn) and it measure the time between first write and first read that seen such
  metrics.
  It's the primary feature of this test, ensure that query cache is invalidated when metric are created.


```
# Using built-in SquirrelDB
go run ./tests/remote-storage-test2

# Using existing remote storage
go run ./tests/remote-storage-test2 --read-urls http://localhost:9201/read --write-urls http://localhost:9201/write
```

# Run on Cassandra cluster

The easiest is to use run-tests-cluster.sh:
```
./run-tests-cluster.sh
```

To execute the go run with a Cassandra cluster, the easiest way is:

* Start a Cassandra & redis cluster using example/squirreldb_ha:
```
(cd examples/squirreldb_ha; docker-compose up -d cassandra{1,2,3} redis{1,2,3,4,5,6} redis_init)
```

* Configure program to use those Cassandra & Redis and use replication 3:
```
export SQUIRRELDB_CASSANDRA_ADDRESSES=172.28.0.11:9042,172.28.0.12:9042
export SQUIRRELDB_REDIS_ADDRESSES=172.28.0.21:6379,172.28.0.22:6379
export SQUIRRELDB_CASSANDRA_REPLICATION_FACTOR=3
```
* Run tests:
```
go run ./tests/squirreldb-cassandra-index-bench/
go run ./tests/remote-storage-test
...
```

Note: the replication factor is only used at initial creation. To change its value, you need
to destroy (docker-compose down -v) and re-create the Cassandra cluster.
