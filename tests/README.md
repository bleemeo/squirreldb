# Tests

This folder contains tests that are not unit-test. Mostly is contains tests
that require an external component.

Most tests are mainly benchmark (but usually with some validating which allow
to ensure correct behavior).

Tests:

* squirreldb-cassandra-index-bench: Tests & benchmark Cassandra Index.


To run test, you will need a Cassandra:

```
docker run --name squirreldb-cassandra -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
```

# squirreldb-cassandra-index-bench

This test does few validation test and the will create a "shard" with a fixed number of metrics.
Think "shard" a one tenant, queries will only ask metric from one shard.

Run it with:
```
export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-cassandra  -f '{{ .NetworkSettings.IPAddress }}'):9042
go run ./tests/squirreldb-cassandra-index-bench/
```

To see impact of other shard (total number of total metrics) you may want to re-run the command increasing the shard-start/shard-end

```
go run ./tests/squirreldb-cassandra-index-bench/
go run ./tests/squirreldb-cassandra-index-bench/ -no-drop -bench.shard-start 6 -bench.shard-end 10
go run ./tests/squirreldb-cassandra-index-bench/ -no-drop -bench.shard-start 11 -bench.shard-end 20
go run ./tests/squirreldb-cassandra-index-bench/ -no-drop -bench.shard-start 21 -bench.shard-end 50
go run ./tests/squirreldb-cassandra-index-bench/ -no-drop -bench.shard-start 51 -bench.shard-end 100
go run ./tests/squirreldb-cassandra-index-bench/ -no-drop -bench.shard-start 101 -bench.shard-end 200
[...]
```

With larger index, query may excess the deadline (5s by default), you may want to run with larger deadline:

```
go run ./tests/squirreldb-cassandra-index-bench/ -no-drop -bench.shard-start 201 -bench.shard-end 400 -bench.max-time 30s
```
