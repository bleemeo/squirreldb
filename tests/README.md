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
go run ./tests/squirreldb-cassandra-index-bench/ -drop
```

To see impact of other shard/number of total metrics, you may be interested in running in two step:

* First insert data without querying (because querying is rather long):
```
go run ./tests/squirreldb-cassandra-index-bench/ -bench.query 0 -bench.shard-end 10
```
* Then only query one shard (the last for example):
```
go run ./tests/squirreldb-cassandra-index-bench/ -bench.shard-size 0 -bench.shard-start 10 -bench.shard-end 10
```

Run-re the two commands, increasing the last shard number to see evolution of times with larger index
