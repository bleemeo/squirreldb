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

And tell test programs to use this Cassandra:
```
export SQUIRRELDB_CASSANDRA_ADDRESSES=$(docker inspect squirreldb-cassandra  -f '{{ .NetworkSettings.IPAddress }}'):9042
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
go run ./tests/squirreldb-cassandra-index-bench/ -bench.worker-max-threads 20 -bench.worker-processes 2 -bench.worker-client 2 -bench.query 0 -bench.batch-size 100 -force-fair-lb
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

Finally if you want to test a bit the expiration of metric you may want to run the following two command:
```
go run ./tests/squirreldb-cassandra-index-bench/ -bench.query 0 -expired-fraction 1 -bench.expiration=false -bench.shard-end 30
go run ./tests/squirreldb-cassandra-index-bench/ -bench.query 0 -expired-fraction 2 -bench.expiration=true -bench.shard-end 30 -no-drop
```

The first command will:

* Create 30k metrics that all are set to expire, but don't run the expiration
* The second will update metric, and keep only 1/2 to expire. The other will get the expiration date updated.
  Then run the expiration which should only delete 15k metrics
