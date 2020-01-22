# Tests

This folder contains tests that are not unit-test. Mostly is contains tests
that require an external component.

Most tests are mainly benchmark (but usually with some validating which allow
to ensure correct behavior).

Tests:

* squirreldb-cassandra-index-bench: Tests & benchmark Cassandra Index.


To run those test, you will need a Cassandra:

```
docker run -p 9042:9042 --name squirreldb-cassandra -d -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
```

Before each test, you should probably drop the keyspace:

```
docker exec squirreldb-cassandra cqlsh -e 'drop keyspace squirreldb_test'
```

Then run it with:
```
go run ./tests/squirreldb-cassandra-index-bench/
```
