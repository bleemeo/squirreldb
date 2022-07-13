## Build cache

Enable the cache to speed-up build and lint.
```sh
docker volume create squirreldb-buildcache
```

## Test and Develop

SquirrelDB needs a Cassandra database, you may run one with:

```sh
docker run -d --name squirreldb-cassandra -p 127.0.0.1:9042:9042 \
    -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
```

To build binary you can use the `build.sh` script. For example to
compile and run SquirrelDB, you can use:

```sh
./build.sh go
./squirreldb
```

SquirrelDB uses golangci-lint as linter. You may run it with:
```sh
./lint.sh
```

SquirrelDB has some tests that run using a real Cassandra (not like Go test which
mocks Cassandra). A helper shell script will start a Cassandra (using Docker) and run
those tests.
The script has options to run on cluster, run longer test and with race detector. 
The options can be combined:
```sh
./run-tests.sh race
./run-tests.sh cluster long
```

## Build a release

SquirrelDB uses Goreleaser and Docker to build its release, to build the release binaries
and Docker images run:
```sh
./build.sh
```

The resulting binaries can be found in the `dist/` folder and a Docker image named `squirreldb` is built.
