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

SquirrelDB uses Goreleaser and Docker buildx to build its release.

A builder needs to be created to build multi-arch images if it doesn't exist.
```sh
docker buildx create --name squirreldb-builder
```

### Test release

To do a test release, run:
```sh
export SQUIRRELDB_BUILDX_OPTION="--builder squirreldb-builder -t squirreldb:latest --load"

./build.sh
unset SQUIRRELDB_BUILDX_OPTION
```

The resulting binaries can be found in the `dist/` folder and a Docker image named `squirreldb` is built.

Release files are present in dist/ folder and a Docker image is build (squirreldb:latest) and loaded to
your Docker images.

### Production release

For production releases, you will want to build the Docker image for multiple architecture, which requires to
push the image into a registry. Set image tags ("-t" options) to the wanted destination and ensure you
are authorized to push to the destination registry:
```sh
export SQUIRRELDB_VERSION="$(date -u +%y.%m.%d.%H%M%S)"
export SQUIRRELDB_BUILDX_OPTION="--builder squirreldb-builder --platform linux/amd64,linux/arm64/v8,linux/arm/v7 -t squirreldb:latest -t squirreldb:${SQUIRRELDB_VERSION} --push"

./build.sh
unset SQUIRRELDB_VERSION SQUIRRELDB_BUILDX_OPTION
```
