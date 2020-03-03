# SquirrelDB

SquirrelDB is a scalable high-available timeseries database (TSDB) compatible with Prometheus remote storage.
SquirrelDB store data in Cassandra which allow to rely on Cassandra's scalability and availability.

## Build a release

SquirrelDB use goreleaser to build its release, to build the release binaries and Docker images run:

```
docker run --rm -u $UID:999 -e HOME=/go/pkg -e CGO_ENABLED=0 \
   -v $(pwd):/src -w /src \
   -v /var/run/docker.sock:/var/run/docker.sock \
   --entrypoint '' \
   goreleaser/goreleaser sh -c 'go test ./... && goreleaser --rm-dist --snapshot'
```

The resulting binaries are in dist/ folder and a Docker image named "squirreldb" is built

## Quickstart using Docker-compose

If you have Docker compose, you may run SquirrelDB, its Cassandra and Prometheus + Grafana
using docker-compose:

```
docker-compose up -d
```

Then, go to http://localhost:3000 and:

* Add a Prometheus datasource (using http://prometheus:9090 as URL)
* Create your dashboard or import a dashboard.


## Test and Develop

SquirrelDB require Golang 1.13. If your system does not provide it, you may run all Go command using Docker.
For example to run test:

```
GOCMD="docker run --net host --rm -ti -v $(pwd):/srv/workspace -w /srv/workspace -u $UID -e HOME=/tmp/home golang go"

$GOCMD test ./...
```

The following will assume "go" is golang 1.13 or more, if not replace it with $GOCMD.


SquirrelDB use golangci-lint as linter. You may run it with:

```
mkdir -p /tmp/golangci-lint-cache; docker run --rm -v $(pwd):/app -u $UID -v /tmp/golangci-lint-cache:/go/pkg -e HOME=/go/pkg -w /app golangci/golangci-lint:v1.23.7 golangci-lint run
```

SquirrelDB use Go tests, you may run them with:

```
go test squirreldb/... || echo "TEST FAILED"
```

SquirrelDB need a Cassandra database, you may run one with:

```
docker run -d --name squirreldb-cassandra -p 127.0.0.1:9042:9042 -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
```

Then run SquirrelDB from source:

```
go run squirreldb
```

### Note on VS code

SquirrelDB use Go module. VS code support for Go module require usage of gopls.
Enable "Use Language Server" in VS code option for Go.

To install or update gopls, use:

```
(cd /tmp; GO112MODULE=on go get golang.org/x/tools/gopls@latest)
```