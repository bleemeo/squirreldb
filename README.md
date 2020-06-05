# SquirrelDB

SquirrelDB is a scalable high-available timeseries database (TSDB) compatible with Prometheus remote storage.
SquirrelDB store data in Cassandra which allow to rely on Cassandra's scalability and availability.

## Build a release

SquirrelDB use goreleaser to build its release, to build the release binaries and Docker images run:

```
docker run --rm -u $UID:`getent group docker|cut -d: -f 3` -e HOME=/go/pkg -e CGO_ENABLED=0 \
   -v $(pwd):/src -w /src \
   -v /var/run/docker.sock:/var/run/docker.sock \
   --entrypoint '' \
   goreleaser/goreleaser:v0.137 sh -c 'go test ./... && goreleaser --rm-dist --snapshot'
```

The resulting binaries are in dist/ folder and a Docker image named "squirreldb" is built

## Quickstart using Docker-compose

If you have Docker compose, you may run SquirrelDB, its Cassandra and Prometheus + Grafana
using docker-compose:

```
docker-compose up -d
```

Then, go to http://localhost:3000 (default credentials are admin/admin) and:

* Add a Prometheus datasource (using http://squirreldb:9090 as URL)
* Create your dashboard or import a dashboard (for example import dashboard with ID 1860).


## Test and Develop

SquirrelDB require Golang 1.13. If your system does not provide it, you may run all Go command using Docker.
For example to run test:

```
GOCMD="docker run --net host --rm -ti -v $(pwd):/srv/workspace -w /srv/workspace -u $UID -e HOME=/tmp/home golang go"

$GOCMD test ./...
```

The following will assume "go" is golang 1.13 or more, if not replace it with $GOCMD or use an alias:
```
alias go=$GOCMD
```


SquirrelDB use golangci-lint as linter. You may run it with:

```
mkdir -p /tmp/golangci-lint-cache; docker run --rm -v $(pwd):/app -u $UID -v /tmp/golangci-lint-cache:/go/pkg -e HOME=/go/pkg -w /app golangci/golangci-lint:v1.27 golangci-lint run
```

SquirrelDB use Go tests, you may run them with:

```
go test ./... || echo "TEST FAILED"
```

SquirrelDB need a Cassandra database, you may run one with:

```
docker run -d --name squirreldb-cassandra -p 127.0.0.1:9042:9042 -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
```

Then run SquirrelDB from source:

```
go run squirreldb
```

## HA setup

SquirrelDB allow both availability and scalability:

* The long term storage availability and scalability is done by a Cassandra cluster. Setup
  a Cassandra cluster and use a replication level > 1 (3 is recommended).
* The short term storage (by default last 15 minutes) which is store in-memory could be
  moved to Redis.
  A Redis cluster will provide availability and scalability of short term storage.
* When short term storage is provided by Redis, SquirrelDB instance are stateless,
  scalling them is just adding more of them behind a load-balancer (like nginx).

See examples/squirreldb-ha for a quickstart on HA setup.

### Note on VS code

SquirrelDB use Go module. VS code support for Go module require usage of gopls.
Enable "Use Language Server" in VS code option for Go.

To install or update gopls, use:

```
(cd /tmp; GO111MODULE=on go get golang.org/x/tools/gopls@latest)
```
