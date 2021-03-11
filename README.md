# SquirrelDB

[![Go Report Card](https://goreportcard.com/badge/github.com/bleemeo/squirreldb)](https://goreportcard.com/report/github.com/bleemeo/squirreldb)
![Docker Image Version (latest by date)](https://img.shields.io/docker/v/bleemeo/squirreldb)
![Docker Image Size (latest by date)](https://img.shields.io/docker/image-size/bleemeo/squirreldb)
[![Open Source? Yes!](https://badgen.net/badge/Open%20Source%20%3F/Yes%21/blue?icon=github)](https://github.com/bleemeo/squirreldb/)

SquirrelDB is a scalable high-available timeseries database (TSDB) compatible with Prometheus remote storage.
SquirrelDB store data in Cassandra which allow to rely on Cassandra's scalability and availability.


## Quickstart using Docker-compose

If you have Docker compose, you may run SquirrelDB, its Cassandra and Prometheus + Grafana
using docker-compose:

```
docker-compose up -d
```

Then, go to http://localhost:3000 (default credentials are admin/admin) and:

* Add a Prometheus datasource (using http://squirreldb:9201 as URL)
* Create your dashboard or import a dashboard (for example import dashboard with ID 1860).


## HA setup

SquirrelDB allow both availability and scalability:

* The long term storage availability and scalability is done by a Cassandra cluster. Setup
  a Cassandra cluster and use a replication level > 1 (3 is recommended).
* The short term storage (by default last 15 minutes) which is store in-memory could be
  moved to Redis.
  A Redis cluster will provide availability and scalability of short term storage.
* When short term storage is provided by Redis, SquirrelDB instance are stateless,
  scalling them is just adding more of them behind a load-balancer (like nginx).

See [examples/squirreldb-ha](./examples/squirreldb_ha/) for a quickstart on HA setup.


## Build a release

SquirrelDB use goreleaser and Docker to build its release, to build the release binaries
and Docker images run:

```
# Optional, to speed-up subsequent build
mkdir -p .build-cache
./build.sh
```

The resulting binaries are in dist/ folder and a Docker image named "squirreldb" is built


## Test and Develop

SquirrelDB need a Cassandra database, you may run one with:

```
docker run -d --name squirreldb-cassandra -p 127.0.0.1:9042:9042 -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
```

To build binary you can use build.sh script. For example to just
compile Go binary (skip building Docker image and Windows installer):
```
mkdir -p .build-cache
./build.sh go
```

Then run SquirrelDB:

```
./squirreldb
```

SquirrelDB use golangci-lint as linter. You may run it with:
```
mkdir -p .build-cache  # enable cache and speed-up build/lint run
./lint.sh
```

SquirrelDB has some tests that run using a real Cassandra (not like Go test which
mock Cassandra). A helper shell script will start a Cassandra (using Docker) and run
those tests:
```
./run-tests.sh
```


### Note on VS code

SquirrelDB use Go module. VS code support for Go module require usage of gopls.
Enable "Use Language Server" in VS code option for Go.

To install or update gopls, use:

```
(cd /tmp; GO111MODULE=on go get golang.org/x/tools/gopls@latest)
```
