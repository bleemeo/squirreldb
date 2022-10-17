<p align="center">
   <img src="logo.svg" alt="SquirrelDB" height="300"/>
</p>

# SquirrelDB

[![Go Report Card](https://goreportcard.com/badge/github.com/bleemeo/squirreldb)](https://goreportcard.com/report/github.com/bleemeo/squirreldb)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/bleemeo/squirreldb/blob/master/LICENSE)
[![Docker Image Version](https://img.shields.io/docker/v/bleemeo/squirreldb)](https://hub.docker.com/r/bleemeo/squirreldb/tags)
[![Docker Image Size](https://img.shields.io/docker/image-size/bleemeo/squirreldb)](https://hub.docker.com/r/bleemeo/squirreldb)

**SquirrelDB** is a scalable and highly available **timeseries database** (TSDB) compatible with **Prometheus remote storage**. Timeseries are stored in **Cassandra** to provide **scalability** and **availability**.

## Features

- Time Series Database with **Prometheus Long Term Storage**
- Support **pre-aggregation of data** for faster read
- Rely on **Cassandra, well known and reliable** NoSQL database
- Support **single node and cluster architectures**
- Expose **PromQL, remote read and write Prometheus endpoints**

## High Availability

SquirrelDB allows both **availability** and **scalability**:

* The **long term storage** availability and scalability is done by a **Cassandra cluster** by using a replication level > 1 (3 is recommended).
* The **short term storage** (by default last 15 minutes) is stored in-memory by default and can be configured to be stored in Redis. A **Redis cluster** will provide availability and scalability of short term storage.
* When short term storage is provided by Redis, SquirrelDB instance are **stateless**, scaling them is just adding more of them behind a load-balancer (like nginx).

Check out [examples/squirreldb-ha](./examples/squirreldb_ha/) for a highly available setup.

## Quickstart

You can run SquirrelDB easily with Cassandra, Prometheus, Grafana and Node Exporter using the provided docker-compose:

```sh
docker-compose up -d
```

Then go to the Grafana dashboard at http://localhost:3000/d/83ceCuenk/, and log in with the user "admin" 
and the password "password". You may need to wait a few minutes to see the graphs, because Cassandra starts slowly.

## Single node installation

### Cassandra

Cassandra must be running before starting SquirrelDB. If you don't have Cassandra, you can run it with Docker:
```sh
# The network is needed only if you run SquirrelDB with Docker.
docker network create squirreldb

docker run -d --name squirreldb-cassandra -p 127.0.0.1:9042:9042 \
    --net squirreldb -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra
```

### Binary

You can run SquirrelDB as a binary using the latest Github release for your platform at https://github.com/bleemeo/squirreldb/releases.

### Docker

You can use docker to run SquirrelDB:
```sh
docker run -d --name squirreldb -p 127.0.0.1:9201:9201 \
    --net squirreldb -e SQUIRRELDB_CASSANDRA_ADDRESSES=squirreldb-cassandra:9042 \
    bleemeo/squirreldb
```

## Configuration

The file `squirreldb.conf` contains all available configuration options. This file must be placed in the same directory as the SquirrelDB binary.

All configuration options can be overriden by environment variables. The environment variable is
- Prefixed by `SQUIRRELDB_`
- All letters are converted to uppercase
- Each yaml indentation is converted to an underscore

For example, cassandra.addresses becomes `SQUIRRELDB_CASSANDRA_ADDRESSES`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
