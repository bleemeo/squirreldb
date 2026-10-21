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
docker compose up -d
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

### Admin endpoints

SquirrelDB serves the Prometheus API (remote read/write, PromQL) on `listen_address`
(`localhost:9201` by default). By default the same address also serves the admin
endpoints: everything under `/debug/` (index inspection, flush, pre-aggregation, pprof,
backend operations such as `/debug/clickhouse/...`) and the `/mutable/` label API.
`curl http://localhost:9201/debug/` lists the debug endpoints.

To expose the Prometheus API without these endpoints, set `admin_listen_address`
(or `--admin_listen_address`, or `SQUIRRELDB_ADMIN_LISTEN_ADDRESS`) to another address,
e.g. `localhost:9202`. The admin endpoints are then served only on that address and
return 404 on `listen_address`. `/metrics` and `/ready` are served on both.

Endpoints that change state (e.g. `/debug/flush`, `/debug/preaggregate`) should be
called with `POST`. They still accept `GET` for backward compatibility, but this is
deprecated and logs a warning.

Open `/debug/` in a browser for a read-only status page telling whether the node is
healthy and, if not, why: backends, ClickHouse hosts and WAL, cluster, TLS and users,
and the effective configuration with its secrets redacted. The same report is available
as JSON on `/debug/status`. See [docs/admin-status.md](docs/admin-status.md).

### Security

By default SquirrelDB serves plain HTTP without authentication and trusts the
`X-SquirrelDB-Tenant` header: only expose it to trusted clients, or keep it on localhost.
To expose it, enable TLS (`tls.cert_file`, `tls.key_file`, optional mTLS) and
authentication (`auth.enabled` and `auth.users`): users authenticated by basic auth
(bcrypt) or bearer token (sha256), with `read`, `write` and `admin` scopes, optionally
bound to a tenant and restricted by a forced matcher. A good setup serves the admin
endpoints on `admin_listen_address: localhost:9202` and exposes `listen_address` with
TLS and authentication. `squirreldb auth hash-password` and `squirreldb auth gen-token`
generate the credentials.

SquirrelDB warns at startup when a listener is reachable from the network without
authentication. See [docs/authentication.md](docs/authentication.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
