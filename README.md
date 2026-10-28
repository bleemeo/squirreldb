<p align="center">
   <img src="logo.svg" alt="SquirrelDB" height="300"/>
</p>

<h1 align="center">SquirrelDB</h1>

<p align="center">
  <b>Scalable, multi-tenant Prometheus long-term storage on ClickHouse or Cassandra.</b>
</p>

<p align="center">
  <a href="https://hub.docker.com/r/bleemeo/squirreldb"><img src="https://img.shields.io/docker/pulls/bleemeo/squirreldb" alt="Docker Pulls"></a>
  <a href="https://github.com/bleemeo/squirreldb/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://hub.docker.com/r/bleemeo/squirreldb/tags"><img src="https://img.shields.io/docker/v/bleemeo/squirreldb" alt="Docker Image Version"></a>
  <a href="https://hub.docker.com/r/bleemeo/squirreldb"><img src="https://img.shields.io/docker/image-size/bleemeo/squirreldb" alt="Docker Image Size"></a>
</p>

SquirrelDB is a stateless layer that speaks Prometheus (**remote write**, **remote read** and
**PromQL**) and stores your metrics in a database you already know how to run:
**ClickHouse** or **Cassandra**. Point Prometheus, Grafana Alloy or any remote-write agent at it,
point Grafana at it as a Prometheus datasource, and keep months or years of metrics without
operating a new kind of database.

It is built and run in production by [Bleemeo](https://bleemeo.com) to store the metrics of its
monitoring platform: many tenants, high write volume, long retention.

## Why SquirrelDB?

- **Drop-in for the Prometheus ecosystem**: remote write (v1 and v2), remote read, and the
  Prometheus HTTP query API (`query`, `query_range`, `series`, `labels`...). Grafana, alerting
  tools and API clients work unchanged.
- **Your choice of storage**: ClickHouse, recommended for new deployments, for compact columnar
  storage and fast scans; Cassandra (ScyllaDB is also tested) for a proven masterless cluster.
  Move from Cassandra to ClickHouse online, without data loss, with the built-in migration.
- **Fast queries over long ranges**: points are pre-aggregated, and a `query_range` with a large
  step automatically reads the aggregated data instead of the raw points.
- **Stateless and horizontally scalable**: with Redis holding the short-term buffer, SquirrelDB
  instances keep no state; add more of them behind a load balancer.
- **Multi-tenant**: one header (`X-SquirrelDB-Tenant`) isolates the reads and writes of each
  tenant, with per-request TTL and query limits.
- **Secure when exposed**: TLS, mTLS, basic auth and bearer tokens with `read` / `write` /
  `admin` scopes, users bound to a tenant or to a forced matcher, and a separate admin listener.
- **A smart gateway, not just a store**: route each request to a backend by header or by tenant,
  serve demo tenants from a built-in **sandbox** backend generating realistic data, and rename or
  regroup series at read time with **mutable labels**.
- **Easy to operate**: a single Go binary or Docker image, configuration by YAML, environment
  variables or flags, a `/debug/` status page telling why a node is unhealthy, Prometheus metrics
  on `/metrics`, and `/ready` for probes.

## How it works

```
 Prometheus / Alloy / agents                Grafana / alerting / API clients
          │  remote write                         │  PromQL, remote read
          ▼                                       ▼
   ┌───────────────────────────────────────────────────────────┐
   │ SquirrelDB  (stateless, behind a load balancer)           │
   │   auth & tenants · routing · PromQL engine · batching     │
   │   pre-aggregation · mutable labels                        │
   └──────┬───────────────────────────────┬────────────────────┘
          │ short-term buffer             │ long-term storage
          ▼                               ▼
   in-memory or Redis              ClickHouse  or  Cassandra
```

Incoming points are buffered (in memory, or in Redis to share it between instances), then
written to the long-term storage in batches: every few seconds for ClickHouse, by series for
Cassandra (where queries also read the buffer, so new points are visible immediately).

## Quickstart

The provided docker compose starts SquirrelDB with a single-node ClickHouse, Prometheus, Grafana
and Node Exporter:

```sh
docker compose up -d
```

Then open the Grafana dashboard at http://localhost:3000/d/83ceCuenk/ and log in with the user
`admin` and the password `password`. The graphs fill in as Prometheus scrapes and sends its
points to SquirrelDB.

For a highly available setup with ClickHouse, see
[examples/squirreldb_ha_clickhouse](examples/squirreldb_ha_clickhouse/).

Want to try the API without any database? Run the [sandbox backend](docs/sandbox.md), which
serves deterministic, realistic-looking data:

```sh
docker run -d --name squirreldb-sandbox -p 127.0.0.1:9201:9201 \
    -e SQUIRRELDB_BACKEND=sandbox -e SQUIRRELDB_LISTEN_ADDRESS=0.0.0.0:9201 \
    bleemeo/squirreldb

curl 'http://localhost:9201/api/v1/label/__name__/values'
```

## Installation

SquirrelDB is available as a [binary](https://github.com/bleemeo/squirreldb/releases) for each
platform and as a [Docker image](https://hub.docker.com/r/bleemeo/squirreldb). The storage must
be running before SquirrelDB starts.

The examples below use a Docker network so that SquirrelDB can reach its storage by name:

```sh
docker network create squirreldb
```

ClickHouse is the recommended storage for new deployments. Cassandra stays fully supported, and
existing Cassandra deployments can [migrate to ClickHouse](docs/clickhouse-migration.md) online.

### With ClickHouse (recommended)

```sh
docker run -d --name squirreldb-clickhouse --net squirreldb \
    -e CLICKHOUSE_DB=squirreldb -e CLICKHOUSE_USER=squirreldb -e CLICKHOUSE_PASSWORD=changeme \
    clickhouse/clickhouse-server:26.8

docker run -d --name squirreldb -p 127.0.0.1:9201:9201 --net squirreldb \
    -e SQUIRRELDB_BACKEND=clickhouse \
    -e SQUIRRELDB_CLICKHOUSE_ADDRESSES=squirreldb-clickhouse:9000 \
    -e SQUIRRELDB_CLICKHOUSE_DATABASE=squirreldb \
    -e SQUIRRELDB_CLICKHOUSE_USERNAME=squirreldb \
    -e SQUIRRELDB_CLICKHOUSE_PASSWORD=changeme \
    bleemeo/squirreldb
```

SquirrelDB creates its tables on start. The tested ClickHouse versions, and the single node,
single-node cluster and multi-node cluster deployments, are described in
[docs/clickhouse-cluster.md](docs/clickhouse-cluster.md).

### With Cassandra

```sh
docker run -d --name squirreldb-cassandra --net squirreldb \
    -e MAX_HEAP_SIZE=128M -e HEAP_NEWSIZE=24M cassandra

docker run -d --name squirreldb -p 127.0.0.1:9201:9201 --net squirreldb \
    -e SQUIRRELDB_CASSANDRA_ADDRESSES=squirreldb-cassandra:9042 \
    bleemeo/squirreldb
```

SquirrelDB creates its keyspace on start, with `cassandra.replication_factor` (1 by default, use
3 on a cluster of at least 3 nodes).

### Connect Prometheus and Grafana

Add SquirrelDB as remote storage in `prometheus.yml`:

```yaml
remote_write:
  - url: http://localhost:9201/api/v1/write
remote_read:
  - url: http://localhost:9201/api/v1/read
```

In Grafana, add a **Prometheus** datasource with the URL `http://localhost:9201`.

## High availability

SquirrelDB scales and stays available as long as each of its dependencies does:

- **Long-term storage**: a ClickHouse cluster (replicated tables, with ClickHouse Keeper) or a
  Cassandra cluster with a replication factor of 3.
- **Short-term buffer**: kept in memory by default. Configure `redis.addresses` to keep it in
  Redis instead; a Redis cluster makes it available and scalable.
- **SquirrelDB**: once the buffer is in Redis, instances are stateless. Run two or more behind a
  load balancer (e.g. nginx).

Complete examples:
[examples/squirreldb_ha_clickhouse](examples/squirreldb_ha_clickhouse/) and
[examples/squirreldb_ha](examples/squirreldb_ha/) (Cassandra).

## Configuration

SquirrelDB reads `squirreldb.conf` (YAML) from its working directory; use `--config` (`-c`) to
load other files or directories. [`squirreldb.conf`](squirreldb.conf) documents the options with
examples.

Every option can be overridden by an environment variable: prefix it with `SQUIRRELDB_`, convert
it to uppercase and replace each level of indentation with `_`. For example `cassandra.addresses`
becomes `SQUIRRELDB_CASSANDRA_ADDRESSES`. A few options can also be set by command-line flags:
`--listen_address`, `--admin_listen_address`, `--cassandra.addresses` and `--redis.addresses`.

### General

| Option                       | Default          | Description |
| ---------------------------- | ---------------- | ----------- |
| `backend`                    | `cassandra`      | Long-term storage: `clickhouse` (recommended for new deployments), `cassandra`, `sandbox` (generated data, no database) or `dummy` (discards everything, for load tests). |
| `extra_write_backend`        |                  | Second backend written best-effort, used to [migrate](docs/clickhouse-migration.md) from Cassandra to ClickHouse. |
| `listen_address`             | `localhost:9201` | Address of the Prometheus API. |
| `admin_listen_address`       |                  | When set, `/debug/` and `/mutable/` are served only on this address. |
| `default_time_to_live`       | `8760h` (1 year) | Retention of the points written without `X-SquirrelDB-TTL`. Applies to points written after a change. |
| `tenant_label_name`          | `__account_id`   | Label holding the tenant set by `X-SquirrelDB-Tenant`. |
| `require_tenant_header`      | `false`          | Reject the requests without `X-SquirrelDB-Tenant`. |
| `max_request_body_size`      | `64`             | Maximum size of a request body, in MiB. |
| `max_allowed_time_in_future` | `48h`            | Points further in the future may be rejected. |
| `batch.size`                 | `15m`            | How long points are buffered before being written to Cassandra. Must be the same on every SquirrelDB. |
| `remote_storage.max_concurrent_requests` | `0`  | Maximum concurrent remote reads, and as many remote writes. `0` means 2 × the number of CPUs. |
| `promql.max_evaluated_points` | `0`             | Abort a query evaluating more points. `0` means unlimited. |
| `promql.max_evaluated_series` | `0`             | Abort a query evaluating more series. `0` means unlimited. |
| `log.level`                  | `1`              | `-1` trace, `0` debug, `1` info, `2` warn, `3` error. |
| `log.disable_color`          | `false`          | Disable the colors in the logs. |
| `sentry.dsn`                 |                  | Send errors and panics to Sentry. |
| `telemetry.enabled`          | `true`           | Send anonymous telemetry to Bleemeo: SquirrelDB version, installation format, cluster size, OS, CPU and memory. |

### ClickHouse

| Option                              | Default          | Description |
| ----------------------------------- | ---------------- | ----------- |
| `clickhouse.addresses`              | `127.0.0.1:9000` | Native protocol endpoints. List every server: SquirrelDB health-checks each one and only queries the healthy ones. |
| `clickhouse.database`               | `default`        | Database holding the SquirrelDB tables. |
| `clickhouse.username`, `clickhouse.password` |         | Credentials. |
| `clickhouse.cluster_name`           |                  | Create replicated tables `ON CLUSTER` this cluster. Empty means a single node. See [docs/clickhouse-cluster.md](docs/clickhouse-cluster.md). |
| `clickhouse.max_concurrent_queries` | `0`              | Maximum in-flight queries sent to ClickHouse. `0` derives it from the server's `max_concurrent_queries`. |
| `clickhouse.wal_flush_delay`        | `0` (≈5s)        | How often the buffered points are written to ClickHouse. |
| `clickhouse.wal_soft_threshold`     | `0` (200k)       | Buffered points above which they are written early; writers are slowed down at 3× and blocked at 10×. |

### Cassandra

| Option                                     | Default          | Description |
| ------------------------------------------ | ---------------- | ----------- |
| `cassandra.addresses`                      | `127.0.0.1:9042` | Cassandra nodes. |
| `cassandra.keyspace`                       | `squirreldb`     | Keyspace, created on start if needed. |
| `cassandra.replication_factor`             | `1`              | Replication factor of a new keyspace. Use 3 on a cluster. |
| `cassandra.username`, `cassandra.password` |                  | Credentials. |
| `cassandra.cert_path`, `cassandra.key_path`, `cassandra.ca_path` | | TLS client certificate, key and CA. |
| `cassandra.enable_host_verification`       | `true`           | Verify the server certificate and hostname. |
| `cassandra.aggregate.intended_duration`    | `1m`             | Target duration of the pre-aggregation of one day of data, to throttle it. |

### Redis

| Option                                 | Default | Description |
| -------------------------------------- | ------- | ----------- |
| `redis.addresses`                      |         | Keep the short-term buffer in Redis, and coordinate the SquirrelDB instances. Several addresses (or a cluster-enabled node) use Redis cluster. |
| `redis.username`, `redis.password`     |         | Credentials. |
| `redis.keyspace`                       |         | Prefix of the keys, to share a Redis between several SquirrelDB clusters. |
| `redis.ssl`, `redis.ssl_insecure`      | `false` | Enable TLS, and skip the certificate verification. |
| `redis.cert_path`, `redis.key_path`, `redis.ca_path` | | TLS client certificate, key and CA. |

### Security

| Option                      | Default | Description |
| --------------------------- | ------- | ----------- |
| `tls.cert_file`, `tls.key_file` |     | Enable TLS on both listeners. Reloaded on each connection. |
| `tls.client_ca_file`        |         | CA verifying the client certificates (mTLS). |
| `tls.client_auth_type`      | `NoClientCert` | `RequestClientCert`, `RequireAnyClientCert`, `VerifyClientCertIfGiven` or `RequireAndVerifyClientCert`. |
| `tls.min_version`           | `TLS12` | `TLS10` to `TLS13`. |
| `auth.enabled`              | `false` | Require an authenticated user on every request but `/ready`. |
| `auth.metrics_require_auth` | `false` | Also require one on `/metrics`. |
| `auth.users`                | `[]`    | Users, each with a `name`, a `password_hash` (bcrypt) and/or a `token_sha256`, `scopes` (`read`, `write`, `admin`) and optionally a `tenant`, a `forced_matcher` and the `backends` it may use. Configuration files only. |

By default SquirrelDB serves plain HTTP without authentication and trusts the
`X-SquirrelDB-Tenant` header: only expose it to trusted clients, and it warns at startup when a
listener is reachable from the network without authentication. A good setup serves the admin
endpoints on `admin_listen_address: localhost:9202` and exposes `listen_address` with TLS and
authentication. See [docs/authentication.md](docs/authentication.md).

### Routing and sandbox

| Option                    | Default | Description |
| ------------------------- | ------- | ----------- |
| `routing.backends`        | `[]`    | Additional backends to start, selectable per request. |
| `routing.rules`           | `[]`    | Route the requests by tenant: a list of `tenants` glob patterns and a `backend`. The first match wins. |
| `sandbox.dataset_config`  |         | Dataset of the sandbox backend. Empty uses the embedded one. |

```yaml
backend: clickhouse
routing:
  backends: [sandbox]
  rules:
    - tenants: ["demo-*"]
      backend: sandbox
```

See [docs/routing.md](docs/routing.md) and [docs/sandbox.md](docs/sandbox.md).

## API

| Endpoint                                   | Description |
| ------------------------------------------ | ----------- |
| `POST /api/v1/write`                       | Prometheus remote write (v1 and v2). |
| `POST /api/v1/read`                        | Prometheus remote read. |
| `/api/v1/query`, `/api/v1/query_range`     | PromQL, like the [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/). |
| `/api/v1/series`, `/api/v1/labels`, `/api/v1/label/<name>/values` | Metadata queries. |
| `GET /metrics`                             | SquirrelDB's own metrics. |
| `GET /ready`                               | Readiness probe, never authenticated. |
| `/debug/`                                  | Status page in a browser, list of the debug endpoints with curl. `/debug/status` returns the same report in JSON. See [docs/admin-status.md](docs/admin-status.md). |
| `POST`/`DELETE /mutable/names`, `/mutable/values` | Manage the mutable labels. |

Endpoints that change state (`/debug/flush`, `/debug/preaggregate`...) should be called with
`POST`; `GET` still works but is deprecated.

Requests accept these headers:

| Header                              | Description |
| ----------------------------------- | ----------- |
| `X-SquirrelDB-Tenant`               | Read and write only the series of this tenant. |
| `X-SquirrelDB-TTL`                  | Retention of the points written by this request. |
| `X-SquirrelDB-Backend`              | Backend serving the request, among the routed ones. |
| `X-SquirrelDB-Forced-Matcher`       | Matcher added to every selector of the query, e.g. `env="prod"`. |
| `X-SquirrelDB-Max-Evaluated-Series`, `X-SquirrelDB-Max-Evaluated-Points` | Limits of this query. |
| `X-SquirrelDB-ForceRaw`, `X-SquirrelDB-ForcePreAggregated` | Read the raw or the pre-aggregated points, instead of choosing by the step. |
| `X-SquirrelDB-Query-Debug`, `X-SquirrelDB-Query-Verbose-Debug` | Log how the query is executed. |

## Commands

| Command                            | Description |
| ---------------------------------- | ----------- |
| `squirreldb` (or `squirreldb daemon`) | Run the server. |
| `squirreldb migrate`               | Copy the history from Cassandra to ClickHouse. See [docs/clickhouse-migration.md](docs/clickhouse-migration.md). |
| `squirreldb auth hash-password`    | Hash a password for `auth.users[].password_hash`. |
| `squirreldb auth gen-token`        | Generate a bearer token and its `token_sha256`. |

## Documentation

- [ClickHouse deployments and how to grow from one to the next](docs/clickhouse-cluster.md)
- [Migrating from Cassandra to ClickHouse](docs/clickhouse-migration.md)
- [TLS and authentication](docs/authentication.md)
- [Backend routing](docs/routing.md)
- [Sandbox backend](docs/sandbox.md)
- [Admin status page](docs/admin-status.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

SquirrelDB is licensed under the [Apache License 2.0](LICENSE).
