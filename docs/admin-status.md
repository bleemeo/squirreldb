# Admin status page

SquirrelDB serves a read-only status of the node on its admin endpoints. It answers
"is this node healthy, and if not why?" in one place, instead of combining `/ready`,
`/metrics`, `/debug/clickhouse/host` and the logs.

- [Access](#access)
- [The page](#the-page)
- [JSON report](#json-report)
- [Statuses](#statuses)
- [Sections](#sections)
- [Configuration redaction](#configuration-redaction)
- [Cost](#cost)
- [Security notes](#security-notes)

## Access

| Request                                 | Response                                       |
| --------------------------------------- | ---------------------------------------------- |
| `GET /debug/` from a browser            | the HTML status page                           |
| `GET /debug/` from curl                 | the plain text help of the debug endpoints     |
| `GET /debug/status`                     | the JSON report, whatever the `Accept` header  |
| `GET /debug/status?format=html`         | the HTML status page                           |

The rule for `/debug/`: the page is served when the `Accept` header lists `text/html`
(or `application/xhtml+xml`) explicitly, with a quality at least that of `text/plain`.
Browsers do; curl sends `Accept: */*` and gets the text help, unchanged.

Both are admin endpoints. When `admin_listen_address` is set they are served only on
that address (404 on `listen_address`). When authentication is enabled they require
the `admin` scope. See [authentication.md](authentication.md).

```sh
curl -u admin http://localhost:9202/debug/status
curl -u admin -s http://localhost:9202/debug/status | jq '.status, .reasons'
```

The response is always `200 OK` when the API is ready, even when the node is down:
the health is in the body (`status`), not in the HTTP status code. Use `/ready` for a
liveness/readiness probe.

## The page

The page shows an overall badge (healthy, degraded, down or unknown) with the reason
of every section that isn't ok, then one card per section: node, build, backends,
backend details (e.g. the ClickHouse hosts table and WAL), cluster, security (TLS,
certificate expiry, users, rejected requests) and the effective configuration
(collapsed, secrets redacted).

It is a single `html/template` response with an embedded stylesheet: no JavaScript, no
external resource (no CDN, no font), light and dark themes following the browser
(`prefers-color-scheme`). Auto-refresh is off by default; the "10s / 30s / 60s" links
add `?refresh=N`, which the page applies with a `<meta http-equiv="refresh">`.

The page links to the read-only endpoints (`/debug/status`, `/debug/pprof/`,
`/metrics`, `/debug/index_info`, the GET endpoints of the backend, ...). The endpoints
that change state, or that are expensive, are only listed as text (`POST /debug/flush`):
the page never contains a link or a form that changes the state of SquirrelDB.

## JSON report

The report is an object with one key per section. Fields are only ever added, never
renamed nor removed. A section that doesn't apply is omitted (e.g. `cassandra` with
the ClickHouse backend).

```json
{
  "status": "degraded",
  "reasons": ["backends: extra clickhouse: 1 of 2 ClickHouse hosts are ready"],
  "generated_at": "2026-10-01T11:39:44+02:00",
  "build": { "status": "ok", "version": "...", "commit": "...", "date": "...",
             "go_version": "go1.27.1", "start_time": "...", "uptime_seconds": 13.0, "uptime": "13s" },
  "node": { "status": "ok", "hostname": "squirreldb-1", "listen_address": "0.0.0.0:9201",
            "admin_listen_address": "localhost:9202", "ready": true,
            "telemetry_cluster_id": "...", "telemetry_node_id": "..." },
  "backends": {
    "status": "degraded", "reason": "extra clickhouse: 1 of 2 ClickHouse hosts are ready",
    "primary": { "name": "cassandra", "configured": true, "started": true, "status": "ok",
                 "info": { "apply_mutable_label_internally": true } },
    "extra": { "name": "clickhouse", "configured": true, "started": true, "status": "degraded",
               "reason": "1 of 2 ClickHouse hosts are ready" }
  },
  "cassandra": { "status": "ok", "details": { "...": "..." } },
  "clickhouse": { "status": "degraded", "reason": "...", "details": { "...": "..." } },
  "cluster": { "status": "ok", "kind": "redis", "size": 3, "details": { "redis_cluster": false, "size": 3 } },
  "security": { "status": "ok", "auth_enabled": true, "tls": { "...": "..." }, "users": [], "auth_failures": {} },
  "config": { "status": "ok", "values": { "...": "..." } }
}
```

Every section has the same header:

| Field    | Description                                                                         |
| -------- | ----------------------------------------------------------------------------------- |
| `status` | `ok`, `degraded`, `down` or `unknown`                                               |
| `reason` | short explanation of the status, set when it isn't ok (sometimes as information)    |
| `error`  | set when the collector of the section failed or timed out; other fields are missing |

The top-level `status` is the worst status of the `node`, `backends`, `cluster` and
`security` sections (and of `build` / `config` when their collector failed), and
`reasons` has one `<section>: <reason>` entry per section that isn't ok.

## Statuses

| Status     | Meaning                                                                          |
| ---------- | -------------------------------------------------------------------------------- |
| `ok`       | healthy                                                                          |
| `degraded` | working, but something needs attention (a host excluded, WAL backpressure, ...) |
| `down`     | not working (no reachable host, primary backend not started, ...)                |
| `unknown`  | the state could not be determined (collector timeout or error)                   |

The extra (dual-write) backend is best-effort: a problem on it never affects the
primary, so it makes the `backends` section at most `degraded`.

## Sections

### build and node

`build`: `version`, `commit`, `date` (build information), `go_version`, `start_time`,
`uptime_seconds` and `uptime`. `node`: `hostname`, `listen_address`,
`admin_listen_address` (omitted when the admin endpoints are on `listen_address`),
`ready`, and the telemetry `telemetry_cluster_id` / `telemetry_node_id` once the
telemetry acquired them (a few minutes after the start; omitted when it is disabled).

### backends

`primary` and `extra` summarize the storage backends: `name`, `configured`, `started`
(for the extra backend: attached, i.e. receiving the dual-writes), `status`, `reason`,
`info` (the backend behavior) and, for the extra backend, `unavailable_since`: when it
was first found unavailable at startup. Points written between that time and its
attachment were not dual-written; the extra backend is `down` while it isn't attached.
`extra.configured` is false when no `extra_write_backend` is set.

The details of each configured backend are in the section named after it, as
`details`. They come from the optional `types.StatusProvider` interface of the
backend; a backend without it is reported `unknown`.

### clickhouse

| Field          | Description                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------- |
| `database`, `cluster_name`, `read_only` | the configuration of the backend                                   |
| `hosts`        | one entry per configured host, from the routing health monitor (probed every 5 s)          |
| `query_limit`  | the query-concurrency limiter: `enabled`, `budget`, `read_budget`, `in_flight`, `in_flight_reads`, `in_flight_writes` |
| `wal`          | the write-ahead log, null until the TSDB is built                                           |

A host has `address`, `state` (`ready`: routed to, `reachable`: answers but excluded,
`down`), `reachable`, `ready`, `cause` (why it isn't ready: ping error, missing tables,
lagging replica), `last_probe` and `replica_lag_seconds` (the delay of its most-delayed
replica, measured by the probe). The section is `down` when no host is reachable, and
`degraded` when a host isn't ready.

The `wal` object has `storage` (`redis` or `memory`), `length` (buffered points),
`soft_threshold`, `critical_threshold` and `stop_threshold`, `drainer` (null on a
read-only node: `leader` tells whether this node drains the WAL, `last_drain`,
`last_error`, `last_error_at`, `flush_delay_seconds`) and `best_effort` (the drops of a
dual-write extra ClickHouse: `dropping`, `dropped_points`, and the repair range
`dropped_from` / `dropped_to`). It makes the section `degraded` when the WAL is above
the critical threshold (writes slowed down) or the stop threshold (writes blocked),
when this node is the drainer leader but did not empty the WAL for 6 flush delays, and
while best-effort points are dropped.

### cassandra

`addresses`, `keyspace`, `read_only`, `session_opened_at` and `nodes` (the number of
Cassandra nodes known by the coordinator). The connectivity is checked with the query
the connection health check uses (`system.local`) and a count of `system.peers`.

### dummy

`batcher`: whether writes go through the batcher before being discarded.

### cluster

`kind` is `redis` when the nodes coordinate through Redis, `local` for a single node
without Redis. `size` is the number of SquirrelDB nodes seen alive, including this one.
For Redis, the section pings Redis: `down` when it is unreachable (Redis also holds the
temporary store and the ClickHouse WAL when configured), and `details.redis_cluster`
tells whether Redis is a Redis Cluster.

### security

`auth_enabled`, `metrics_require_auth`, `tls` (`enabled`, `client_auth_type`,
`client_ca`, `min_version`, and the server `certificate`: `subject`, `dns_names`,
`not_before`, `not_after`, `expires_in_days`, read from `cert_file` on each request so
a renewed certificate shows up), `users` and `auth_failures` (the requests rejected
since the start, by reason, as in `squirreldb_api_auth_failures_total`). `notes` lists
remarks that don't change the status, e.g. "TLS is disabled".

A user has `name`, `scopes`, `tenant`, `methods` (`basic`, `bearer`) and
`has_forced_matcher`: never its password hash nor its token hash.

The section is `degraded` when the certificate can't be read or expires within 14 days,
and `down` when it expired.

### config

`values` is the effective configuration (files, environment and flags merged), with
the keys of the configuration file and the secrets redacted.

## Configuration redaction

The configuration is redacted on the server, before it is serialized:

- the fields tagged `sensitive:"true"` in `config/types.go` (passwords, password and
  token hashes, the Sentry DSN) are replaced by `<redacted>` when set, and shown empty
  when not set, so the page still tells whether a secret is configured;
- as a safety net for a secret added without the tag, a field or map key whose name
  contains `password`, `secret`, `token`, `hash`, `dsn`, `credential`, `private_key` or
  `api_key` is redacted too (a test checks that every such field has the tag);
- in every string, the credentials of a URL (`scheme://user:password@host`,
  `user:password@host`) and the query parameters with such names are redacted;
- a value of an unsupported kind is redacted.

When adding a secret to the configuration, tag it `sensitive:"true"`.

## Cost

Each section is collected concurrently with a 2 s timeout: a slow or failing collector
yields an `error` in its section (status `unknown`), never a failed or slow response.
The collectors read state that is already known (the routing health probes, the
drainer state, counters) or send requests no heavier than the health probes: one WAL
length read (`LLEN`), one Redis ping, and on Cassandra the `system.local` and
`system.peers` queries. Auto-refresh every 10 s is fine; don't scrape it as a metric
source, use `/metrics` for that.

## Security notes

- The page and the JSON report are read-only by design: a request never changes the
  state of SquirrelDB. Any future action (a button to flush, to block the index, ...)
  would need CSRF protection: browsers resend basic-auth credentials automatically to
  the site, so another site could make an authenticated admin's browser send the
  request. That is why the endpoints that change state are listed as text only.
- The responses have `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`,
  `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer` and a restrictive
  `Content-Security-Policy`: `default-src 'none'`, only the inline stylesheet allowed
  (by hash), `frame-ancestors 'none'`, `base-uri 'none'`, `form-action 'none'`.
- Every value is HTML-escaped by `html/template`.
- The report reveals the topology (hosts, addresses, users and scopes) and the
  redacted configuration: keep the admin endpoints on a private `admin_listen_address`
  and require authentication when it is reachable by others.
