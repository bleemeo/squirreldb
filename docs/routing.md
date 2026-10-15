# Backend routing

A single SquirrelDB process can serve several backends: the backend serving a
request is selected per request, by an HTTP header sent by the caller or by
routing rules on the tenant. A typical use is serving demo accounts from the
[sandbox backend](sandbox.md) and every other account from ClickHouse, from the
same SquirrelDB.

## Configuration

```yaml
# The default route: the backend serving every request not routed elsewhere.
backend: cassandra
# Dual-write applies to the writes to the default backend only (see below).
extra_write_backend: clickhouse

routing:
  # Additional backends to start and make selectable. The default backend
  # ("backend") is always selectable and must not be listed here.
  backends: [sandbox]
  # Rules route the requests without the X-SquirrelDB-Backend header by tenant.
  # They are evaluated in order, the first matching rule wins.
  rules:
    - tenants: ["demo-*", "1234"]
      backend: sandbox
```

- `routing.backends` lists the routed backends: `cassandra`, `clickhouse`,
  `dummy` or `sandbox`. Each one is started when SquirrelDB starts, like the
  default backend, and a routed backend failing to start makes SquirrelDB fail
  to start. A backend is configured by its usual section (`clickhouse:`,
  `sandbox:`, ...), whether it's the default backend or a routed one.
  The list can be set with an environment variable:
  `SQUIRRELDB_ROUTING_BACKENDS=sandbox`.
- `routing.rules[].tenants` are glob patterns matched against the tenant
  (`X-SquirrelDB-Tenant` header), with the Go
  [`path.Match`](https://pkg.go.dev/path#Match) syntax: `*` matches any
  sequence of characters except `/`, `?` matches a single character except `/`,
  `[...]` matches a character class (e.g. `[0-9]`), `\` escapes a special
  character. A request without tenant never matches a rule.
- `routing.rules[].backend` must be the default backend or one of
  `routing.backends`. Routing rules can only be set in a configuration file.

The configuration is rejected when:

- a backend name is unknown;
- `routing.backends` contains the default backend, the `extra_write_backend`,
  or the same backend twice (a backend is started once, either as the
  dual-write target of the default route or as a routed backend);
- a rule has no tenant, an invalid pattern, or a backend which is neither the
  default backend nor in `routing.backends`.

Without a `routing` section (the default), SquirrelDB behaves exactly as without
this feature: every request goes to the default backend, and no routed backend
is started.

## Selecting the backend of a request

The backend of a request, for reads (PromQL, remote read) and writes (remote
write) alike, is:

1. the backend named by the `X-SquirrelDB-Backend` header, if present;
2. else the backend of the first rule matching the tenant of the request;
3. else the default backend (`backend`).

The header must name a backend enabled by the configuration, the default
backend or one of `routing.backends`, else the request fails with a status
400 and an error listing the enabled backends. The value is case sensitive.
Without a `routing` section, the header may only name the default backend.

Every other per-request header (tenant, TTL, limits, forced matcher, debug)
applies unchanged to the selected backend. In particular the tenant still
restricts the series to the tenant, and the sandbox backend uses it to add the
tenant label to its series.

## Writes

A write is sent only to the backend selected for the request:

- When the default backend is selected, whether by the `X-SquirrelDB-Backend`
  header, by a rule or by default, the write goes to the default backend and,
  when `extra_write_backend` is set, is dual-written to the extra backend, as
  without routing. The extra backend can't be selected on its own.
- When a routed backend is selected, the write goes to that backend only: it's
  never dual-written. A write routed to the sandbox backend is accepted and
  discarded.

`internal.read_only` makes every backend read-only.

## What stays bound to the default backend

Some features are not routed and always use the default backend:

- The mutable labels API (`/mutable/names`, `/mutable/values`) and the
  `/debug/mutable_*` endpoints manage the mutable labels of the default backend.
  A routed backend applies its own mutable labels when reading (the sandbox
  backend keeps them in memory, so they are empty), and the detection of the
  mutable labels to ignore on write uses the default backend's.
- Telemetry, the cluster coordination (`redis`), and the `/debug/` index,
  flush and pre-aggregation endpoints.
- The backend-specific debug endpoints are registered for each backend under
  its own prefix, `/debug/<backend>/` (only Cassandra and ClickHouse have some).

The per-request backend options parsed from headers (such as
`X-SquirrelDB-ForceRaw`) are parsed by every selectable backend, and each
backend receives the options it parsed.

## Metrics

`squirreldb_routing_requests_total{backend, operation, source}` counts the
routed reads (`operation="read"`, one per querier, so roughly one per query) and
writes (`operation="write"`, one per remote write request), by backend and by
source of the decision: `header`, `rule` or `default`. It's only exported when
routing is configured.

The routed backends export their own metrics (e.g.
`squirreldb_sandbox_requests_points`). The metrics of components shared with the
default backend, such as the mutable label cache, are only exported for the
default backend.

## Demo use case

Bleemeo serves its demo servers from the sandbox backend. Two ways of doing it
with a single SquirrelDB:

- The caller selects the backend: the Bleemeo API (or Dashglass) sends
  `X-SquirrelDB-Backend: sandbox` on the queries of demo servers, with the usual
  `X-SquirrelDB-Tenant` header.

  ```yaml
  backend: clickhouse
  routing:
    backends: [sandbox]
  ```

- SquirrelDB selects the backend: the demo accounts are routed to the sandbox by
  a rule, with no change on the caller side.

  ```yaml
  backend: clickhouse
  routing:
    backends: [sandbox]
    rules:
      - tenants: ["demo-*"]
        backend: sandbox
  ```

```sh
curl -H 'X-SquirrelDB-Backend: sandbox' -H 'X-SquirrelDB-Tenant: 1234' \
  'http://localhost:9201/api/v1/query?query=agent_status'
```

## Security

The `X-SquirrelDB-Backend` header lets any client of SquirrelDB pick any enabled
backend, for reads and writes. Only enable backends that are safe to expose to
every client. A future authentication feature will allow restricting the
backends an identity may select.
