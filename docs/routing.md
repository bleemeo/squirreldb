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

When [authentication](authentication.md) is enabled, the backends of the user
(`auth.users[].backends`) may restrict this choice, see
[below](#restricting-the-backends-of-a-user).

Every other per-request header (tenant, TTL, limits, forced matcher, debug)
applies unchanged to the selected backend. In particular the tenant still
restricts the series to the tenant, and the sandbox backend uses it to add the
tenant label to its series.

## Restricting the backends of a user

With [authentication](authentication.md#backends), a user may be restricted to some
backends with `backends`, each one being the default backend or one of
`routing.backends`:

```yaml
backend: clickhouse
routing:
  backends: [sandbox]
  rules:
    - tenants: ["load-*"]
      backend: clickhouse

auth:
  enabled: true
  users:
    - name: demo
      token_sha256: "<sha256 printed by squirreldb auth gen-token>"
      scopes: [read]
      backends: [sandbox]
    - name: dashglass
      password_hash: "<hash printed by squirreldb auth hash-password>"
      scopes: [read]
```

The backend of a request of a restricted user is resolved like any other request,
then checked against its backends:

| Resolved by                         | Backend in `backends`  | Backend not in `backends`                     |
| ----------------------------------- | ---------------------- | --------------------------------------------- |
| `X-SquirrelDB-Backend` header       | that backend           | `403 Forbidden`                               |
| header naming a disabled backend    | `400 Bad Request`      | `400 Bad Request`                             |
| first routing rule matching tenant  | the rule's backend     | `403 Forbidden`                               |
| default (no header, no rule)        | the default backend    | the **first** backend of `backends` (`user`)  |

An unrestricted user (no `backends`), or any request when authentication is disabled,
is routed exactly as described above. With the configuration above:

| User        | Request                                   | Backend                          |
| ----------- | ----------------------------------------- | -------------------------------- |
| `demo`      | no header                                 | sandbox (first of its backends)  |
| `demo`      | `X-SquirrelDB-Backend: sandbox`           | sandbox                          |
| `demo`      | `X-SquirrelDB-Backend: clickhouse`        | 403                              |
| `demo`      | `X-SquirrelDB-Tenant: load-1` (rule)      | 403                              |
| `dashglass` | no header                                 | clickhouse (default)             |
| `dashglass` | `X-SquirrelDB-Backend: sandbox`           | sandbox                          |

The 403 is counted by `squirreldb_api_auth_failures_total{reason="backend_not_allowed"}`,
and the error message names the backend the user isn't allowed to use. For a restricted
user, the error of a header naming a disabled backend only lists the user's backends.

The restriction applies to reads and writes alike: a restricted user with the `write`
scope only writes to its backends.

```sh
# The demo token reads the sandbox data without any routing header.
curl -H "Authorization: Bearer $DEMO_TOKEN" 'http://localhost:9201/api/v1/query?query=agent_status'
```

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
source of the decision: `header`, `rule`, `default`, or `user` for a request
of a restricted user served by its first backend instead of the default one. It's
only exported when routing is configured.

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

Without authentication, the `X-SquirrelDB-Backend` header lets any client of
SquirrelDB pick any enabled backend, for reads and writes. Only enable backends
that are safe to expose to every client, or enable
[authentication](authentication.md) and restrict the backends of the users which
must not use every backend (see
[above](#restricting-the-backends-of-a-user)).
