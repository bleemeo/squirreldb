# TLS and authentication

By default SquirrelDB serves plain HTTP without authentication, and trusts the
`X-SquirrelDB-Tenant` header sent by its clients. This is fine when SquirrelDB only
listens on localhost or on a private network reachable only by trusted clients. When
SquirrelDB must be reachable by other clients (Prometheus, Alloy or Glouton sending
remote write, Grafana or Dashglass dashboards), enable TLS and authentication.

Both are optional and disabled by default: an existing configuration keeps working
unchanged. SquirrelDB warns at startup when a listener is reachable from the network
(any address other than loopback, e.g. `0.0.0.0:9201`) without authentication, or with
authentication but without TLS (passwords and tokens would be sent in clear text).

- [TLS](#tls)
- [Users](#users)
- [Scopes](#scopes)
- [Tenant binding](#tenant-binding)
- [Forced matcher](#forced-matcher)
- [Admin listener](#admin-listener)
- [Client configuration](#client-configuration)
- [Recommended setup](#recommended-setup)

## TLS

```yaml
tls:
  cert_file: /etc/squirreldb/tls/server.pem
  key_file: /etc/squirreldb/tls/server-key.pem
  # Optional, to require client certificates (mTLS).
  # client_ca_file: /etc/squirreldb/tls/client-ca.pem
  # client_auth_type: RequireAndVerifyClientCert
  # min_version: TLS12
```

TLS is enabled when `cert_file` is set, and applies to both `listen_address` and
`admin_listen_address`. The options have the same meaning as the `tls_server_config` of
the [Prometheus web configuration file](https://prometheus.io/docs/prometheus/latest/configuration/https/),
and SquirrelDB uses the same implementation (prometheus/exporter-toolkit):

| Option             | Description                                                                                                                                           |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cert_file`        | Server certificate (PEM), may contain the intermediate certificates.                                                                                  |
| `key_file`         | Private key of the certificate (PEM).                                                                                                                 |
| `client_ca_file`   | CA used to verify the client certificates. Requires `client_auth_type`.                                                                               |
| `client_auth_type` | `NoClientCert` (default), `RequestClientCert`, `RequireAnyClientCert`, `VerifyClientCertIfGiven` or `RequireAndVerifyClientCert`.                     |
| `min_version`      | Minimum TLS version: `TLS10`, `TLS11`, `TLS12` (default) or `TLS13`.                                                                                  |

The files are checked when the configuration is loaded: SquirrelDB doesn't start if
they can't be read or if the key doesn't match the certificate. Relative paths are
relative to the working directory of SquirrelDB. The certificate, the key and the client
CA are read again on each new TLS connection: a renewed certificate (e.g. by cert-manager
or certbot) is used without restarting SquirrelDB.

A client certificate only proves that the client holds a certificate signed by the CA,
it doesn't identify a user: with mTLS, the requests still need credentials when
authentication is enabled.

## Users

```yaml
auth:
  enabled: true
  # /metrics is public unless this is true. /ready is always public.
  metrics_require_auth: false
  users:
    - name: glouton-acme
      token_sha256: "4b0e...c51a"
      tenant: acme
      scopes: [write]
    - name: dashglass
      password_hash: "$2a$10$..."
      scopes: [read]
    - name: ops
      password_hash: "$2a$10$..."
      scopes: [admin]
```

When `auth.enabled` is true, every request needs credentials, except `/ready` and (unless
`metrics_require_auth` is true) `/metrics`. SquirrelDB doesn't start if no user is
configured. A user is authenticated with one of:

- **Basic auth** (`Authorization: Basic ...`): the username is `name`, the password is
  checked against `password_hash`, a bcrypt hash. To avoid paying the bcrypt cost on each
  request, a successful verification is remembered in memory for one minute.
- **Bearer token** (`Authorization: Bearer <token>`): the sha256 of the token is compared
  to `token_sha256`. Tokens must be random and long (use `squirreldb auth gen-token`),
  which is why a fast hash is enough for them.

A user needs at least one of `password_hash` and `token_sha256`, and may have both.
Generate them with the `auth` subcommand:

```sh
# Prints the bcrypt hash of the password read on stdin.
printf '%s' 'my password' | squirreldb auth hash-password
# Or, to avoid the password in the shell history:
read -rs PASSWORD && printf '%s' "$PASSWORD" | squirreldb auth hash-password

# Prints a random token and its sha256: give the token to the client and
# put the sha256 in token_sha256.
squirreldb auth gen-token
```

With Docker: `docker run --rm -i bleemeo/squirreldb /usr/sbin/squirreldb auth gen-token`. Any bcrypt hash
works, for example the ones generated by `htpasswd -nB user`.

A request without valid credentials is rejected with `401 Unauthorized` and a
`WWW-Authenticate: Basic realm="SquirrelDB"` header. An authenticated request to a
route its user has no scope for, or with a tenant header different from the user's
tenant, is rejected with `403 Forbidden`. The rejections are counted by the metric
`squirreldb_api_auth_failures_total{reason}`, with the reasons `missing_credentials`,
`malformed_credentials`, `invalid_credentials`, `missing_scope` and `tenant_mismatch`.
Credentials are never logged; set the log level to debug to log the rejected requests.

`users` can only be set in configuration files. Unlike the other options, it can't be
set by an environment variable or a flag (`SQUIRRELDB_AUTH_USERS` is not supported).
`auth.enabled`, `auth.metrics_require_auth` and the `tls` options can be set by
environment variables, e.g. `SQUIRRELDB_AUTH_ENABLED=true` or `SQUIRRELDB_TLS_CERT_FILE`.
When configuration is split in multiple files (e.g. a directory of `.conf` files), the
users of all files are concatenated; user names must be unique.

Unknown keys under `auth` and `tls`, and unknown `SQUIRRELDB_AUTH_*` and `SQUIRRELDB_TLS_*`
environment variables, stop SquirrelDB at startup instead of only logging a warning
like elsewhere in the configuration: a typo such as `tenat` instead of `tenant` would
otherwise leave a user without its restriction.

## Scopes

| Scope   | Routes                                                                                                                                                                                                                                                                                                                                        |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `read`  | The Prometheus read API under `/api/v1/`: `query`, `query_range`, `query_exemplars`, `format_query`, `parse_query`, `series` (GET and POST), `labels`, `label/<name>/values`, `metadata`, `read` (remote read), `status/*`, and the stubs `targets`, `targets/metadata`, `scrape_pools`, `alertmanagers`, `alerts`, `rules`. |
| `write` | Remote write `/api/v1/write` and OTLP write `/api/v1/otlp/v1/metrics`.                                                                                                                                                                                                                                                                        |
| `admin` | The admin endpoints: everything under `/debug/` (including the backend ones such as `/debug/clickhouse/...` and pprof) and `/mutable/`.                                                                                                                                                                                                       |

Scopes don't imply each other: a user that both writes and reads needs `[read, write]`,
and `admin` doesn't grant `read`.

Any route not listed above requires the `admin` scope, including routes that a future
version would add and paths that don't exist (an unauthorized client gets a 401 or 403
rather than a 404). For instance `DELETE /api/v1/series` and `/api/v1/admin/...` require
`admin`. Paths are matched exactly: a non-canonical path (e.g. with a trailing slash)
requires `admin`.

A CORS preflight request (`OPTIONS`) has no credentials, so it is rejected when
authentication is enabled: browsers can't query SquirrelDB directly, use a server-side
proxy such as the Grafana datasource proxy.

## Tenant binding

Without `tenant`, a user sends the `X-SquirrelDB-Tenant` header like today: it can
read and write any tenant. Use this for trusted multi-tenant clients (e.g. a service
that serves all tenants).

With `tenant`, the user is bound to this tenant:

- Requests without the tenant header are processed as if they had
  `X-SquirrelDB-Tenant: <tenant>`: written points get the tenant label
  (`tenant_label_name`, `__account_id` by default) and queries only match the series of
  this tenant.
- Requests with the same tenant header are accepted, requests with another tenant are
  rejected with `403 Forbidden`.

A user with a `tenant` can't have the `admin` scope, because the admin endpoints are not
restricted to a tenant (e.g. `/debug/index_dump` lists every series). `tenant_label_name`
must not be empty when a user has a `tenant`.

## Forced matcher

`forced_matcher` restricts the series a user can read (it has no effect on writes),
like the `X-SquirrelDB-Forced-Matcher` header. It accepts PromQL label matchers, with or
without braces, or the format of the header:

```yaml
forced_matcher: 'env="prod"'
forced_matcher: '{env="prod", team=~"web|db"}'
forced_matcher: 'env=prod'   # Header format: one equality matcher.
```

The matchers of the user are added to the ones of the request: if the request also has
an `X-SquirrelDB-Forced-Matcher` header, both apply (AND), the header can't replace or
widen the user's matcher. A user with a `forced_matcher` can't have the `admin` scope.

## Admin listener

Authentication applies to both listeners:

- Without `admin_listen_address`, `listen_address` serves the read, write and admin
  routes, each with its scope.
- With `admin_listen_address`, every route of the admin listener requires the `admin`
  scope, except `/ready` and `/metrics`. The admin routes still require `admin` on
  `listen_address` before returning 404.

## Client configuration

The examples use `https://squirreldb.example.com:9201`, a TLS certificate signed by a
private CA, a user `prometheus-acme` with the `write` scope and a user `grafana` with
the `read` scope.

Prometheus remote write, with basic auth or a bearer token:

```yaml
remote_write:
  - url: https://squirreldb.example.com:9201/api/v1/write
    basic_auth:
      username: prometheus-acme
      password_file: /etc/prometheus/squirreldb-password
    tls_config:
      ca_file: /etc/prometheus/squirreldb-ca.pem

  - url: https://squirreldb.example.com:9201/api/v1/write
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/squirreldb-token
    # For a user without tenant, send the tenant header.
    headers:
      X-SquirrelDB-Tenant: acme
```

Grafana Alloy:

```alloy
prometheus.remote_write "squirreldb" {
  endpoint {
    url          = "https://squirreldb.example.com:9201/api/v1/write"
    bearer_token = sys.env("SQUIRRELDB_TOKEN")

    tls_config {
      ca_file = "/etc/alloy/squirreldb-ca.pem"
    }
  }
}
```

Any other remote write client (e.g. Glouton) is configured the same way: the remote
write URL, basic auth or a bearer token, and the CA if the certificate is not signed by
a public CA.

Grafana, Dashglass or any client using a Prometheus datasource: use the URL
`https://squirreldb.example.com:9201`, enable basic authentication with the user and
password, and add the CA certificate (or "TLS client auth" settings for mTLS). For a user
without `tenant`, add the custom HTTP header `X-SquirrelDB-Tenant` with the tenant to
query; a tenant-bound user doesn't need it.

curl:

```sh
curl --cacert ca.pem -u grafana:password 'https://squirreldb.example.com:9201/api/v1/query?query=up'
curl --cacert ca.pem -H "Authorization: Bearer $TOKEN" -XPOST https://squirreldb.example.com:9202/debug/flush
```

## Recommended setup

- Serve the admin endpoints on a separate listener bound to localhost, so they are only
  reachable from the host (or through an SSH tunnel), and give `admin` only to the users
  that operate SquirrelDB.
- Expose `listen_address` with TLS and authentication.
- Give each client its own user with the fewest scopes: `write` for the remote write
  clients, `read` for the dashboards, and bind the users of a single tenant to it.
- Prefer bearer tokens for machines: they are random, and cheap to verify.

```yaml
listen_address: "0.0.0.0:9201"
admin_listen_address: "localhost:9202"

tls:
  cert_file: /etc/squirreldb/tls/server.pem
  key_file: /etc/squirreldb/tls/server-key.pem

auth:
  enabled: true
  users:
    - name: prometheus-acme
      token_sha256: "<sha256 printed by squirreldb auth gen-token>"
      tenant: acme
      scopes: [write]
    - name: grafana-acme
      password_hash: "<hash printed by squirreldb auth hash-password>"
      tenant: acme
      scopes: [read]
    - name: ops
      password_hash: "<hash printed by squirreldb auth hash-password>"
      scopes: [admin]
```
