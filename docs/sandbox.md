# Sandbox backend

The `sandbox` backend turns SquirrelDB into a "fake" SquirrelDB: it provides the
same PromQL, remote read and remote write endpoints, but instead of returning
data that was stored, it serves generated data that looks plausible. It needs
no Cassandra, ClickHouse or Redis.

It is meant for demos, and for developing or testing tools that read from
SquirrelDB (dashboards, alerting, API clients) without real data.

The generated data is **deterministic and stateless**: the same series at the
same timestamp always has the same value, on every SquirrelDB instance and
after any restart.

## Usage

Select the backend in the configuration:

```yaml
backend: sandbox

sandbox:
  # Path to a dataset definition file. Empty (the default) uses the dataset
  # embedded in SquirrelDB.
  dataset_config: ""
```

or with environment variables:

```sh
SQUIRRELDB_BACKEND=sandbox ./squirreldb
SQUIRRELDB_BACKEND=sandbox SQUIRRELDB_SANDBOX_DATASET_CONFIG=/path/to/dataset.yaml ./squirreldb
```

The dataset is loaded when SquirrelDB starts; an invalid dataset makes the start
fail. Series that have no matching samples are dropped with a warning in the log.

Behavior to be aware of:

- **Writes are accepted and discarded.** Remote write requests succeed, but the
  points are never returned. For this reason `sandbox` can't be used as
  `extra_write_backend`.
- **Every tenant sees the same dataset.** The dataset series have no tenant label.
  When a request has the `X-SquirrelDB-Tenant` header, the tenant label
  (`tenant_label_name`, `__account_id` by default) is added to every returned
  series, label name and label value, so the tenant restriction matches them.
  Without the header, the series are returned as they are in the dataset.
- **Mutable labels** work as with the other backends, but they are stored in
  memory only and lost on restart.
- The backend exposes two histograms: `squirreldb_sandbox_requests_points` and
  `squirreldb_sandbox_requests_series`, the number of points read and series
  returned by each query.

## Dataset

A dataset aims to produce data that looks real:

- It uses samples dumped from a real Prometheus or SquirrelDB. The dumped samples
  are repeated forever.
- It has a list of the series (metric name and labels) that should exist.
- Each series is associated with dumped samples. By default only the metric name
  must match between a series and its samples. Additional labels that must match
  can be listed (e.g. the `mode` of `node_cpu_seconds_total` must match:
  `mode="idle"` isn't the same as `mode="system"`). The other labels are used to
  pick one of the matching samples and to offset them in time, which produces
  more variation when several series use the same samples.
- Related metrics should be dumped in the same file and with the same labels
  (e.g. network packets and network bytes of `device="eth0"`): related series
  then use related samples at the same time.

Besides recorded samples, a series may be defined by a PromQL expression
evaluated at each step, e.g. for a certificate that always expires in 30 days.

### Definition file

```yaml
files:
  # Each metrics file contains one series per line, in PromQL syntax
  # (e.g. cpu_used{instance="server1:8015"}). Empty lines and lines starting
  # with # are ignored.
  metrics:
    - file: ./bleemeo-agent.txt
    - file: ./bleemeo-probe.txt
      # Labels that must match between the series and the dumped samples, in
      # addition to __name__ (which must always match).
      matching_labels:
        - item
        - scraper
        - phase
  # Samples files, as produced by squirreldb-sandbox-dump. If several samples
  # match a series, one is picked based on a hash of the series labels, and
  # this pick never changes. Samples can also be hand-made.
  samples:
    - file: ./bleemeo-agent-samples.json
    - file: ./bleemeo-probe-samples.json

promql:
  # A series whose value is the result of query, evaluated every step_seconds.
  # The query must return a scalar or a single sample; it may use the series
  # defined in "files". A PromQL series replaces a "files" series with the
  # same labels.
  - name: probe_domain_expiry{instance="sandbox.bleemeo.com:8015"}
    step_seconds: 10
    query: "time() + 86400*30"
```

Relative paths are resolved relative to the directory of the definition file.
The embedded dataset is in [sandbox/dataset/default](../sandbox/dataset/default/);
copying it is a good starting point for a custom dataset.

A samples file is a stream of JSON objects, one per recorded series:

```json
{"labels":{"__name__":"cpu_used"},"step_seconds":10,"samples":["1.74","1.47","1.24"]}
```

### How values are computed

A series with samples `s[0..n-1]` recorded every `step_seconds` has a point at
every multiple of `step_seconds` since the Unix epoch. The value at timestamp
`ts` is `s[(offset + ts / step) % n]`, where `offset` is a hash of the series
labels (without the matching labels) modulo `n`. Points are aligned on the step
whatever the query range, so two queries always agree.

### Building a dataset

The easiest way to produce samples is `squirreldb-sandbox-dump`, which queries
an existing SquirrelDB (or any Prometheus API) for every metric of a metrics
file. For example:

```sh
go run ./cmd/squirreldb-sandbox-dump \
    -promql-url http://localhost:9201/api/v1/query_range \
    -tenant-header-value 1234 \
    -start 2021-03-28T08:00:00Z \
    -end 2021-03-28T13:59:59Z \
    -metric-file ./sandbox/dataset/default/bleemeo-agent.txt \
    > ./sandbox/dataset/default/bleemeo-agent-samples.json
```

Run it with `-help` for the other options (additional matchers, labels to drop
or override, step, maximum number of series dumped per metric).

To apply some simple modifications (like dropping NaN values), use
`squirreldb-sandbox-samples`:

```sh
go run ./cmd/squirreldb-sandbox-samples \
    -drop-nan < ./sandbox/dataset/default/bleemeo-agent-samples.json \
    > ./sandbox/dataset/default/bleemeo-agent-samples.tmp
mv ./sandbox/dataset/default/bleemeo-agent-samples.tmp ./sandbox/dataset/default/bleemeo-agent-samples.json
```

The default dataset is embedded in the SquirrelDB binary: changes to
`sandbox/dataset/default` require a rebuild.
