# PromQL compliance test

## Ingest metrics

For the compliance tests, we need to ingest data from the demo Prometheus
endpoints for one hour. Prometheus will scrape the endpoint and use remote write
to send the data to SquirrelDB.

First you need to start SquirrelDB, either as a cluster or as a single node. You
may use the docker compose at the root of this repository or the one in
`examples/squirreldb_ha`.

Then you can start Prometheus with docker compose. The config expects SquirrelDB
to be accessible at http://127.0.0.1:9201. It can be modified in
`prometheus-test-data.yml`.

```shell
docker compose up -d
```

## Run compliance test

Once the metric ingestion has run for one hour, you can clone the compliance
repository and run the test.

```shell
git clone git@github.com:prometheus/compliance
(cd compliance/promql/cmd/promql-compliance-tester && go build .)
```

```shell
./compliance/promql/cmd/promql-compliance-tester/promql-compliance-tester \
   -config-file ./promql-test-queries.yml -config-file ./test-squirreldb.yml -query-parallelism 10
```
