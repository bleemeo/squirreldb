# SquirrelDB HA setup with Clickhouse

This setup provide both scalability and availability of a Prometheus remote store using SquirrelDB.

To achieve this goal, you will need:

* A Clihkouse cluster, which means
  * 3 Clickhouse-keeper node
  * 2 Clickhouse-server node
* A Redis cluster (start at 6 nodes)
* A load-balancer in front of SquirrelDB (e.g. nginx)
* two or more of SquirrelDB instances

For the details on how to configure a Clickhouse cluster or Redis cluster, look at their
respective documentation or follow the quickstart below.

If you are not there yet, [docs/clickhouse-cluster.md](../../docs/clickhouse-cluster.md) describes
the three supported ClickHouse deployments (single node, single-node cluster, multi-node cluster),
which one to pick, and how to move from one to the next — including migrating an existing
single-node ClickHouse to a cluster without losing its data.

SquirrelDB instance will be stateless (they are stateless, because short term data will be stored in Redis) and
need the following configurations:

```
clickhouse:
  # List every ClickHouse server node here. SquirrelDB opens one connection per
  # address, health-checks each (reachable + holds the tables + replication not
  # lagging), and routes queries only to healthy nodes — so a node that is down,
  # rebuilt empty, or still resyncing is excluded automatically (see "Recovering a
  # lost ClickHouse node" below). No external load balancer is needed for this.
  addresses:
  - "clickhouse1:9000"
  - "clickhouse2:9000"
  # Tables are created with SQL option "ON CLUSTER <name>", we need to known the name to use.
  # This is the same you configured in the Clihckouse server configuration.
  # The replicated tables use an explicit, name-based Keeper path
  # (/clickhouse/tables/{shard}/{database}/<table>), so these tables must never be
  # RENAMEd. See "Recovering a lost ClickHouse node" below.
  cluster_name: main_cluster
redis:
  # Like for Clickhouse addresses, you just need to have enough addresses to connected to one
  # working Redis node, from here client will discover all nodes in the cluster.
  addresses: "redis1:6379,redis2:6379"
```

## Quickstart

This quickstart will start a SquirrelDB in HA (with Clickhouse cluster and Redis) using docker compose.
Obvisouly this is only for testing since all components will run on the same machine.

Like the single-node quickstart, this will also start a Prometheus + Grafana and one node_exporter to
have some data in SquirrelDB.

Start the stack:

```
docker compose up -d
```

Then as for single SquirrelDB, go to http://localhost:3000 (default credentials are admin/admin) and:

* Add a Prometheus datasource (using http://squirreldb:9201 as URL)
* Create your dashboard or import a dashboard (for example import dashboard with ID 1860).

### Testing availability

To test availability, you can stop+start or remove/recreate component.

Example of test:

```
# Recreate Prometheus, losing its local store of metrics. This ensure that metric points
# are read from SquirrelDB
docker compose up -d --force-recreate --renew-anon-volumes prometheus

# Kill one Clickhouse then restart it
docker compose stop -t0 clickhouse-server-0
docker compose start clickhouse-server-0

# Stop and recreate one SquirrelDB
docker compose rm --force --stop -v squirreldb2
docker compose up -d squirreldb2

docker compose stop -t0 redis4
docker compose up -d redis4
docker compose stop -t0 redis2
docker compose up -d redis2
```

### Recovering a lost ClickHouse node

As long as a server is only **stopped/started**, ClickHouse replication catches it
up on its own — nothing to do. The procedure below is only for a server that lost
its data and is **recreated empty** (the ClickHouse process runs, but with no
tables), e.g.:

```
docker compose rm --force --stop -v clickhouse-server-0
docker compose up -d clickhouse-server-0
```

SquirrelDB health-checks each host and routes only to *ready* ones (reachable +
holds the tables + replica not lagging), so a rebuilt-empty or still-resyncing
node is excluded until it catches up, then re-included — no address edit, no LB
change. It also never recreates tables on restart (only at first bootstrap) and
uses name-based Keeper paths, so re-creating a table re-attaches to the existing
data instead of forking. Bringing the node back is the manual operation below:

Steps (the recreated node is `clickhouse-server-0`, whose replica macro is
`replica01` — see `clickhouse-server-start.sh`; run the SQL on a **healthy** node,
here `clickhouse-server-1`):

1. Drop the dead replica's metadata from Keeper, for every replicated table. This
   stale registration is the one thing blocking re-creation; dropping it is kept
   manual on purpose, since `SYSTEM DROP REPLICA` is destructive:
   ```
   # The table list is read from a healthy node, so it cannot go stale as the schema grows.
   for t in $(docker compose exec clickhouse-server-1 clickhouse-client -q \
       "SELECT name FROM system.tables WHERE database = 'squirreldb' AND engine LIKE 'Replicated%'"); do
     docker compose exec clickhouse-server-1 clickhouse-client -q "SYSTEM DROP REPLICA 'replica01' FROM TABLE squirreldb.$t"
   done
   ```
   (`SYSTEM DROP REPLICA` only affects the inactive/dead replica; it refuses to drop a live one.)

2. Recreate the tables. Ask any SquirrelDB instance to re-run the schema creation
   (`CREATE ... IF NOT EXISTS ON CLUSTER`, which re-attaches the rebuilt node now
   that its stale replica is gone):
   ```
   curl -XPOST http://squirreldb:9201/debug/clickhouse/create_tables
   ```
   Without SquirrelDB, do it by hand: copy each table's definition from a healthy
   node and run it on the rebuilt one (the name-based path and `{replica}` macro
   make it re-attach):
   ```
   docker compose exec clickhouse-server-1 clickhouse-client -q "SHOW CREATE TABLE squirreldb.tsdb_data"
   # run the printed statement on clickhouse-server-0, and likewise for every table
   ```

3. Nothing else to do: SquirrelDB re-includes the node once it has resynced. Watch
   the per-host view SquirrelDB uses for routing:
   ```
   curl -s http://squirreldb:9201/debug/clickhouse/host
   ```
   (or the `squirreldb_clickhouse_ready_nodes` / `_reachable_nodes` metrics), and
   the replica catch-up directly (no rows = caught up):
   ```
   docker compose exec clickhouse-server-1 clickhouse-client -q \
     "SELECT database, table, absolute_delay FROM system.replicas WHERE is_session_expired OR absolute_delay > 0"
   ```

If you skip step 1 and recreate the table first, it fails with
`Replica ... already exists` — a safe error, not a split-brain. Just run step 1 and
retry.

For Redis, if you recreate a node (e.g. lost data, not just restart it), you will need to drop and re-add the node:
* First find the node id of the lost node, using:
  ```
  docker compose exec redis1 redis-cli cluster nodes
  ```
  It will be the node with "fail" and "noaddr"
* Drop this node on every Redis node:
  ```
  for i in 1 2 3 4 5 6; do docker compose exec redis${i}_1 redis-cli cluster forget $NODE_ID;done
  ```
* Add the new node (for example, assuming lost node is the 172.28.0.26 and 172.28.0.21 is a working node):
  ```
  docker compose exec redis_init_1 redis-cli --cluster add-node 172.28.0.26:6379 172.28.0.21:6378 --cluster-slave
  ```

For grafana, if you recreate it you will lose all your dashboard and must recreate them, but metric data are not lost.

For Prometheus and SquirrelDB are stateless. Prometheus also store recent metric data (15 days by default), but will query SquirrelDB store
when its local store is empty. So restarting/recreating them as no impact.
