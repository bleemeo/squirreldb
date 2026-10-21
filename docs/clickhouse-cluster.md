# ClickHouse deployments, and how to grow from one to the next

SquirrelDB supports three ClickHouse deployments. Which one you run is decided by
the ClickHouse configuration plus a single SquirrelDB option, `clickhouse.cluster_name`.

| Deployment              | ClickHouse            | `cluster_name` | Table engines         |
| ----------------------- | --------------------- | -------------- | --------------------- |
| **Single node**         | 1 server, no Keeper   | empty          | `MergeTree`           |
| **Single-node cluster** | 1 server, 1 Keeper    | set            | `ReplicatedMergeTree` |
| **Multi-node cluster**  | 2+ servers, 3 Keepers | set            | `ReplicatedMergeTree` |

**Single node** is the simplest and stays fully supported: no Keeper, no cluster
configuration. Use it to evaluate SquirrelDB, and in production wherever one server
meets your requirements.

**Single-node cluster** is not more available than single node — one server is still
one point of failure — but the replication machinery is already in place, so adding a
second server later is just starting it. If there is any chance you will grow, start
here: it costs one Keeper and saves you the migration described below.

**Multi-node cluster** is the highly-available setup; see
[examples/squirreldb_ha_clickhouse](../examples/squirreldb_ha_clickhouse/) for a
complete one (ClickHouse cluster, Redis cluster, several SquirrelDB).

What replication costs compared to plain single node: replicated tables need Keeper.
While Keeper is unreachable they go read-only and SquirrelDB cannot write (a plain
`MergeTree` would keep accepting writes), and a Keeper sharing the server's disk dies
with it.

## Growing

Go **single node → single-node cluster → multi-node cluster**, in that order. The
first move is a data migration, and it is much easier to reason about with exactly
one ClickHouse server. The second move needs no migration of your own: the tables are
already replicated, so ClickHouse itself copies the data to the new server.

> **Admin endpoints.** The `curl` commands below call SquirrelDB's `/debug/` endpoints
> on its main port (`9201`). If `admin_listen_address` is set, these endpoints are only
> served on that address: use it instead (they return 404 on the main port).

# Single node → single-node cluster

## 1. Configure ClickHouse

Server-side configuration is out of scope here (see the ClickHouse documentation);
what SquirrelDB needs is:

- **a Keeper.** One is enough if you stay on a single-node cluster; go straight to
  three if you intend to add a second ClickHouse server. Never two: a two-node Keeper
  needs both to be up, which is worse than one. It can be embedded in
  `clickhouse-server` (`<keeper_server>`) or run separately.
- **the cluster declaration** in `<remote_servers>` — it can start with this server as
  its only replica. Its name is what you will put in `cluster_name`.
- **the `shard` and `replica` macros**, used by the tables' Keeper path.

Restart ClickHouse and check all three:

```sql
SELECT cluster, host_name, is_local FROM system.clusters WHERE cluster = 'main_cluster';
SELECT macro, substitution FROM system.macros;
SELECT count() FROM system.zookeeper WHERE path = '/';  -- proves Keeper answers
```

## 2. Point SquirrelDB at the cluster

Set the option and restart:

```yaml
clickhouse:
  cluster_name: main_cluster
```

SquirrelDB starts and keeps serving reads and writes, but it now sees tables whose
engine contradicts the configuration, and says so:

- an error log per table, at startup;
- the host is excluded from the *ready* set: `squirreldb_clickhouse_ready_nodes`
  drops to 0 and the reason shows in `curl -s http://squirreldb:9201/debug/clickhouse/host`.

Nothing is replicated in this state — it is a transition, not a destination.

## 3. Look at the plan

```sh
curl -XPOST 'http://squirreldb:9201/debug/clickhouse/migrate_to_cluster?dry_run=1'
```

This reports the ClickHouse host holding the data, and for each table what will
happen and which partitions (and how much disk) will move. It changes nothing, so run
it as often as you like.

## 4. Migrate

```sh
curl -XPOST -N http://squirreldb:9201/debug/clickhouse/migrate_to_cluster
```

Per table, it renames the single-node table to `<table>_pre_cluster`, creates the
replicated table under the original name, and attaches the source's partitions to it.
Attaching uses filesystem hardlinks, so **no data is copied** whatever the volume.

What to expect while it runs:

- **writes to ClickHouse are blocked**, by design. Incoming points accumulate in the
  write-ahead log (Redis, or memory without Redis) and land in ClickHouse once the
  migration finishes. Clients keep succeeding until the WAL passes its backpressure
  thresholds, where SquirrelDB slows them down, then blocks them until it drains.
- **reads fail** for the moments a table is between its rename and its creation.
- run it with **a single SquirrelDB instance** if you can. Others keep working, but
  their reads and writes error the same way, and one restarting mid-migration
  re-creates the write path: the migration still finishes correctly, but writes are
  back in the replicated tables, so a rollback would lose everything written since.
- **it is re-callable.** If it fails, times out, or you interrupt it, call it again: it
  works out what is left from ClickHouse itself and resumes. Interruption is only ever
  honoured between two partitions. A partition interrupted mid-attach is the one case a
  re-call can attach data twice; that only skews the pre-aggregate, which
  `POST /debug/clickhouse/agg/rebuild?from=&to=` recomputes over the affected range.
- **on failure, writes stay down.** That is deliberate: letting writes into the new
  replicated tables would strand them if you then rolled back. The response tells you
  the two ways out (finish, or roll back).

## 5. Check, then clean up

```sh
curl -s http://squirreldb:9201/debug/clickhouse/host
```

The host must be back to *ready*, which means SquirrelDB found every table with the
engine this mode expects. At this point SquirrelDB is fully operational, you can query
some history — a dashboard over a range older than the migration — to confirm the
migrated data is served.

The migration leaves each `<table>_pre_cluster` in place, emptied (its data is under
the table's `detached/` directory), as a rollback net. Dropping them is a separate,
explicit call:

```sh
curl -XPOST 'http://squirreldb:9201/debug/clickhouse/migrate_to_cluster?cleanup=1'
```

Do it once you have checked your data, and do not forget it: right after the
migration the sources cost almost no disk (they share it with the migrated tables
through the hardlinks), but that sharing ends as merges and TTL rewrite the migrated
tables, so they grow into a full duplicate of your pre-migration data.

If you do not want the net at all — a test instance, a small dataset — pass
`?cleanup=1` on the migration call itself and it drops them at the end.

## Rolling back

Possible as long as the `<table>_pre_cluster` tables are still there, so until you run
`?cleanup=1`. The cost is the points written to the replicated tables since the
migration started: they stay behind. That cost is nil while writes are still down (a
failed migration you decide not to finish), and grows once they have resumed.

For **every** `<table>_pre_cluster` still present, drop the replicated table, re-attach
the source's detached partitions, and put the source back under the original name:

```sql
DROP TABLE <table> ON CLUSTER main_cluster SYNC;
ALTER TABLE <table>_pre_cluster ATTACH PARTITION ID '<id>';   -- for each detached partition
RENAME TABLE <table>_pre_cluster TO <table>;
```

Both lists come from ClickHouse:

```sql
SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '%\_pre\_cluster';
SELECT DISTINCT partition_id FROM system.detached_parts WHERE table = '<table>_pre_cluster';
```

Then restart SquirrelDB with `cluster_name` removed and run
`curl -XPOST http://squirreldb:9201/debug/clickhouse/create_tables`.

# Single-node cluster → multi-node cluster

No data migration to run here — you are adding a replica to tables that are already
replicated.

1. **Go to three Keepers** — a single Keeper is a single point of failure, which
   defeats the purpose of the second server.
2. **Start the new ClickHouse server** with the same cluster configuration and its own
   `replica` macro, and add it to `<remote_servers>` on every server.
3. **Create the tables on it.** Ask any SquirrelDB to re-run the schema creation
   (`CREATE ... IF NOT EXISTS ON CLUSTER`, a no-op on the existing server):
   ```sh
   curl -XPOST http://squirreldb:9201/debug/clickhouse/create_tables
   ```
   ClickHouse then replicates the existing data to it on its own.
4. **Add its address** to `clickhouse.addresses` in every SquirrelDB and restart them.
   The order of steps 3 and 4 does not matter: SquirrelDB excludes a host that is
   missing tables or whose replica is still catching up, and includes it once ready.
5. **Watch it catch up**:
   ```sh
   curl -s http://squirreldb:9201/debug/clickhouse/host
   ```
   ```sql
   SELECT database, table, absolute_delay FROM system.replicas WHERE absolute_delay > 0;
   ```

For a setup that is highly available end to end you also need Redis for the short-term
store and more than one SquirrelDB behind a load balancer — see
[examples/squirreldb_ha_clickhouse](../examples/squirreldb_ha_clickhouse/).
