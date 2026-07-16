# Migrating from Cassandra to ClickHouse

SquirrelDB can move an existing deployment from the Cassandra backend to the
ClickHouse backend **without data loss and with no (or minimal) downtime**. Reads
keep serving from Cassandra the whole time; you switch to ClickHouse only once the
copy is complete. The migration is incremental, resumable,
rate-limitable, and reversible up until the moment you cut over.

## How it works

The migration has three parts:

1. **Dual-write** — while migrating, SquirrelDB writes each new point to *both*
   backends. Cassandra stays the source of truth (it serves reads and receives
   writes that must succeed); ClickHouse is written as a *best-effort* target, so
   a ClickHouse problem should not disrupt normal operation. Best-effort means a
   write to ClickHouse can be dropped — for example if ClickHouse is briefly
   unavailable — without affecting Cassandra or your clients; those dropped points
   are repaired afterwards (see step 3).
2. **Backfill** — the `squirreldb migrate` command copies historical data from
   Cassandra into ClickHouse, newest first.
3. **Cutover** — once the backfill is complete, you flip reads and writes to
   ClickHouse with a config change and a restart.

> **The one rule that matters: enable dual-write *before* you start the backfill.**
> Dual-write then covers everything from that moment onward, and the backfill
> covers everything before it. Together they leave no gap, which is what makes the
> final cutover clean.

## Limitation: data written before version 25.01.31.100908

Due to a bug introduced in version 25.01.31.100908 (January 2025), shards
created by an earlier version have an unreadable expiration date, and the
backfill **skips them** — their data is not migrated.

This only risks data loss if **both** are true:
- your SquirrelDB predates 25.01.31.100908 (it wrote shards before ~February
  2025), and
- your highest TTL is long enough that some of that pre-upgrade data is still
  within its retention when you run the migration.

If your deployment started on 25.01.31.100908 or later, or your TTL is short
enough that all pre-upgrade data has already expired, there is nothing to lose.

To check, look at the backfill log line `planned migration export shards ...
skipped_dead_shards=N`:
- `N = 0` — nothing was skipped, you are safe.
- `N > 0` — some shards were skipped. Harmless if their data had already expired;
  you only lose data if your TTL still covers data from before your
  25.01.31.100908 upgrade.


## Before you begin

- **A running ClickHouse**, reachable from SquirrelDB and configured in the
  `clickhouse:` section of your configuration (the same setup as any fresh
  ClickHouse deployment).
- **Disk headroom.** Both stores hold a full copy of the data at the same time
  during the migration — provision for Cassandra *and* ClickHouse together.
  Cassandra's disk is reclaimed only at the very end, when you decommission it.
- **Time.** On a large deployment the backfill can run for many hours. It runs in
  the background and can be stopped and resumed at any time.

## Configuration

Two options drive the migration:

- `backend` — the active backend. It serves reads, owns mutable labels and
  telemetry, and receives writes that must succeed. This is the source of truth.
- `extra_write_backend` — an optional second write target, written best-effort.
  Empty means single-backend (no migration). It must differ from `backend`.

| Phase      | `backend`    | `extra_write_backend` | Effect                                                                                     |
| ---------- | ------------ | --------------------- | ------------------------------------------------------------------------------------------ |
| Normal     | `cassandra`  | *(empty)*             | Cassandra only — your current setup.                                                       |
| Dual-write | `cassandra`  | `clickhouse`          | Reads and must-succeed writes on Cassandra; ClickHouse written best-effort; backfill runs. |
| Cutover    | `clickhouse` | *(empty)*             | Reads and writes switch to ClickHouse; Cassandra is no longer written.                     |

## Step by step

### 1. Enable dual-write

Edit the configuration and restart SquirrelDB:

```yaml
backend: cassandra
extra_write_backend: clickhouse
```

From now on, every new point is written to both backends. Nothing else changes for
your clients: reads still come from Cassandra, and writes still succeed based on
Cassandra alone. If ClickHouse is unavailable at any point during this phase, its
writes are dropped (Cassandra's are not) and are repaired in step 3.

### 2. Run the backfill

`squirreldb migrate` reads the same configuration file as the daemon and copies
data from `backend` (Cassandra) into `extra_write_backend` (ClickHouse). The migration
runs at the same time as the SquirrelDB daemon; `squirreldb migrate` only copies
historical data — it does not start the Prometheus remote storage or PromQL API
services.

```
squirreldb migrate --import-mutable-labels --throttle 0.8
```

Two options need to be tuned for your environment:

- `--throttle`: limits the number of ClickHouse inserts per second, which
  indirectly limits the read rate from Cassandra. It reduces throughput but may be
  necessary to avoid overloading the environment being migrated (e.g. a production
  one). The value is a float, and useful values are small — ClickHouse tolerates
  only a few inserts per second, since it has to keep merging the parts each insert
  creates. Use a fraction such as `0.1` to keep the load very low, or a value like
  `10` as a fast-but-bounded ceiling (10 inserts/s is already a lot for ClickHouse).
  Omit the flag entirely if disruption is acceptable or you want the fastest
  possible migration. Since the migration is resumable, don't hesitate to start low,
  Ctrl-C, and restart higher.
- `--batch-size`: together with `--throttle`, it sets the maximum throughput:
  roughly `throttle * batch-size` points per second. Because ClickHouse wants a few
  large inserts per second, each batch is a large chunk of points — 1 million by
  default. The higher this value, the faster the migration is expected to be, but
  the more memory the migration process uses and the more it stresses Cassandra
  (reading points faster). The sweet spot for best performance is around 3 to 5
  million points, at a memory cost of roughly 1 GiB; the default of 1 million points
  is a good trade-off — decent performance with a limited memory overhead of around
  200 MiB.

It copies newest data first and checkpoints its progress, so if it is interrupted
(Ctrl-C is graceful) it resumes where it left off. It prints its throughput and how
far back in time it has reached.

The backfill only copies data from before the instant dual-write took over, so it
does not re-copy what dual-write already wrote. By default that instant is *now* — the
moment you first launch the command — which means the points written between
enabling dual-write and launching the backfill are copied twice (harmless
duplicates, de-duplicated on read). Launch the backfill soon after enabling
dual-write to keep that overlap small, or pass `--dual-write-start` with the exact
instant to eliminate it.

Available flags:

| Flag                       | Default           | Meaning                                                                                                                                                                     |
| -------------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--throttle`               | `0` (unlimited)   | Maximum ClickHouse **inserts per second**. Lower it to leave a busy ClickHouse headroom to merge parts.                                                                     |
| `--batch-size`             | `1000000`         | Points per ClickHouse insert. Larger = higher throughput but more memory; smaller = less memory.                                                                            |
| `--dual-write-start`       | *(now)*           | RFC3339 instant dual-write began; the backfill migrates points before it. Chosen on the first run and persisted, so resumes stay consistent.                                |
| `--import-mutable-labels`  | `false`           | At the end of the run, copy the mutable labels from Cassandra, replacing ClickHouse's. Safe to re-run. Does nothing if mutable labels aren't used. See step 4.              |
| `--metrics-listen-address` | *(off)*           | Serve Prometheus `/metrics` (and `/debug/pprof/`) at this address for the duration of the run.                                                                              |
| `--print-metrics`          | `false`           | Print all metric values to stdout when the command exits.                                                                                                                   |

> **Resource note.** On a memory-constrained host, set the environment variable
> `GOGC=50` to lower peak memory further, at a small CPU cost.

### 3. Repair dropped points (if any)

Because ClickHouse is written best-effort, if it is briefly unavailable during
dual-write those writes are dropped — Cassandra, the source of truth, is never
affected. Each time ClickHouse recovers, SquirrelDB logs the **time range** of the
points that were dropped.

To repair, re-run the migration over that range with `--repair-from` / `--repair-to`
(RFC3339). A repair run migrates exactly that window and does not touch the main
backfill's resume checkpoint:

```
squirreldb migrate --repair-from 2026-07-01T00:00:00Z --repair-to 2026-07-01T02:00:00Z
```

Over-covering the range is safe: it only creates a bounded number of duplicate
points on disk in ClickHouse, which reads de-duplicate automatically.

### 4. Cut over

To ensure no data is lost, cut over only when **all** of these hold:

- the full backfill has completed,
- every logged drop range has been repaired,
- ClickHouse is healthy (no ongoing drop episode).

If you are using [mutable labels](https://docs.bleemeo.com/squirreldb/features/#mutable-labels),
`--import-mutable-labels` copies them as they stand at the
moment the copy runs (the end of the `squirreldb migrate` execution), so an edit made after that copy but
before the cutover would be lost: it lands in Cassandra, which is no longer read
once you cut over. For an exact copy, pause mutable-label edits, run one final
`squirreldb migrate --import-mutable-labels` (the data is already migrated, so it
just re-copies the labels in seconds), cut over, then resume edits.

To perform the cut-over, edit the configuration and restart:

```yaml
backend: clickhouse
# extra_write_backend removed / empty
```

Reads and writes now use ClickHouse. Cassandra is no longer written.

## Reverting

- **Before cutover** — simply stop the backfill. Reads never left Cassandra, so
  nothing is lost. If you want a completely clean restart, drop the ClickHouse
  data (e.g. drop & recreate the SquirrelDB database); the migration's progress
  is stored in ClickHouse, so dropping it resets the migration from scratch.
- **After cutover** — set `backend` back to `cassandra` and restart. Any points
  written to ClickHouse *after* the cutover exist only there, so they must be
  replayed from whatever feeds SquirrelDB. Revert within a short observation window
  to keep that gap small.
  Note: you can't set `extra_write_backend` to Cassandra to keep a dual-write
  during the observation window, because SquirrelDB only supports ClickHouse
  as `extra_write_backend`.
