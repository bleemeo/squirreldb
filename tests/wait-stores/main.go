package main

import (
	"context"
	"flag"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/backend"
	"github.com/bleemeo/squirreldb/daemon"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

//nolint:gochecknoglobals
var (
	maxWaitTime = flag.Duration("max-wait-time", 8*time.Minute, "Maximum time to wait for stores")
)

func main() {
	daemon.SetTestEnvironment()

	err := daemon.RunWithSignalHandler(run)
	if err != nil {
		log.Fatal().Err(err).Msg("Run daemon failed")
	}
}

func run(ctx context.Context) error {
	cfg, warnings, err := daemon.Config()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	squirreldb := &daemon.SquirrelDB{
		Config:         cfg,
		MetricRegistry: prometheus.NewRegistry(),
		Logger:         log.With().Str("component", "daemon").Logger(),
	}

	replication := cfg.Cassandra.ReplicationFactor

	msg := "Using Cassandra at %v with RF=%d. Redis at %v"
	log.Printf(msg, cfg.Cassandra.Addresses, replication, cfg.Redis.Addresses)

	firstLoop := true

	ctx, cancel := context.WithTimeout(ctx, *maxWaitTime)
	defer cancel()

	var store *backend.Backend

	for ctx.Err() == nil {
		if !firstLoop {
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				break
			}
		}

		firstLoop = false

		if store == nil {
			tmp, err := squirreldb.CassandraBackend(ctx)
			if err != nil {
				log.Printf("connection to cassandra failed: %s", err)

				continue
			}

			store = tmp

			defer store.CloseAll(ctx)
		}

		count, err := store.StoreNodeCount(ctx)
		if err != nil {
			log.Printf("failed to query node count: %s", err)

			continue
		}

		if count < replication {
			log.Printf("Not enough Cassandra up. Had %d want at least %d", count+1, replication)

			continue
		}

		log.Printf("Cassandra is ready")

		break
	}

	firstLoop = true

	for ctx.Err() == nil {
		if !firstLoop {
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				break
			}
		}

		cluster, err := squirreldb.Cluster(ctx)
		_ = cluster.Close()

		if err != nil {
			log.Printf("connection to Redis failed: %s", err)

			continue
		}

		log.Printf("Redis is ready")

		break
	}

	if cfg.Internal.TSDB == "clickhouse" {
		waitClickHouse(ctx, squirreldb)
	}

	return ctx.Err()
}

// waitClickHouse waits until the ClickHouse TSDB can be built (connection ping
// and schema creation succeed). It reuses the daemon's config-driven discovery,
// so it picks up the same addresses/database/auth as the rest of the run.
func waitClickHouse(ctx context.Context, squirreldb *daemon.SquirrelDB) {
	firstLoop := true

	for ctx.Err() == nil {
		if !firstLoop {
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				return
			}
		}

		firstLoop = false

		db, err := squirreldb.TSDB(ctx, false, nil)
		if err != nil {
			log.Printf("connection to ClickHouse failed: %s", err)

			continue
		}

		_ = db.Close(ctx)

		log.Printf("ClickHouse is ready")

		break
	}
}
