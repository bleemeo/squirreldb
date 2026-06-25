package main

import (
	"context"
	"flag"
	"time"

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

	ctx, cancel := context.WithTimeout(ctx, *maxWaitTime)
	defer cancel()

	// Redis (clustering) first: the backend needs the cluster to start.
	waitRedis(ctx, squirreldb)
	waitBackend(ctx, squirreldb, cfg.Cassandra.ReplicationFactor)

	return ctx.Err()
}

// waitRedis waits until the cluster (Redis) is reachable.
func waitRedis(ctx context.Context, squirreldb *daemon.SquirrelDB) {
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

		if _, err := squirreldb.Cluster(ctx); err != nil {
			log.Printf("connection to Redis failed: %s", err)

			continue
		}

		log.Printf("Redis is ready")

		return
	}
}

// nodeCounter is implemented by the Cassandra backend, to wait for the
// replication factor. The ClickHouse backend does not implement it.
type nodeCounter interface {
	StoreNodeCount(ctx context.Context) (int, error)
}

// waitBackend waits until the configured backend is reachable (its connection
// starts). For Cassandra it also waits until at least replication nodes are up.
func waitBackend(ctx context.Context, squirreldb *daemon.SquirrelDB, replication int) {
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

		b, err := squirreldb.Backend(ctx)
		if err != nil {
			log.Printf("backend not ready: %s", err)

			continue
		}

		if counter, ok := b.(nodeCounter); ok {
			count, err := counter.StoreNodeCount(ctx)
			if err != nil {
				log.Printf("failed to query node count: %s", err)

				continue
			}

			if count < replication {
				log.Printf("Not enough Cassandra up. Had %d want at least %d", count, replication)

				continue
			}
		}

		log.Printf("Backend %s is ready", squirreldb.Config.Backend)

		return
	}
}
