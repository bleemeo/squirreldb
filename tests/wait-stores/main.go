package main

import (
	"context"
	"flag"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core"
	"github.com/bleemeo/squirreldb/core/processsetup"
	"github.com/bleemeo/squirreldb/tests/common/testsetup"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

//nolint:gochecknoglobals
var (
	maxWaitTime = flag.Duration("max-wait-time", 8*time.Minute, "Maximum time to wait for stores")
)

func main() {
	testsetup.SetTestEnvironment()

	err := processsetup.RunWithSignalHandler(run)
	if err != nil {
		log.Fatal().Err(err).Msg("Run daemon failed")
	}
}

func run(ctx context.Context, _ context.Context) error {
	cfg, warnings, err := testsetup.LoadConfigForTest()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	squirreldb := core.New(
		core.Deps{
			Logger: log.With().Str("component", "wait-store").Logger(),
			// A private registry per process avoids duplicate-registration panics when
			// several instances (and restarts) register the same backend metrics.
			MetricRegistry: prometheus.NewRegistry(),
		},
		cfg,
		core.SquirrelDBOption{},
	)

	ctx, cancel := context.WithTimeout(ctx, *maxWaitTime)
	defer cancel()

	// Redis (clustering) first: the backend needs the cluster to start.
	waitRedis(ctx, cfg)
	waitBackend(ctx, squirreldb, cfg.Cassandra.ReplicationFactor)

	log.Printf("Backend %s is ready", cfg.Backend)

	return ctx.Err()
}

// waitRedis waits until the cluster (Redis) is reachable.
func waitRedis(ctx context.Context, cfg config.Config) {
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

		// We need one squirreldb per try, because re-trying Cluster don't work
		// because the same MetricRegistry could be used for the same metrics.
		squirreldb := core.New(
			core.Deps{
				Logger: log.With().Str("component", "wait-store").Logger(),
				// A private registry per process avoids duplicate-registration panics when
				// several instances (and restarts) register the same backend metrics.
				MetricRegistry: prometheus.NewRegistry(),
			},
			cfg,
			core.SquirrelDBOption{},
		)

		if _, err := squirreldb.Cluster(ctx); err != nil {
			log.Printf("connection to Redis failed: %s", err)

			continue
		}

		log.Printf("Redis is ready")

		squirreldb.CloseAll(ctx)

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
func waitBackend(ctx context.Context, squirreldb *core.SquirrelDB, replication int) {
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

		b, err := squirreldb.Primary(ctx)
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

		return
	}
}
