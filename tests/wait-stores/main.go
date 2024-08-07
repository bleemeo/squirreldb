package main

import (
	"context"
	"flag"
	"time"

	"github.com/bleemeo/squirreldb/daemon"

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
		Config: cfg,
		Logger: log.With().Str("component", "daemon").Logger(),
	}

	replication := cfg.Cassandra.ReplicationFactor

	msg := "Using Cassandra at %v with RF=%d. Redis at %v"
	log.Printf(msg, cfg.Cassandra.Addresses, replication, cfg.Redis.Addresses)

	firstLoop := true

	ctx, cancel := context.WithTimeout(ctx, *maxWaitTime)
	defer cancel()

	for ctx.Err() == nil {
		if !firstLoop {
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				break
			}
		}

		firstLoop = false

		connection, err := squirreldb.CassandraConnection(ctx)
		if err != nil {
			log.Printf("connection to cassandra failed: %s", err)

			continue
		}

		defer connection.Close()

		session, err := connection.Session()
		if err != nil {
			log.Printf("connection to cassandra failed: %s", err)

			continue
		}

		count := 0
		it := session.Query("SELECT peer from system.peers").WithContext(ctx).Iter()

		for it.Scan(nil) {
			count++
		}

		if err := it.Close(); err != nil {
			log.Printf("failed to query Cassandra peer: %s", err)
			session.Close()

			continue
		}

		// +1 because current Cassandra is not in peers
		if count+1 < replication {
			log.Printf("Not enough Cassandra up. Had %d want at least %d", count+1, replication)

			continue
		}

		session.Close()
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
		cluster.Close()

		if err != nil {
			log.Printf("connection to Redis failed: %s", err)

			continue
		}

		log.Printf("Redis is ready")

		break
	}

	return ctx.Err()
}
