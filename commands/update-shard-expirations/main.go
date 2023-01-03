// Script to update the expiration of all shards.
// By default the shard TTL is 2 years.
// Shards older than the TTL will be deleted.
// Other shard will have their expiration updated.

package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"squirreldb/daemon"
	"squirreldb/logger"
	"squirreldb/types"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

//nolint:gochecknoglobals
var (
	timeToLive = flag.Duration("shard.ttl", 2*365*24*time.Hour, "Default time to live for shards")
	delay      = flag.Duration("throttle.delay", time.Second, "Delay to wait between each shard")
)

var errShardExpirationUpdater = errors.New("index doesn't implement shardExpirationUpdater")

func main() {
	log.Logger = logger.NewTestLogger()

	if err := run(); err != nil {
		log.Fatal().Err(err).Msg("Failed to create all metrics")
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg, err := daemon.Config()
	if err != nil {
		return err
	}

	squirreldb := &daemon.SquirrelDB{
		Config:         cfg,
		MetricRegistry: prometheus.DefaultRegisterer,
		Logger:         log.With().Str("component", "daemon").Logger(),
	}

	cassandraIndex, err := squirreldb.Index(ctx, false)
	if err != nil {
		return err
	}

	updater, ok := cassandraIndex.(types.IndexInternalShardExpirer)
	if !ok {
		return errShardExpirationUpdater
	}

	return updater.InternalUpdateAllShards(ctx, *timeToLive, *delay)
}
