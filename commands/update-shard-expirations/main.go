// Script to update the expiration of all shards.
// By default the shard TTL is 2 years.
// Shards older than the TTL will be deleted.
// Other shard will have their expiration updated.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"squirreldb/cassandra/index"
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

	updater, ok := cassandraIndex.(types.IndexShardExpirationUpdater)
	if !ok {
		return errShardExpirationUpdater
	}

	shards, err := getShards(ctx, updater)
	if err != nil {
		return fmt.Errorf("get shards: %w", err)
	}

	for _, shard := range shards {
		if ctx.Err() != nil {
			break
		}

		err := updateShard(ctx, shard, updater)
		if err != nil {
			return fmt.Errorf("update shard: %w", err)
		}

		time.Sleep(*delay)
	}

	updater.ApplyExpirationUpdateRequests(ctx, time.Now())

	return ctx.Err()
}

// updateShard updates a single shard expiration.
// The shard is deleted if the expiration is in the past.
func updateShard(ctx context.Context, shard int32, updater types.IndexShardExpirationUpdater) error {
	now := time.Now()
	shardTime := index.TimeForShard(shard)

	log.Trace().Msgf("Processing shard %s", shardTime)

	// The expiration is calculated from the shard time.
	expiration := shardTime.Add(*timeToLive)

	if expiration.Before(now) {
		err := updater.DeleteShard(ctx, shard)
		if err != nil {
			return err
		}

		return nil
	}

	return updater.UpdateShardExpiration(ctx, now, shard, expiration)
}

// getShards returns all known shards.
func getShards(ctx context.Context, updater types.IndexShardExpirationUpdater) ([]int32, error) {
	shardBitmap, err := updater.Postings(
		ctx,
		[]int32{index.GlobalShardNumber},
		index.ExistingShardsLabel,
		index.ExistingShardsLabel,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("get postings for existing shards: %w", err)
	}

	var shards []int32

	it := shardBitmap.Iterator()

	for ctx.Err() == nil {
		shard, eof := it.Next()
		if eof {
			break
		}

		shards = append(shards, int32(shard))
	}

	return shards, nil
}
