package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/core/processsetup"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/tests/common/testsetup"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
)

//nolint:gochecknoglobals
var (
	argURL    = flag.String("squirreldb-url", "", "URL of SquirrelDB (if unset, a built-in SquirrelDB will be used)")
	tenant    = flag.String("tenant", "", "Tenant header")
	threads   = flag.Int("threads", 2, "Number of writing/reading threads")
	scale     = flag.Int("scale", 5, "Scaling factor")
	skipWrite = flag.Bool("skip-write", false, "Skip write phase")
	skipRead  = flag.Bool("skip-read", false, "Skip read phase")
	nowStr    = flag.String("now", time.Now().Round(10*time.Second).Format(time.RFC3339), "Value for \"now\"")
)

const (
	labelName   = "__name__"
	labelNowStr = "nowStr"
	labelSize   = "size"
	labelFiller = "filler"
	labelBatch  = "batch"
	labelScale  = "scale"

	valLargeWrite = "large_write"
	valWeek       = "week"
	valYes        = "yes"
)

// A just-written point is only queryable once the ClickHouse backend has drained
// its write-ahead log (normally ~5s); reads are eventually consistent. Retry a read
// until it succeeds or the budget elapses — a consistent read passes on the first
// try, so this only adds delay right after a write. Reads are idempotent, so
// retrying the whole read phase is safe.
const (
	queryRetryBudget = 10 * time.Second
	queryRetryStep   = 500 * time.Millisecond
)

func retryUntilConsistent(ctx context.Context, fn func() error) error {
	start := time.Now()

	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			if attempt > 1 {
				log.Info().Dur("delay", time.Since(start)).Msg("read became consistent (write now visible)")
			}

			return nil
		}

		if time.Since(start) >= queryRetryBudget {
			return err
		}

		select {
		case <-ctx.Done():
			return err
		case <-time.After(queryRetryStep):
		}
	}
}

func main() {
	testsetup.SetTestEnvironment()

	err := processsetup.RunWithSignalHandler(run)

	if *argURL == "" {
		metricResult, _ := prometheus.DefaultGatherer.Gather()
		for _, mf := range metricResult {
			_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
		}
	}

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

	squirrelDBURL := *argURL

	var squirreldb *daemon.SquirrelDB

	if squirrelDBURL == "" {
		squirreldb = &daemon.SquirrelDB{
			Config: cfg,
			Logger: log.With().Str("component", "daemon").Logger(),
			MetricRegistry: prometheus.WrapRegistererWith(
				map[string]string{"restarted": "false"},
				prometheus.DefaultRegisterer,
			),
			// Drop stale data when this (first, non-restarted) instance starts. The
			// restarted instance below must NOT drop — it reads the persisted data.
			DropDataOnStartup: true,
		}
		defer squirreldb.Stop()

		err = squirreldb.SquirrelDBCore().TestTools().DropTemporaryStore(ctx, false)
		if err != nil {
			return err
		}

		err = squirreldb.Start(ctx)
		if err != nil {
			return fmt.Errorf("fail to Start SquirrelDB: %w", err)
		}

		squirrelDBURL = fmt.Sprintf("http://127.0.0.1:%d/", squirreldb.ListenPort())
	} else if !strings.HasSuffix(squirrelDBURL, "/") {
		squirrelDBURL += "/"
	}

	now, err := time.Parse(time.RFC3339, *nowStr)
	if err != nil {
		return err
	}

	log.Printf("now = %v", now)

	if !*skipWrite {
		if err := write(ctx, now, squirrelDBURL+"api/v1/write", *tenant); err != nil {
			return err
		}
	}

	if !*skipRead { //nolint:nestif
		for _, withRestart := range []bool{false, true} {
			if withRestart && squirreldb == nil {
				// can't restart if not using built-in SquirrelDB
				continue
			}

			if withRestart {
				squirreldb.Stop()

				// Purge the Redis WAL (DropTemporaryStore deletes every key under the
				// keyspace, which includes the WAL) so the post-restart reads can ONLY
				// come from ClickHouse — proving the data is durably persisted there,
				// not merely buffered in the WAL. Redis stays configured: the WAL
				// backend requires it (the earlier read already drained the WAL, so
				// nothing is lost by clearing it). Safe: the previous read succeeded,
				// meaning the data is queryable = drained to ClickHouse.
				if err := squirreldb.SquirrelDBCore().TestTools().DropTemporaryStore(ctx, false); err != nil {
					return err
				}

				squirreldb = &daemon.SquirrelDB{
					Config: cfg,
					Logger: log.With().Str("component", "daemon").Logger(),
					MetricRegistry: prometheus.WrapRegistererWith(
						map[string]string{"restarted": "true"},
						prometheus.DefaultRegisterer,
					),
				}

				defer squirreldb.Stop()

				err = squirreldb.Start(ctx)
				if err != nil {
					return err
				}

				squirrelDBURL = fmt.Sprintf("http://127.0.0.1:%d/", squirreldb.ListenPort())
			}

			// Retry: right after the write the point may not be drained to ClickHouse
			// yet (eventual consistency). After the restart the WAL is purged, so the
			// read comes from ClickHouse and is consistent on the first try.
			if err := retryUntilConsistent(ctx, func() error {
				return read(ctx, now, squirrelDBURL+"api/v1/read", *tenant)
			}); err != nil {
				return err
			}

			if err := retryUntilConsistent(ctx, func() error {
				return readPromQL(ctx, now, squirrelDBURL, *tenant)
			}); err != nil {
				if errors.Is(err, errMatrixDiffError) {
					log.Print(err)

					err = fmt.Errorf("%w: see above for details", errMatrixDiffError)
				}

				return err
			}
		}
	}

	return nil
}

func time2Millisecond(t time.Time) int64 {
	return t.Unix()*1000 + (t.UnixNano()%1e9)/1e6
}

func makeSampleModel(
	fromTime time.Time,
	stepTime time.Duration,
	fromValue float64,
	stepValue float64,
	count int,
) []model.SamplePair {
	result := make([]model.SamplePair, count)

	for i := range result {
		currentTime := fromTime.Add(stepTime * time.Duration(i))
		result[i] = model.SamplePair{
			Value:     model.SampleValue(fromValue + stepValue*float64(i)),
			Timestamp: model.TimeFromUnixNano(currentTime.UnixNano()),
		}
	}

	return result
}

func makeSample(
	fromTime time.Time,
	stepTime time.Duration,
	fromValue float64,
	stepValue float64,
	count int,
) []prompb.Sample {
	resultBase := makeSampleModel(fromTime, stepTime, fromValue, stepValue, count)
	result := make([]prompb.Sample, len(resultBase))

	for i, row := range resultBase {
		result[i] = prompb.Sample{
			Value:     float64(row.Value),
			Timestamp: time2Millisecond(row.Timestamp.Time()),
		}
	}

	return result
}
