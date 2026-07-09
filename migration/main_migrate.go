// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/logger"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// migrateCommandFlags are the command flags specific to `squirreldb migrate`.
// They are combined with config.ConfigFlags() the same way the daemon combines
// DaemonCommandFlags() — so migrate accepts the same config flags (and the same
// default config file) as the daemon; only these command flags differ.
func migrateCommandFlags() []config.Flag {
	return []config.Flag{
		{
			Name:  "repair-from",
			Value: "",
			Usage: "Repair run: migrate only points at/after this RFC3339 time (ignores the checkpoint)",
		},
		{
			Name:  "repair-to",
			Value: "",
			Usage: "Repair run: migrate only points at/before this RFC3339 time (ignores the checkpoint)",
		},
		{
			Name:  "dual-write-start",
			Value: "",
			Usage: "RFC3339 instant dual-write began; backfill migrates points before it (default: now, persisted)",
		},
		{
			Name:  "import-mutable-labels",
			Value: false,
			Usage: "At the end of the run, copy mutable labels from the source after clearing the target's",
		},
		{Name: "throttle", Value: float64(0), Usage: "Max ClickHouse inserts/s (0 = unlimited); leaves room to merge parts"},
		{Name: "batch-size", Value: 1_000_000, Usage: "Points per ClickHouse insert (bigger = fewer parts, more memory)"},
		{Name: "print-metrics", Value: false, Usage: "Print all metrics to stdout on exit (final/global values)"},
		{
			Name:  "metrics-listen-address",
			Value: "",
			Usage: "Serve Prometheus /metrics on this address during the migration (metrics only); empty = off",
		},
	}
}

// RunMigrate implements the `squirreldb migrate` subcommand: it backfills raw
// data from the source backend (config `backend`) into the target
// (`extra_write_backend`). It returns a process exit code.
func RunMigrate(args []string) int {
	flagSet, cfg, warnings, err := daemon.LoadConfigFromFlags("migrate", migrateCommandFlags(), args)
	if errors.Is(err, daemon.ErrNormalExit) {
		return 0
	}

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse flags or load configuration")
	}

	if warnings != nil {
		log.Warn().Err(warnings).Msg("Got warnings while loading config")
	}

	repairFromStr, _ := flagSet.GetString("repair-from")
	repairToStr, _ := flagSet.GetString("repair-to")
	dualWriteStartStr, _ := flagSet.GetString("dual-write-start")
	importMutableLabels, _ := flagSet.GetBool("import-mutable-labels")
	throttle, _ := flagSet.GetFloat64("throttle")
	batchSize, _ := flagSet.GetInt("batch-size")
	printMetrics, _ := flagSet.GetBool("print-metrics")
	metricsAddr, _ := flagSet.GetString("metrics-listen-address")

	opts, err := parseMigrateOptions(migrateFlags{
		repairFrom:          repairFromStr,
		repairTo:            repairToStr,
		dualWriteStart:      dualWriteStartStr,
		importMutableLabels: importMutableLabels,
		throttle:            throttle,
		batchSize:           batchSize,
	})
	if err != nil {
		log.Error().Err(err).Msg("Invalid migrate options")

		return 2
	}

	if cfg.ExtraWriteBackend == "" {
		log.Error().Msg("migrate requires 'extra_write_backend' to be set (the migration target)")

		return 2
	}

	if sentryCloser := daemon.SetupSentryLogger(cfg); sentryCloser != nil {
		defer sentryCloser.Close()
	}

	defer logger.ProcessPanic()

	squirreldb := &daemon.SquirrelDB{
		Config: cfg,
		Logger: log.With().Str("component", "migrate").Logger(),
		// We don't call Init() (no daemon run loop), so set the metric registry
		// explicitly — otherwise it stays nil and the backends register their
		// metrics nowhere, leaving --print-metrics / --metrics-listen-address empty.
		MetricRegistry: prometheus.DefaultRegisterer,
		// The source is read-only: the migration only reads it, and this is a double
		// safety against writing to the store being migrated away. The write target
		// (MigrateToBackend) is unaffected.
		PrimaryBackendReadOnly: true,
		// The target is import-only: `migrate` writes via ImportRawData (bypassing the
		// WAL), so it must not run a WAL drainer — that would join the drainer lease
		// election of the live daemon that owns the shared WAL, and emit a misleading
		// "WAL not fully drained" warning at shutdown about points it never wrote.
		MigrateTargetImportOnly: true,
	}

	logger := log.With().Str("component", "migrate").Logger()
	logger.Info().Str("source", cfg.Backend).Str("target", cfg.ExtraWriteBackend).Msg("Starting migration")

	// Metrics observability (dev): the backends register into the default
	// registry, so both options read from it. They are complementary: print gives
	// the final values even for a short run; the HTTP endpoint lets a long run be
	// scraped. The endpoint serves ONLY /metrics (no read/write API).
	if metricsAddr != "" {
		defer serveMetrics(metricsAddr, logger)()
	}

	if printMetrics {
		defer printAllMetrics(logger)
	}

	run := func(ctx context.Context, shutdownCtx context.Context) error {
		// Close both backends on exit: closing the target flushes its write-ahead log
		// so the last imported points reach ClickHouse. shutdownCtx outlives ctx, so
		// the close still runs after a graceful cancel.
		defer closeBackends(shutdownCtx, squirreldb, logger)

		return Run(ctx, shutdownCtx, squirreldb, opts, logger)
	}

	defer func() {
		log.Debug().Msg("SquirrelDB migration stopped")

		if cfg.Internal.ShowGoroutingOnShutdown {
			_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		}
	}()

	if err := daemon.RunWithSignalHandler(run); err != nil {
		logger.Error().Err(err).Msg("Migration failed")

		return 1
	}

	return 0
}

// migrateFlags is the raw command-line input for a migration, before parsing.
type migrateFlags struct {
	repairFrom          string
	repairTo            string
	dualWriteStart      string
	importMutableLabels bool
	throttle            float64
	batchSize           int
}

func parseMigrateOptions(f migrateFlags) (Options, error) {
	opts := Options{
		ThrottleWrites:      f.throttle,
		BatchPoints:         f.batchSize,
		ImportMutableLabels: f.importMutableLabels,
	}

	for _, p := range []struct {
		name string
		raw  string
		dst  *time.Time
	}{
		{"repair-from", f.repairFrom, &opts.RepairFrom},
		{"repair-to", f.repairTo, &opts.RepairTo},
		{"dual-write-start", f.dualWriteStart, &opts.DualWriteStart},
	} {
		if p.raw == "" {
			continue
		}

		t, err := time.Parse(time.RFC3339, p.raw)
		if err != nil {
			return opts, fmt.Errorf("invalid --%s: %w", p.name, err)
		}

		*p.dst = t
	}

	return opts, nil
}

// serveMetrics starts an HTTP server exposing only Prometheus /metrics on addr
// and returns a function that shuts it down. It is metrics-only: no PromQL read
// and no remote-write API are wired.
func serveMetrics(addr string, logger zerolog.Logger) func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// Also expose pprof on the same (dev-only) endpoint, for heap/CPU profiling of
	// a running migration. pprof.Index serves /debug/pprof/ and the named profiles
	// (heap, goroutine, allocs, ...); the others are their own handlers.
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("metrics HTTP server")
		}
	}()

	logger.Info().Str("address", addr).Msg("serving Prometheus /metrics")

	return func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutCtx)
	}
}

// printAllMetrics writes every metric (text format) to stdout — the final,
// global values, useful even for a short migration.
func printAllMetrics(logger zerolog.Logger) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		logger.Warn().Err(err).Msg("gather metrics")
	}

	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(os.Stdout, mf); err != nil {
			logger.Warn().Err(err).Msg("print metrics")

			return
		}
	}
}

func closeBackends(ctx context.Context, squirreldb *daemon.SquirrelDB, logger zerolog.Logger) {
	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if b, err := squirreldb.Backend(closeCtx); err == nil {
		b.CloseAll(closeCtx)
	} else {
		logger.Warn().Err(err).Msg("close source backend")
	}

	if b, err := squirreldb.MigrateToBackend(closeCtx); err == nil {
		b.CloseAll(closeCtx)
	} else {
		logger.Warn().Err(err).Msg("close target backend")
	}
}
