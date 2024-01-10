package promql

import (
	"squirreldb/logger"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/rs/zerolog"
	"github.com/thanos-io/promql-engine/engine"
)

func NewEngine(
	queryLogger zerolog.Logger,
	useThanosPromQLEngine bool,
	metricRegistry prometheus.Registerer,
) v1.QueryEngine {
	engineOpts := promql.EngineOpts{
		Logger:               logger.NewKitLogger(&queryLogger),
		Reg:                  metricRegistry,
		MaxSamples:           50000000,
		Timeout:              2 * time.Minute,
		ActiveQueryTracker:   nil,
		LookbackDelta:        5 * time.Minute,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		EnablePerStepStats:   false,
	}

	var queryEngine v1.QueryEngine

	if useThanosPromQLEngine {
		queryLogger.Info().Msg("Using Thanos PromQL engine")

		queryEngine = engine.New(engine.Opts{
			EngineOpts:        engineOpts,
			LogicalOptimizers: nil,
			DisableFallback:   false,
		})
	} else {
		queryEngine = promql.NewEngine(engineOpts)
	}

	return queryEngine
}
