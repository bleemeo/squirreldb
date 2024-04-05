package promql

import (
	"context"
	"fmt"
	"squirreldb/logger"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
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

func WrapEngine(engine v1.QueryEngine, logger zerolog.Logger) WrapperEngine {
	return WrapperEngine{engine, logger}
}

type WrapperEngine struct {
	v1.QueryEngine
	logger zerolog.Logger
}

func (e WrapperEngine) NewInstantQuery(
	ctx context.Context,
	q storage.Queryable,
	opts promql.QueryOpts,
	qs string,
	ts time.Time,
) (promql.Query, error) {
	query, err := e.QueryEngine.NewInstantQuery(ctx, q, opts, qs, ts)

	optsStr := "noOptions"
	if opts != nil {
		optsStr = fmt.Sprintf("options=%#v", opts)
	}

	query = wrapperQuery{
		Query: query,
		logMessage: fmt.Sprintf(
			"Start query %s qs=%s ts=%s",
			optsStr,
			qs,
			ts.Format(time.RFC3339),
		),
		logger: e.logger,
	}

	return query, err
}

func (e WrapperEngine) NewRangeQuery(
	ctx context.Context,
	q storage.Queryable,
	opts promql.QueryOpts,
	qs string,
	start,
	end time.Time,
	interval time.Duration,
) (promql.Query, error) {
	query, err := e.QueryEngine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)

	optsStr := "noOptions"
	if opts != nil {
		optsStr = fmt.Sprintf("options=%#v", opts)
	}

	query = wrapperQuery{
		Query: query,
		logMessage: fmt.Sprintf(
			"Start query_range %s qs=%s start/end/interval=%s to %s interval %s",
			optsStr,
			qs,
			start.Format(time.RFC3339),
			end.Format(time.RFC3339),
			interval.String(),
		),
		logger: e.logger,
	}

	return query, err
}

type wrapperQuery struct {
	promql.Query
	logMessage string
	logger     zerolog.Logger
}

func (q wrapperQuery) Exec(ctx context.Context) *promql.Result {
	perRequestData, err := getPerRequestData(ctx)
	if err == nil && perRequestData.enableDebug {
		q.logger.Info().Msg(q.logMessage)
	}

	return q.Query.Exec(ctx)
}
