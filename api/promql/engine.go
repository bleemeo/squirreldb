// Copyright 2015-2025 Bleemeo
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

package promql

import (
	"context"
	"fmt"
	"time"

	"github.com/bleemeo/squirreldb/logger"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
	"github.com/thanos-io/promql-engine/engine"
)

func NewEngine(
	queryLogger zerolog.Logger,
	useThanosPromQLEngine bool,
	metricRegistry prometheus.Registerer,
) promql.QueryEngine {
	engineOpts := promql.EngineOpts{
		Logger:               logger.NewSLogger(queryLogger),
		Reg:                  metricRegistry,
		MaxSamples:           50000000,
		Timeout:              2 * time.Minute,
		ActiveQueryTracker:   nil,
		LookbackDelta:        5 * time.Minute,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
		EnablePerStepStats:   false,
	}

	var queryEngine promql.QueryEngine

	if useThanosPromQLEngine {
		queryLogger.Info().Msg("Using Thanos PromQL engine")

		queryEngine = engine.New(engine.Opts{
			EngineOpts:        engineOpts,
			LogicalOptimizers: nil,
		})
	} else {
		queryEngine = promql.NewEngine(engineOpts)
	}

	return queryEngine
}

func WrapEngine(engine promql.QueryEngine, logger zerolog.Logger) WrapperEngine {
	return WrapperEngine{engine, logger}
}

type WrapperEngine struct {
	promql.QueryEngine

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
