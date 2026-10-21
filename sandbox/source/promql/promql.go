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

// Package promql is the sandbox source of series whose value is a PromQL
// expression, evaluated at each step (e.g. "time() + 86400*30" for a
// certificate that always expires in 30 days).
package promql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/sandbox/dataset"
	"github.com/bleemeo/squirreldb/sandbox/source"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

// queryTimeout bounds the evaluation of the expression for a single point.
const queryTimeout = 10 * time.Second

var (
	ErrInvalidStepSeconds = errors.New("step seconds should be a strictly positive integer")
	ErrUnexpectedResult   = errors.New("query must return a scalar or a single sample")
)

// Metric is a series whose value is the result of Query.
type Metric struct {
	Labels      labels.Labels
	StepSeconds int
	Query       string
}

// Load parses the PromQL series of a dataset definition.
func Load(promqlConfig dataset.Promql) ([]Metric, error) {
	metrics := make([]Metric, len(promqlConfig))

	parserInstance := parser.NewParser(parser.Options{})

	for i, metric := range promqlConfig {
		lbls, err := parserInstance.ParseMetric(metric.Name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", metric.Name, err)
		}

		if metric.StepSeconds <= 0 {
			return nil, fmt.Errorf("metric %s: %w", metric.Name, ErrInvalidStepSeconds)
		}

		if _, err := parserInstance.ParseExpr(metric.Query); err != nil {
			return nil, fmt.Errorf("metric %s: invalid query: %w", metric.Name, err)
		}

		metrics[i] = Metric{
			Labels:      lbls,
			StepSeconds: metric.StepSeconds,
			Query:       metric.Query,
		}
	}

	return metrics, nil
}

// NewEngine returns a small PromQL engine suitable to evaluate the expressions of
// the PromQL series. It registers no metric, so it doesn't conflict with the
// engine of the API.
func NewEngine(lgr zerolog.Logger) promql.QueryEngine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:               logger.NewSLogger(lgr),
		MaxSamples:           1000000,
		Timeout:              queryTimeout,
		LookbackDelta:        5 * time.Minute,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})
}

// Series returns the metrics as sandbox series. Their expressions are evaluated
// by engine against queryable, so they may use the series of queryable.
func Series(
	metrics []Metric,
	engine promql.QueryEngine,
	queryable storage.Queryable,
	lgr zerolog.Logger,
) []source.Series {
	result := make([]source.Series, len(metrics))

	for i, m := range metrics {
		result[i] = source.Series{
			Labels: m.Labels,
			Step:   int64(m.StepSeconds) * 1000,
			Value: func(ctx context.Context, ts int64) float64 {
				value, err := executeQuery(ctx, engine, queryable, m.Query, ts)
				if err != nil {
					lgr.Err(err).Str("metric", m.Labels.String()).Msg("Failed to evaluate the PromQL series")

					return 0
				}

				return value
			},
		}
	}

	return result
}

func executeQuery(
	ctx context.Context,
	engine promql.QueryEngine,
	queryable storage.Queryable,
	queryStr string,
	ts int64,
) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	query, err := engine.NewInstantQuery(ctx, queryable, nil, queryStr, time.UnixMilli(ts))
	if err != nil {
		return 0, fmt.Errorf("failed to parse promql metric query: %w", err)
	}

	defer query.Close()

	result := query.Exec(ctx)
	if result.Err != nil {
		return 0, fmt.Errorf("failed to execute promql metric query: %w", result.Err)
	}

	switch value := result.Value.(type) {
	case promql.Scalar:
		return value.V, nil
	case promql.Vector:
		if len(value) == 1 {
			return value[0].F, nil
		}

		return 0, fmt.Errorf("%w: got %d samples", ErrUnexpectedResult, len(value))
	default:
		return 0, fmt.Errorf("%w: got %s", ErrUnexpectedResult, result.Value.Type())
	}
}
