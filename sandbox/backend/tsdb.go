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

package backend

import (
	"context"
	"fmt"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/sandbox/dataset"
	"github.com/bleemeo/squirreldb/sandbox/source"
	"github.com/bleemeo/squirreldb/sandbox/source/file"
	"github.com/bleemeo/squirreldb/sandbox/source/promql"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

// sandboxTSDB is the types.TSDB of the sandbox backend. Reads are served from
// the dataset; writes are accepted and discarded.
type sandboxTSDB struct {
	// series are the series of both sources, sorted by labels.
	series      []source.Series
	tenantLabel string
	metrics     *metrics
}

// newTSDB loads the dataset and builds its series. The PromQL series are
// evaluated by an engine private to the sandbox, against the file series.
func newTSDB(ds *dataset.Dataset, tenantLabel string, m *metrics, logger zerolog.Logger) (*sandboxTSDB, error) {
	store, warnings, err := file.Load(ds.Config.Files, ds.Open)
	if err != nil {
		return nil, fmt.Errorf("load file source: %w", err)
	}

	for _, w := range warnings {
		logger.Warn().Err(w).Msg("Series of the dataset ignored")
	}

	promqlMetrics, err := promql.Load(ds.Config.Promql)
	if err != nil {
		return nil, fmt.Errorf("load promql source: %w", err)
	}

	fileSeries := source.Merge(store.Series())
	promqlLogger := logger.With().Str("component", "promql_source").Logger()
	promqlSeries := promql.Series(
		promqlMetrics,
		promql.NewEngine(promqlLogger),
		source.Queryable{Series: fileSeries},
		promqlLogger,
	)

	return &sandboxTSDB{
		// A PromQL series takes precedence over a file series with the same labels.
		series:      source.Merge(promqlSeries, fileSeries),
		tenantLabel: tenantLabel,
		metrics:     m,
	}, nil
}

// QuerierWithOptions returns a querier on the dataset. With a tenant, every
// series carries the tenant label: all tenants see the same dataset.
func (t *sandboxTSDB) QuerierWithOptions(opts types.QuerierOptions, mint, maxt int64) (storage.Querier, error) {
	querierOptions := source.QuerierOptions{
		OnClose: t.metrics.observeQuery,
	}

	if t.tenantLabel != "" && opts.Tenant != "" {
		querierOptions.TenantLabel = labels.Label{Name: t.tenantLabel, Value: opts.Tenant}
	}

	return source.NewQuerier(t.series, mint, maxt, querierOptions), nil
}

func (t *sandboxTSDB) ChunkQuerierWithOptions(
	opts types.QuerierOptions,
	mint, maxt int64,
) (storage.ChunkQuerier, error) {
	q, err := t.QuerierWithOptions(opts, mint, maxt)
	if err != nil {
		return nil, err
	}

	return convert.NewQuerierToChunkQuerier(q), nil
}

// AppenderWithOptions returns an appender discarding every point.
func (t *sandboxTSDB) AppenderWithOptions(ctx context.Context, opts types.AppenderOptions) storage.Appender {
	return dummy.DiscardTSDB{}.AppenderWithOptions(ctx, opts)
}

func (t *sandboxTSDB) Close(context.Context) error {
	return nil
}
