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

package tsdb

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

type batchingAppender struct {
	c   *CassandraTSDB
	ttl int64
	ctx context.Context //nolint:containedctx

	// Map of pending timeseries indexed by their labels hash.
	pendingTimeSeries map[uint64]types.Timeseries
}

func (app *batchingAppender) Append(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	v float64,
) (storage.SeriesRef, error) {
	labelsHash := l.Hash()

	ts, ok := app.pendingTimeSeries[labelsHash]
	if !ok {
		app.pendingTimeSeries[labelsHash] = types.Timeseries{
			Labels: l,
			Points: []types.MetricPoint{{
				Timestamp: t,
				Value:     v,
			}},
		}
	} else {
		ts.Points = append(ts.Points, types.MetricPoint{
			Timestamp: t,
			Value:     v,
		})
		app.pendingTimeSeries[labelsHash] = ts
	}

	return 0, nil
}

func (app *batchingAppender) Commit() error {
	defer func() {
		app.pendingTimeSeries = make(map[uint64]types.Timeseries)
	}()

	pendingTimeSeries := slices.Collect(maps.Values(app.pendingTimeSeries))

	metrics, err := app.resolveMetricIDs(app.ctx, pendingTimeSeries)
	if err != nil {
		return fmt.Errorf("unable to convert metrics: %w", err)
	}

	if err := app.c.batcher.Write(app.ctx, app.c.writerInterface(), metrics); err != nil {
		return fmt.Errorf("unable to write metrics: %w", err)
	}

	return nil
}

func (app *batchingAppender) Rollback() error {
	app.pendingTimeSeries = make(map[uint64]types.Timeseries)

	return nil
}

func (app *batchingAppender) SetOptions(opts *storage.AppendOptions) {
}

func (app *batchingAppender) AppendExemplar(
	ref storage.SeriesRef,
	l labels.Labels,
	e exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) AppendHistogram(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) AppendHistogramSTZeroSample(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	st int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) UpdateMetadata(
	ref storage.SeriesRef,
	l labels.Labels,
	m metadata.Metadata,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) AppendSTZeroSample(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	st int64,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *batchingAppender) resolveMetricIDs(
	ctx context.Context,
	pendingTimeSeries []types.Timeseries,
) ([]types.MetricData, error) {
	if len(pendingTimeSeries) == 0 {
		return nil, nil
	}

	idToIndex := make(map[types.MetricID]int, len(pendingTimeSeries))
	metrics := make([]types.MetricData, 0, len(pendingTimeSeries))
	requests := make([]types.LookupRequest, 0, len(pendingTimeSeries))

	for _, promSeries := range pendingTimeSeries {
		tMin := int64(math.MaxInt64)
		tMax := int64(math.MinInt64)

		for _, s := range promSeries.Points {
			if tMin > s.Timestamp {
				tMin = s.Timestamp
			}

			if tMax < s.Timestamp {
				tMax = s.Timestamp
			}
		}

		if tMin < time.Now().Add(-MaxPastDelay).Unix()*1000 {
			app.c.warnPointTooOld(tMin)
		}

		requests = append(requests, types.LookupRequest{
			Labels:     promSeries.Labels,
			TTLSeconds: app.ttl,
			End:        time.Unix(tMax/1000, tMax%1000),
			Start:      time.Unix(tMin/1000, tMin%1000),
		})
	}

	ids, ttls, err := app.c.index.LookupIDs(ctx, requests)
	if err != nil {
		return nil, fmt.Errorf("metric ID lookup failed: %w", err)
	}

	for i, promSeries := range pendingTimeSeries {
		data := types.MetricData{
			ID:         ids[i],
			Points:     promSeries.Points,
			TimeToLive: ttls[i],
		}

		if idx, found := idToIndex[data.ID]; found {
			metrics[idx].Points = append(metrics[idx].Points, data.Points...)
			if metrics[idx].TimeToLive < data.TimeToLive {
				metrics[idx].TimeToLive = data.TimeToLive
			}
		} else {
			metrics = append(metrics, data)
			idToIndex[data.ID] = len(metrics) - 1
		}
	}

	return metrics, nil
}
