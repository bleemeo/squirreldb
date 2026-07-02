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
	"errors"
	"fmt"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

var (
	errNotImplemented      = errors.New("not implemented")
	errTimestampOutOfRange = errors.New("timestamp out of storable range")
	errClosed              = errors.New("clickhouse tsdb is closed")
)

// appender accumulates the points of a single remote write request, keyed by
// label hash to merge repeated samples of the same series, and on Commit appends
// them (labels + points) to the shared Redis WAL. The drainer later writes the WAL
// to ClickHouse; there is no metric-index lookup.
type appender struct {
	c   *ClickHouseTSDB
	ttl int64
	ctx context.Context //nolint:containedctx

	// Pending timeseries indexed by their labels hash.
	pendingTimeSeries map[uint64]types.Timeseries
}

func (app *appender) Append(
	_ storage.SeriesRef,
	l labels.Labels,
	t int64,
	v float64,
) (storage.SeriesRef, error) {
	// Reject (rather than clamp) a timestamp the store cannot represent: a point
	// timestamp is an int64 of milliseconds, a wider range than the DateTime64(3)
	// columns. Clamping would silently store the point at the wrong time, so we
	// surface it as an error to the writing client instead.
	if t < storeMinTimeMillis || t > storeMaxTimeMillis {
		return 0, fmt.Errorf(
			"%w: %d ms is outside [%d, %d]", errTimestampOutOfRange, t, storeMinTimeMillis, storeMaxTimeMillis,
		)
	}

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

func (app *appender) Commit() error {
	defer func() {
		app.pendingTimeSeries = make(map[uint64]types.Timeseries)
	}()

	if len(app.pendingTimeSeries) == 0 {
		return nil
	}

	// One WAL entry per point (not per series), so the WAL list length is the exact
	// buffered-point count. A series with N points therefore produces N entries that
	// repeat its labels; the drainer regroups them into one arrays-per-series insert,
	// so the label repetition costs only transient Redis memory, never ClickHouse
	// write efficiency.
	var entries []walEntry

	for _, ts := range app.pendingTimeSeries {
		for _, p := range ts.Points {
			entries = append(entries, walEntry{Labels: ts.Labels, TTL: app.ttl, Points: []types.MetricPoint{p}})
		}
	}

	// Slow (or, at the stop threshold, block) before appending when the WAL is
	// backed up, so a lagging drainer bounds the WAL instead of letting it grow
	// unbounded. Applied before the append so a client timeout during it leaves
	// nothing written.
	if err := app.c.applyBackpressure(app.ctx); err != nil {
		return fmt.Errorf("wal backpressure: %w", err)
	}

	walLen, err := app.c.wal.Append(app.ctx, entries)
	if err != nil {
		return fmt.Errorf("append to wal: %w", err)
	}

	app.c.lastWALLen.Store(walLen)

	return nil
}

func (app *appender) Rollback() error {
	app.pendingTimeSeries = make(map[uint64]types.Timeseries)

	return nil
}

func (app *appender) SetOptions(*storage.AppendOptions) {}

func (app *appender) AppendExemplar(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) AppendHistogram(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ *histogram.Histogram,
	_ *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) AppendHistogramCTZeroSample(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ int64,
	_ *histogram.Histogram,
	_ *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) UpdateMetadata(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ metadata.Metadata,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}

func (app *appender) AppendCTZeroSample(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ int64,
) (storage.SeriesRef, error) {
	return 0, errNotImplemented
}
