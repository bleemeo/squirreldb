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
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/prometheus/prometheus/model/labels"
)

const secondsPerDay = 24 * 60 * 60

// sqlWriter implements types.MetricWriter and is used by the batcher to flush
// batched points to ClickHouse through SQL. It inserts raw rows
// into the Null-engine datapoint_input table; materialized views compute the
// series id and fan them out into tsdb_data and tsdb_tags.
type sqlWriter struct {
	c *ClickHouseTSDB
}

func (w *sqlWriter) Write(ctx context.Context, metrics []types.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		w.c.metrics.FlushSeconds.Observe(time.Since(start).Seconds())
	}()

	ids := make([]types.MetricID, len(metrics))
	for i, m := range metrics {
		ids[i] = m.ID
	}

	labelsList, err := w.c.temporaryStore.GetLabelsByID(ctx, ids)
	if err != nil {
		return fmt.Errorf("get labels: %w", err)
	}

	batch, err := w.c.conn.PrepareBatch(ctx, "INSERT INTO datapoint_input")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	defer batch.Close()

	globalDays := uint16(w.c.ttlDays()) //nolint:gosec
	rows := 0

	var missingLabels int

	for i, m := range metrics {
		lbls := labelsList[i]
		if lbls.Len() == 0 {
			missingLabels++

			continue
		}

		metricName, allTags, rowTTLDays := seriesToInput(lbls, m.TimeToLive, globalDays)

		for _, p := range m.Points {
			if err := batch.Append(metricName, allTags, time.UnixMilli(p.Timestamp), p.Value, rowTTLDays); err != nil {
				_ = batch.Abort()

				w.c.metrics.FlushErrorsTotal.Inc()

				return fmt.Errorf("append row: %w", err)
			}

			rows++
		}
	}

	if missingLabels > 0 {
		w.c.logger.Warn().
			Int("count", missingLabels).
			Msg("Missing labels for some metrics at flush time, their points were dropped")
	}

	if rows == 0 {
		return batch.Abort()
	}

	if err := batch.Send(); err != nil {
		w.c.metrics.FlushErrorsTotal.Inc()

		return fmt.Errorf("send batch to clickhouse: %w", err)
	}

	w.c.metrics.FlushPointsTotal.Add(float64(rows))

	return nil
}

// seriesToInput maps a series' labels to the datapoint_input columns: the metric
// name, the other labels as the all_tags map (the tenant label stays in the map;
// the materialized view extracts it), and the per-row TTL in days. A metric
// without an explicit TTL (or below one day) falls back to the global default.
//
// all_tags is built in a deterministic (sorted) key order so that the series id
// ClickHouse computes (sipHash128(metric_name, all_tags)) is stable across
// flushes — clickhouse-go serializes a plain Go map in random order, which would
// otherwise yield a different id (and thus duplicate series) each flush. We rely
// on labels.Labels being canonically sorted by name (a Prometheus invariant).
func seriesToInput(lbls labels.Labels, ttlSeconds int64, globalDays uint16) (string, *sortedTags, uint16) {
	metricName := lbls.Get(labels.MetricName)

	allTags := &sortedTags{}

	lbls.Range(func(l labels.Label) {
		if l.Name != labels.MetricName {
			allTags.keys = append(allTags.keys, l.Name)
			allTags.values = append(allTags.values, l.Value)
		}
	})

	rowTTLDays := globalDays
	if days := uint16(ttlSeconds / secondsPerDay); days > 0 { //nolint:gosec
		rowTTLDays = days
	}

	return metricName, allTags, rowTTLDays
}

// sortedTags is an ordered string->string map preserving its insertion order
// when serialized by clickhouse-go (it implements column.IterableOrderedMap),
// unlike a plain Go map which is serialized in random order.
type sortedTags struct {
	keys   []string
	values []string
}

// Ensure clickhouse-go uses our ordering (its IterableOrderedMap path) instead
// of serializing a Go map in random order.
var _ column.IterableOrderedMap = (*sortedTags)(nil)

func (t *sortedTags) Put(key, value any) {
	t.keys = append(t.keys, key.(string))       //nolint:forcetypeassert
	t.values = append(t.values, value.(string)) //nolint:forcetypeassert
}

func (t *sortedTags) Iterator() column.MapIterator {
	return &sortedTagsIterator{tags: t, index: -1}
}

type sortedTagsIterator struct {
	tags  *sortedTags
	index int
}

func (it *sortedTagsIterator) Next() bool {
	it.index++

	return it.index < len(it.tags.keys)
}

func (it *sortedTagsIterator) Key() any   { return it.tags.keys[it.index] }
func (it *sortedTagsIterator) Value() any { return it.tags.values[it.index] }
