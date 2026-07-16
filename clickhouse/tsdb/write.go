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
	"math"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/prometheus/prometheus/model/labels"
)

const secondsPerDay = 24 * 60 * 60

// seriesColumns are the datapoint_input / tsdb_tags columns derived from a
// series' labels.
type seriesColumns struct {
	metricName string
	tenant     string
	tags       *sortedTags // non-__name__, non-tenant labels, in sorted key order
	rowTTLDays uint16
}

// seriesToInput splits a series' labels into the metric name, the tenant value
// (the tenantLabel, kept out of tags), the remaining labels as the tags map, and
// the per-row TTL in days (falling back to the global default below one day).
//
// tags is built in a deterministic (sorted) key order so the series id ClickHouse
// computes (sipHash128(metric_name, tenant, tags)) is stable across flushes —
// clickhouse-go serializes a plain Go map in random order, which would otherwise
// yield a different id (and thus duplicate series) each flush. We rely on
// labels.Labels being canonically sorted by name (a Prometheus invariant).
func seriesToInput(lbls labels.Labels, tenantLabel string, ttlSeconds int64, globalDays uint16) seriesColumns {
	cols := seriesColumns{
		metricName: lbls.Get(labels.MetricName),
		tags:       &sortedTags{},
		rowTTLDays: rowTTLDays(ttlSeconds, globalDays),
	}

	lbls.Range(func(l labels.Label) {
		switch l.Name {
		case labels.MetricName:
		case tenantLabel:
			cols.tenant = l.Value
		default:
			cols.tags.keys = append(cols.tags.keys, l.Name)
			cols.tags.values = append(cols.tags.values, l.Value)
		}
	})

	return cols
}

// rowTTLDays converts a per-request TTL (seconds) to whole days for the
// row_ttl_days column. The store's TTL granularity is one day (ClickHouse stores
// the retention in days, unlike Cassandra's seconds), so this is where the TTL is
// quantized: to the NEAREST day, not ceil. Rounding matters because the source
// retention can carry sub-day noise — e.g. the migration reconstructs it a
// read-duration after sampling Cassandra's whole-second remaining — and ceil would
// turn a few seconds of that noise into a whole extra day. A
// positive sub-day TTL still keeps at least 1 day (never 0 = immediate drop). Only
// a non-positive TTL (none requested) falls back to the global default; a positive
// TTL is always honored (never silently replaced by the much larger default). The
// result is capped at the uint16 column range.
func rowTTLDays(ttlSeconds int64, globalDays uint16) uint16 {
	if ttlSeconds <= 0 {
		return globalDays
	}

	days := (ttlSeconds + secondsPerDay/2) / secondsPerDay // round half up
	days = max(days, 1)                                    // a positive TTL keeps at least a day

	if days > math.MaxUint16 {
		return math.MaxUint16
	}

	return uint16(days)
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
