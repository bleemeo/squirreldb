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

package index

import (
	"context"
	"errors"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// These accessors expose the read internals the Cassandra→ClickHouse migration
// needs (the `squirreldb migrate` subcommand). They are read-only and go
// straight to the postings / labels store, reusing the query-side helpers.

// MigrationShards returns every existing time-shard number, ascending. A
// migrator iterates them (typically newest→oldest) to copy data shard by shard.
// It reads the global existing-shards posting directly (no cache) — appropriate
// for a one-shot enumeration.
func (c *CassandraIndex) MigrationShards(ctx context.Context) ([]int32, error) {
	bitmap, err := c.postings(ctx, []int32{globalShardNumber}, existingShardsLabel, existingShardsLabel, false)
	if err != nil {
		return nil, err
	}

	slice := bitmap.Slice() // ascending
	shards := make([]int32, len(slice))

	for i, v := range slice {
		shards[i] = int32(v) //nolint:gosec // shard numbers are small positive ints
	}

	return shards, nil
}

// MigrationShardExpiration returns when the shard's index postings are scheduled
// to be dropped. That date is set to outlive the raw data, so a past expiration
// means the data has already expired. The zero time means the shard has no
// expiration entry: either it is not created yet (it is only set on the first
// metric expiration refresh, about a day after the metrics are created), or it
// was lost (pre-feature or orphaned shard).
func (c *CassandraIndex) MigrationShardExpiration(ctx context.Context, shard int32) (time.Time, error) {
	expiration, _, err := c.getShardExpirationFromStore(ctx, shard)
	if errors.Is(err, errMetricDoesNotExist) {
		return time.Time{}, nil
	}

	if err != nil {
		return time.Time{}, err
	}

	return expiration, nil
}

// MigrationShardWidth is the duration each migration time-shard covers (the
// posting shard size). Exposed so a shard's start time can be turned into its end
// without re-deriving the shard number.
const MigrationShardWidth = postingShardSize

// ShardTimeRange returns the [start, end) time window a shard covers. A metric
// present in the shard (see MigrationSeriesInShard) has at least one point in
// this window; the migrator reads that window from the raw data.
func ShardTimeRange(shard int32) (start, end time.Time) {
	start = timeForShard(shard)

	return start, start.Add(postingShardSize)
}

// MigrationSeriesInShard returns the metric ids present in a time-shard.
func (c *CassandraIndex) MigrationSeriesInShard(ctx context.Context, shard int32) ([]types.MetricID, error) {
	bitmap, err := c.postings(ctx, []int32{shard}, allPostingLabel, allPostingLabel, false)
	if err != nil {
		return nil, err
	}

	return bitsetToIDs(bitmap), nil
}

// MigrationLabels resolves metric ids to their stored labels (in the same order
// as ids), using the same LRU-cached path as the query read side. These are the
// labels actually stored — mutable-label aliasing is a query-time concern and is
// intentionally bypassed — which is exactly what the migrator copies to
// ClickHouse (which re-derives its series id from them).
func (c *CassandraIndex) MigrationLabels(ctx context.Context, ids []types.MetricID) ([]labels.Labels, error) {
	return c.lookupLabels(ctx, ids, time.Now())
}
