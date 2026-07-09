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

	"github.com/bleemeo/squirreldb/types"
)

// MigrationReadRaw returns the raw points of one metric over
// [fromTimestamp, toTimestamp] (milliseconds) and the maximum remaining
// Cassandra TTL (seconds) among the rows read. Used by the `squirreldb migrate`
// subcommand to copy data into ClickHouse.
//
// It reads only the raw `data` table, never the pre-aggregated table: raw holds
// every point (the aggregated table is only a query-performance copy with the
// same retention), so raw is the complete source. Points are deduplicated by
// timestamp exactly like the query read path (this goes through the same
// readRawData), so the migrator gets what a Cassandra read would serve and never
// feeds duplicate (id, timestamp) points to ClickHouse.
//
// The remaining TTL is the whole range's maximum; the caller turns it into
// ClickHouse's per-metric row TTL by reconstructing the original retention
// (remaining + point age), which is ~constant per series. Using the maximum can
// over-retain the very oldest points in the range by well under a day — within
// the accepted rounding tolerance.
func (c *CassandraTSDB) MigrationReadRaw(
	ctx context.Context,
	id types.MetricID,
	fromTimestamp, toTimestamp int64,
) (points []types.MetricPoint, ttlSeconds int64, err error) {
	data, _, err := c.readRawData(ctx, id, types.MetricData{}, fromTimestamp, toTimestamp, nil, "")
	if err != nil {
		return nil, 0, err
	}

	return data.Points, data.TimeToLive, nil
}
