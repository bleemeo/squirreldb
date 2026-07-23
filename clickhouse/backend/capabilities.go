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
	"net/http"

	"github.com/bleemeo/squirreldb/clickhouse/tsdb"
	"github.com/bleemeo/squirreldb/export"
	"github.com/bleemeo/squirreldb/types"
)

// The ClickHouse backend exposes its TSDB's extra capabilities on the Backend
// itself (never on a wrapped TSDB): the HTTP option parser (ForceRaw /
// ForcePreAggregated headers) and the raw importer used by the migration target.
// Each delegates to the started TSDB, so callers reach them without type-asserting
// a possibly-wrapped store. (ClickHouse has no index / forced pre-aggregation, so
// it does not implement the legacy api.TSDBDebugEndpoint.)

var (
	_ types.HTTPOptionsParser = (*Backend)(nil)
	_ export.RawImporter      = (*Backend)(nil)
)

// startedTSDB returns the built TSDB, or nil if TSDB() has not run yet.
func (b *Backend) startedTSDB() *tsdb.ClickHouseTSDB {
	b.l.Lock()
	defer b.l.Unlock()

	return b.tsdb
}

// ParseHTTPOptions parses per-request backend options from HTTP headers
// (types.HTTPOptionsParser).
func (b *Backend) ParseHTTPOptions(header http.Header) (any, error) {
	t := b.startedTSDB()
	if t == nil {
		return nil, fmt.Errorf("%w: cannot parse HTTP options", ErrNotStarted)
	}

	return t.ParseHTTPOptions(header)
}

// ImportRawData writes a batch of series-chunks straight to ClickHouse, bypassing
// the WAL (export.RawImporter) — the migration target path.
func (b *Backend) ImportRawData(ctx context.Context, chunks []export.ExportChunk) error {
	t, err := b.TSDB(ctx)
	if err != nil {
		return err
	}

	return t.(*tsdb.ClickHouseTSDB).ImportRawData(ctx, chunks) //nolint:forcetypeassert
}
