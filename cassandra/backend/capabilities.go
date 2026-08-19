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
	"time"

	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

// The Cassandra backend exposes its TSDB's extra capabilities on the Backend
// itself (never on a wrapped TSDB): the HTTP option parser and the legacy debug
// endpoints (index, flush, forced pre-aggregation). Each delegates to the started
// TSDB, so callers reach them without type-asserting a possibly-wrapped store.

var _ types.HTTPOptionsParser = (*Backend)(nil)

// startedTSDB returns the built TSDB, or nil if TSDB() has not run yet. These
// capabilities are only wired after startup (once the TSDB is built), so nil is a
// defensive guard, not an expected state.
func (b *Backend) startedTSDB() *tsdb.CassandraTSDB {
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

// Index returns the metric index (api.TSDBDebugEndpoint).
func (b *Backend) Index() types.Index {
	t := b.startedTSDB()
	if t == nil {
		return nil
	}

	return t.Index()
}

// Flush drains the temporary store to the TSDB (api.TSDBDebugEndpoint).
func (b *Backend) Flush(ctx context.Context) error {
	t, err := b.TSDB(ctx)
	if err != nil {
		return err
	}

	return t.(*tsdb.CassandraTSDB).Flush(ctx) //nolint:forcetypeassert
}

// ForcePreAggregation recomputes the pre-aggregate over a range
// (api.TSDBDebugEndpoint).
func (b *Backend) ForcePreAggregation(
	ctx context.Context,
	threadCount int,
	from time.Time,
	to time.Time,
	matchers []*labels.Matcher,
) error {
	t, err := b.TSDB(ctx)
	if err != nil {
		return err
	}

	return t.(*tsdb.CassandraTSDB).ForcePreAggregation(ctx, threadCount, from, to, matchers) //nolint:forcetypeassert
}
