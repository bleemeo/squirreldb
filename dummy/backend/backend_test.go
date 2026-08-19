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
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

// flusher is implemented by the batcher-backed TSDB so the test can force a
// flush (exercising the writer).
type flusher interface {
	Flush(ctx context.Context) error
}

// TestDiscardWrites checks that the dummy backend accepts writes and serves
// empty reads, both without and with the batcher.
func TestDiscardWrites(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		batcher bool
	}{
		{name: "no-batcher", batcher: false},
		{name: "batcher", batcher: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			b, err := New(ctx, zerolog.Nop(), Options{
				MetricRegistry: prometheus.NewRegistry(),
				TenantLabel:    "__account_id",
				Batch:          config.Batch{Size: time.Minute},
				Batcher:        tc.batcher,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if err := b.Start(ctx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			defer b.CloseAll(ctx)

			tsdb, err := b.TSDB(ctx)
			if err != nil {
				t.Fatalf("TSDB: %v", err)
			}

			app := tsdb.AppenderWithOptions(ctx, types.AppenderOptions{WriteTTL: 86400})

			lbls := labels.FromStrings("__name__", "cpu_used", "__account_id", "1234")
			if _, err := app.Append(0, lbls, time.Now().UnixMilli(), 42); err != nil {
				t.Fatalf("Append: %v", err)
			}

			if err := app.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			// Force a flush so the writer runs and must not error.
			if f, ok := tsdb.(flusher); ok {
				if err := f.Flush(ctx); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}

			// Reads always return nothing: the dummy backend stores no point.
			querier, err := tsdb.QuerierWithOptions(types.QuerierOptions{}, 0, time.Now().UnixMilli())
			if err != nil {
				t.Fatalf("QuerierWithOptions: %v", err)
			}

			defer querier.Close()

			set := querier.Select(ctx, false, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu_used"))
			if set.Next() {
				t.Fatal("expected no series from the dummy backend, got at least one")
			}

			if err := set.Err(); err != nil {
				t.Fatalf("series set error: %v", err)
			}
		})
	}
}
