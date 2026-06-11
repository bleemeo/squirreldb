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
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/batch"
	"github.com/bleemeo/squirreldb/dummy/temporarystore"
	"github.com/bleemeo/squirreldb/types"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/rs/zerolog"
)

// fakeWriteClient captures the payloads sent to Store.
type fakeWriteClient struct {
	requests []*prompb.WriteRequest
	err      error
}

const tenantLabelForTest = "__acme_account__"

func (f *fakeWriteClient) Store(_ context.Context, req []byte, _ int) (remote.WriteResponseStats, error) {
	if f.err != nil {
		return remote.WriteResponseStats{}, f.err
	}

	buffer, err := snappy.Decode(nil, req)
	if err != nil {
		return remote.WriteResponseStats{}, err
	}

	var wr prompb.WriteRequest
	if err := proto.Unmarshal(buffer, &wr); err != nil {
		return remote.WriteResponseStats{}, err
	}

	f.requests = append(f.requests, &wr)

	return remote.WriteResponseStats{}, nil
}

func (f *fakeWriteClient) Name() string     { return "fake" }
func (f *fakeWriteClient) Endpoint() string { return "fake" }

func newTestTSDB(t *testing.T, writeClient remote.WriteClient) *ClickHouseTSDB {
	t.Helper()

	reg := prometheus.NewRegistry()
	store := temporarystore.New(reg, zerolog.Nop())

	return &ClickHouseTSDB{
		temporaryStore: store,
		batcher:        batch.New(reg, 15*time.Minute, store, zerolog.Nop()),
		writeClient:    writeClient,
		labelsTTL:      time.Hour,
		metrics:        newMetrics(reg, "test"),
		logger:         zerolog.Nop(),
	}
}

// TestAppenderCommit checks that appending then committing stores both the
// labels (for later rebuild) and the points in the temporary store.
func TestAppenderCommit(t *testing.T) {
	ctx := context.Background()
	c := newTestTSDB(t, &fakeWriteClient{})

	lbls := labels.FromStrings("__name__", "cpu_used", tenantLabelForTest, "tenant-1", "item", "/")
	id := types.MetricID(lbls.Hash())

	app := c.AppenderWithOptions(ctx, types.AppenderOptions{WriteTTL: 86400})

	if _, err := app.Append(0, lbls, 1000, 1.5); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if _, err := app.Append(0, lbls, 2000, 2.5); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotLabels, err := c.temporaryStore.GetLabelsByID(ctx, []types.MetricID{id})
	if err != nil {
		t.Fatalf("GetLabelsByID: %v", err)
	}

	if diff := cmp.Diff(lbls.String(), gotLabels[0].String()); diff != "" {
		t.Errorf("stored labels mismatch (-want +got):\n%s", diff)
	}

	data, _, err := c.temporaryStore.ReadPointsAndOffset(ctx, []types.MetricID{id})
	if err != nil {
		t.Fatalf("ReadPointsAndOffset: %v", err)
	}

	wantPoints := []types.MetricPoint{{Timestamp: 1000, Value: 1.5}, {Timestamp: 2000, Value: 2.5}}
	if diff := cmp.Diff(wantPoints, data[0].Points); diff != "" {
		t.Errorf("stored points mismatch (-want +got):\n%s", diff)
	}
}

// TestWriterWrite checks that the flush writer rebuilds labels from the
// temporary store and sends a correct remote_write request.
func TestWriterWrite(t *testing.T) {
	ctx := context.Background()
	fake := &fakeWriteClient{}
	c := newTestTSDB(t, fake)

	lbls := labels.FromStrings("__name__", "cpu_used", tenantLabelForTest, "tenant-1")
	id := types.MetricID(lbls.Hash())

	if err := c.temporaryStore.StoreLabelsByID(ctx, []types.MetricID{id}, []labels.Labels{lbls}, time.Hour); err != nil {
		t.Fatalf("StoreLabelsByID: %v", err)
	}

	metrics := []types.MetricData{{
		ID:         id,
		Points:     []types.MetricPoint{{Timestamp: 1000, Value: 1.5}, {Timestamp: 2000, Value: 2.5}},
		TimeToLive: 86400,
	}}

	if err := (&remoteWriter{c: c}).Write(ctx, metrics); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(fake.requests) != 1 {
		t.Fatalf("expected 1 remote write request, got %d", len(fake.requests))
	}

	want := []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: tenantLabelForTest, Value: "tenant-1"},
			{Name: "__name__", Value: "cpu_used"},
		},
		Samples: []prompb.Sample{{Timestamp: 1000, Value: 1.5}, {Timestamp: 2000, Value: 2.5}},
	}}

	if diff := cmp.Diff(want, fake.requests[0].Timeseries); diff != "" {
		t.Errorf("remote write timeseries mismatch (-want +got):\n%s", diff)
	}
}

func TestSeriesToInput(t *testing.T) {
	lbls := labels.FromStrings("__name__", "cpu_used", tenantLabelForTest, "tenant-1", "item", "/")

	t.Run("per-metric TTL and sorted tags", func(t *testing.T) {
		name, tags, ttlDays := seriesToInput(lbls, 7*86400, 365)

		if name != "cpu_used" {
			t.Errorf("metric name = %q, want cpu_used", name)
		}

		// __name__ is excluded; the remaining keys must be in sorted order so the
		// id ClickHouse computes is stable across flushes.
		wantKeys := []string{tenantLabelForTest, "item"}
		wantValues := []string{"tenant-1", "/"}

		if diff := cmp.Diff(wantKeys, tags.keys); diff != "" {
			t.Errorf("all_tags keys mismatch (-want +got):\n%s", diff)
		}

		if diff := cmp.Diff(wantValues, tags.values); diff != "" {
			t.Errorf("all_tags values mismatch (-want +got):\n%s", diff)
		}

		if ttlDays != 7 {
			t.Errorf("row_ttl_days = %d, want 7", ttlDays)
		}
	})

	t.Run("falls back to global default when TTL is zero or sub-day", func(t *testing.T) {
		if _, _, ttlDays := seriesToInput(lbls, 0, 365); ttlDays != 365 {
			t.Errorf("row_ttl_days = %d, want 365 (global default)", ttlDays)
		}

		if _, _, ttlDays := seriesToInput(lbls, 3600, 365); ttlDays != 365 {
			t.Errorf("row_ttl_days = %d, want 365 (sub-day TTL falls back)", ttlDays)
		}
	})
}

// TestWriterWriteMissingLabels checks that metrics whose labels are missing
// (e.g. expired) are dropped and do not produce a remote_write request.
func TestWriterWriteMissingLabels(t *testing.T) {
	ctx := context.Background()
	fake := &fakeWriteClient{}
	c := newTestTSDB(t, fake)

	metrics := []types.MetricData{{
		ID:     types.MetricID(1234),
		Points: []types.MetricPoint{{Timestamp: 1000, Value: 1.5}},
	}}

	if err := (&remoteWriter{c: c}).Write(ctx, metrics); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(fake.requests) != 0 {
		t.Fatalf("expected no remote write request, got %d", len(fake.requests))
	}
}
