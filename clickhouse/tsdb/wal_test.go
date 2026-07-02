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
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/prometheus/model/labels"
)

func TestWALEntryRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []walEntry{
		{
			Labels: labels.FromStrings("__name__", "cpu_used", "item", "/", "tricky", "a=b\nc\\d"),
			TTL:    86400,
			Points: []types.MetricPoint{{Timestamp: 1000, Value: 1.5}, {Timestamp: 2000, Value: -3.25}},
		},
		{
			// No points, single label.
			Labels: labels.FromStrings("__name__", "up"),
			TTL:    0,
			Points: nil,
		},
		{
			// No labels at all.
			Labels: labels.EmptyLabels(),
			TTL:    3600,
			Points: []types.MetricPoint{{Timestamp: 42, Value: 0}},
		},
	}

	for i, want := range cases {
		got, err := decodeWALEntry(encodeWALEntry(want))
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}

		if !labels.Equal(got.Labels, want.Labels) {
			t.Errorf("case %d: labels = %v, want %v", i, got.Labels, want.Labels)
		}

		if got.TTL != want.TTL {
			t.Errorf("case %d: ttl = %d, want %d", i, got.TTL, want.TTL)
		}

		if diff := cmp.Diff(want.Points, got.Points); diff != "" {
			t.Errorf("case %d: points mismatch (-want +got):\n%s", i, diff)
		}
	}
}

func TestDecodeWALEntryTruncated(t *testing.T) {
	t.Parallel()

	full := encodeWALEntry(walEntry{
		Labels: labels.FromStrings("__name__", "x"),
		Points: []types.MetricPoint{{Timestamp: 1, Value: 1}},
	})

	if _, err := decodeWALEntry(full[:len(full)-3]); err == nil {
		t.Error("decoding a truncated entry should error")
	}
}

func TestBackpressureDelay(t *testing.T) {
	t.Parallel()

	const (
		critical = 100
		stop     = 200
		maxSleep = time.Second
	)

	cases := []struct {
		name      string
		walLen    int64
		wantSleep time.Duration
		wantBlock bool
	}{
		{"below critical: no delay", 50, 0, false},
		{"at critical: no delay yet", 100, 0, false},
		{"mid ramp", 150, maxSleep / 2, false},
		{"near stop", 199, time.Duration(int64(maxSleep) * 99 / 100), false},
		{"at stop: block", 200, 0, true},
		{"above stop: block", 500, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotSleep, gotBlock := backpressureDelay(tc.walLen, critical, stop, maxSleep)
			if gotSleep != tc.wantSleep || gotBlock != tc.wantBlock {
				t.Errorf("backpressureDelay(%d) = (%v, %v), want (%v, %v)",
					tc.walLen, gotSleep, gotBlock, tc.wantSleep, tc.wantBlock)
			}
		})
	}

	// Disabled thresholds (0) never delay or block.
	if d, b := backpressureDelay(1_000_000, 0, 0, maxSleep); d != 0 || b {
		t.Errorf("disabled thresholds = (%v, %v), want (0, false)", d, b)
	}
}

// benchWALEntry is a realistic WAL entry (one series' worth of a single Commit:
// ~8 labels, 10 points) for the encode/decode benchmarks.
func benchWALEntry() walEntry {
	points := make([]types.MetricPoint, 10)
	for i := range points {
		points[i] = types.MetricPoint{Timestamp: int64(1_700_000_000_000 + i*10_000), Value: float64(i) * 1.5}
	}

	return walEntry{
		Labels: labels.FromStrings(
			"__name__", "node_cpu_seconds_total",
			"__acme_account__", "acme-tenant-1234",
			"instance", "server-42.example.com:9100",
			"job", "node-exporter",
			"cpu", "7",
			"mode", "idle",
			"item", "/var/lib/very/long/path/to/something",
			"region", "eu-west-1",
		),
		TTL:    86400,
		Points: points,
	}
}

func BenchmarkWALEntryEncode(b *testing.B) {
	e := benchWALEntry()

	b.ReportAllocs()

	for b.Loop() {
		_ = encodeWALEntry(e)
	}
}

func BenchmarkWALEntryDecode(b *testing.B) {
	encoded := encodeWALEntry(benchWALEntry())

	b.ReportAllocs()

	for b.Loop() {
		if _, err := decodeWALEntry(encoded); err != nil {
			b.Fatal(err)
		}
	}
}
