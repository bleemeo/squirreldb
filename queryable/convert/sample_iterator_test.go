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

package convert

import (
	"testing"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func points(ts ...int64) []types.MetricPoint {
	p := make([]types.MetricPoint, len(ts))
	for i, t := range ts {
		p[i] = types.MetricPoint{Timestamp: t, Value: float64(t)}
	}

	return p
}

func TestSampleIterator_Next(t *testing.T) {
	it := NewSampleIterator(points(10, 20, 30))

	var got []int64

	for it.Next() == chunkenc.ValFloat {
		got = append(got, it.AtT())
	}

	if len(got) != 3 || got[0] != 10 || got[2] != 30 {
		t.Fatalf("Next walk = %v, want [10 20 30]", got)
	}

	// Exhausted iterator stays exhausted.
	if it.Next() != chunkenc.ValNone {
		t.Fatal("Next after exhaustion should return ValNone")
	}
}

func TestSampleIterator_Seek(t *testing.T) {
	tests := []struct {
		name    string
		seek    int64
		wantVal chunkenc.ValueType
		wantTS  int64
	}{
		{name: "before first", seek: 5, wantVal: chunkenc.ValFloat, wantTS: 10},
		{name: "exact match", seek: 20, wantVal: chunkenc.ValFloat, wantTS: 20},
		{name: "between points", seek: 25, wantVal: chunkenc.ValFloat, wantTS: 30},
		{name: "past end", seek: 100, wantVal: chunkenc.ValNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Seek as the very first call (offset still -1) must not panic and
			// must scan from the first sample.
			it := NewSampleIterator(points(10, 20, 30))

			if got := it.Seek(tt.seek); got != tt.wantVal {
				t.Fatalf("Seek(%d) = %v, want %v", tt.seek, got, tt.wantVal)
			}

			if tt.wantVal == chunkenc.ValFloat && it.AtT() != tt.wantTS {
				t.Fatalf("after Seek(%d), AtT = %d, want %d", tt.seek, it.AtT(), tt.wantTS)
			}
		})
	}
}

func TestSampleIterator_SeekIsForwardOnly(t *testing.T) {
	it := NewSampleIterator(points(10, 20, 30))

	if it.Seek(20) != chunkenc.ValFloat || it.AtT() != 20 {
		t.Fatalf("Seek(20) failed to position on 20")
	}

	// Seeking backward must have no effect (Seek only advances forward).
	if it.Seek(10) != chunkenc.ValFloat || it.AtT() != 20 {
		t.Fatalf("backward Seek(10) moved iterator, AtT = %d, want 20", it.AtT())
	}
}

func TestSampleIterator_Empty(t *testing.T) {
	it := NewSampleIterator(nil)

	if it.Next() != chunkenc.ValNone {
		t.Fatal("Next on empty iterator should return ValNone")
	}

	if it.Seek(0) != chunkenc.ValNone {
		t.Fatal("Seek on empty iterator should return ValNone")
	}
}
