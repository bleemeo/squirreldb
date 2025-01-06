// Copyright 2015-2025 Bleemeo
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
	"testing"

	"github.com/pilosa/pilosa/v2/roaring"
)

func Test_truncatedIDList(t *testing.T) {
	tests := []struct {
		name    string
		input   []uint64
		maxItem int
		want    string
	}{
		{
			name:    "empty",
			input:   []uint64{},
			maxItem: 10,
			want:    "nothing",
		},
		{
			name:    "some",
			input:   []uint64{1, 2, 5},
			maxItem: 10,
			want:    "1, 2, 5",
		},
		{
			name:    "exactly-at-limit",
			input:   []uint64{1, 5, 10000, 25975},
			maxItem: 4,
			want:    "1, 5, 10000, 25975",
		},
		{
			name:    "above-limit",
			input:   []uint64{1, 2, 3, 4, 5, 6},
			maxItem: 4,
			want:    "1, 2, 3, 4... (2 more)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bitmap := roaring.NewBTreeBitmap()

			_, err := bitmap.AddN(tt.input...)
			if err != nil {
				t.Fatal(err)
			}

			if got := truncatedIDList(bitmap, tt.maxItem); got != tt.want {
				t.Errorf("truncatedIDList() = %v, want %v", got, tt.want)
			}

			if got := truncatedSliceIDList(tt.input, tt.maxItem); got != tt.want {
				t.Errorf("truncatedIDList() = %v, want %v", got, tt.want)
			}
		})
	}
}
