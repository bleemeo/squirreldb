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
			want:    "",
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
		tt := tt

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
