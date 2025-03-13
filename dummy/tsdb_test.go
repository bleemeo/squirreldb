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

package dummy

import (
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/google/go-cmp/cmp"
)

func TestMemoryTSDB_ReadIter(t *testing.T) {
	sampleData := []types.MetricData{
		{
			ID: types.MetricID(1),
			Points: []types.MetricPoint{
				{
					Timestamp: time.Date(2023, 5, 10, 13, 51, 3, 0, time.UTC).UnixMilli(),
					Value:     1,
				},
				{
					Timestamp: time.Date(2023, 5, 11, 13, 51, 3, 0, time.UTC).UnixMilli(),
					Value:     2,
				},
			},
		},
		{
			ID: types.MetricID(2),
			Points: []types.MetricPoint{
				{
					Timestamp: time.Date(2023, 5, 11, 13, 51, 3, 0, time.UTC).UnixMilli(),
					Value:     1,
				},
				{
					Timestamp: time.Date(2023, 5, 12, 13, 51, 3, 0, time.UTC).UnixMilli(),
					Value:     2,
				},
			},
		},
	}

	tests := []struct {
		name    string
		Data    []types.MetricData
		request types.MetricRequest
		want    []types.MetricData
	}{
		{
			name: "read-all",
			Data: sampleData,
			request: types.MetricRequest{
				IDs:           []types.MetricID{1, 2},
				FromTimestamp: time.Date(2023, 5, 10, 13, 51, 3, 0, time.UTC).UnixMilli(),
				ToTimestamp:   time.Date(2023, 5, 12, 13, 51, 3, 0, time.UTC).UnixMilli(),
			},
			want: sampleData,
		},
		{
			name: "read-metric1",
			Data: sampleData,
			request: types.MetricRequest{
				IDs:           []types.MetricID{1},
				FromTimestamp: time.Date(2023, 5, 10, 13, 51, 3, 0, time.UTC).UnixMilli(),
				ToTimestamp:   time.Date(2023, 5, 12, 13, 51, 3, 0, time.UTC).UnixMilli(),
			},
			want: sampleData[:1],
		},
		{
			name: "read-metric2",
			Data: sampleData,
			request: types.MetricRequest{
				IDs:           []types.MetricID{2},
				FromTimestamp: time.Date(2023, 5, 10, 13, 51, 3, 0, time.UTC).UnixMilli(),
				ToTimestamp:   time.Date(2023, 5, 12, 13, 51, 3, 0, time.UTC).UnixMilli(),
			},
			want: sampleData[1:],
		},
		{
			name: "read-partial-time",
			Data: sampleData,
			request: types.MetricRequest{
				IDs:           []types.MetricID{1, 2},
				FromTimestamp: time.Date(2023, 5, 11, 13, 51, 3, 0, time.UTC).UnixMilli(),
				ToTimestamp:   time.Date(2023, 5, 12, 13, 51, 3, 0, time.UTC).UnixMilli(),
			},
			want: []types.MetricData{
				{
					ID: types.MetricID(1),
					Points: []types.MetricPoint{
						{
							Timestamp: time.Date(2023, 5, 11, 13, 51, 3, 0, time.UTC).UnixMilli(),
							Value:     2,
						},
					},
				},
				{
					ID: types.MetricID(2),
					Points: []types.MetricPoint{
						{
							Timestamp: time.Date(2023, 5, 11, 13, 51, 3, 0, time.UTC).UnixMilli(),
							Value:     1,
						},
						{
							Timestamp: time.Date(2023, 5, 12, 13, 51, 3, 0, time.UTC).UnixMilli(),
							Value:     2,
						},
					},
				},
			},
		},
		{
			name: "read-partial-time-metric1",
			Data: sampleData,
			request: types.MetricRequest{
				IDs:           []types.MetricID{1},
				FromTimestamp: time.Date(2023, 5, 11, 13, 51, 3, 0, time.UTC).UnixMilli(),
				ToTimestamp:   time.Date(2023, 5, 12, 13, 51, 3, 0, time.UTC).UnixMilli(),
			},
			want: []types.MetricData{
				{
					ID: types.MetricID(1),
					Points: []types.MetricPoint{
						{
							Timestamp: time.Date(2023, 5, 11, 13, 51, 3, 0, time.UTC).UnixMilli(),
							Value:     2,
						},
					},
				},
			},
		},
		{
			name: "read-empty-time",
			Data: sampleData,
			request: types.MetricRequest{
				IDs:           []types.MetricID{1, 2, 3},
				FromTimestamp: time.Date(2023, 5, 19, 13, 51, 3, 0, time.UTC).UnixMilli(),
				ToTimestamp:   time.Date(2023, 5, 20, 13, 51, 3, 0, time.UTC).UnixMilli(),
			},
			want: []types.MetricData{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dataMap := make(map[types.MetricID]types.MetricData, len(tt.Data))
			for _, row := range tt.Data {
				dataMap[row.ID] = row
			}

			db := &MemoryTSDB{
				Data: dataMap,
			}

			gotIter, err := db.ReadIter(t.Context(), tt.request)
			if err != nil {
				t.Fatalf("MemoryTSDB.ReadIter() error = %v", err)
			}

			got, err := types.MetricIterToList(gotIter, 0)
			if err != nil {
				t.Fatalf("MemoryTSDB.ReadIter() error = %v", err)
			}

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch in ReadIter() (-want +got)\n%s", diff)
			}
		})
	}
}
