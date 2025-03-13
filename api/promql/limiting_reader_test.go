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

package promql

import (
	"testing"

	"github.com/bleemeo/squirreldb/types"
)

func Test_limitingReader_ReadIter(t *testing.T) {
	type fields struct {
		reader         types.MetricReader
		maxTotalPoints uint64
	}

	type readRequest struct {
		req        types.MetricRequest
		countPoint uint64
		wantErr    bool
	}

	tests := []struct {
		name   string
		fields fields
		reads  []readRequest
	}{
		{
			name: "high-limit",
			fields: fields{
				reader:         &mockStore{pointsPerSeries: 100},
				maxTotalPoints: 500000,
			},
			reads: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					countPoint: 100,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2},
					},
					countPoint: 200,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID2},
					},
					countPoint: 100,
				},
			},
		},
		{
			name: "medium-limit",
			fields: fields{
				reader:         &mockStore{pointsPerSeries: 100},
				maxTotalPoints: 300,
			},
			reads: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					countPoint: 100,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2},
					},
					countPoint: 200,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID2},
					},
					wantErr: true,
				},
			},
		},
		{
			name: "low-limit",
			fields: fields{
				reader:         &mockStore{pointsPerSeries: 100},
				maxTotalPoints: 101,
			},
			reads: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					countPoint: 100,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2},
					},
					wantErr: true,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID2},
					},
					wantErr: true,
				},
			},
		},
		{
			name: "very-low-limit",
			fields: fields{
				reader:         &mockStore{pointsPerSeries: 100},
				maxTotalPoints: 99,
			},
			reads: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					wantErr: true,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2},
					},
					wantErr: true,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID2},
					},
					wantErr: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdr := &limitingReader{
				reader:         tt.fields.reader,
				maxTotalPoints: tt.fields.maxTotalPoints,
				returnedPoints: new(uint64),
			}

			for i, r := range tt.reads {
				got, err := rdr.ReadIter(t.Context(), r.req)
				if err != nil {
					t.Fatal(err)
				}

				points, err := countPoints(got)
				if (err != nil) != r.wantErr {
					t.Errorf("limitingReader.ReadIter(#%d) error = %v, wantErr %v", i, err, r.wantErr)

					return
				} else if points != r.countPoint {
					t.Errorf("limitingReader.ReadIter(#%d) return %d points, want %d", i, points, r.countPoint)
				}
			}
		})
	}
}

func countPoints(iter types.MetricDataSet) (uint64, error) {
	if iter == nil {
		return 0, nil
	}

	count := uint64(0)

	for iter.Next() {
		count += uint64(len(iter.At().Points))
	}

	return count, iter.Err()
}
