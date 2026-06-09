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
	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// SeriesSetToTimeSeries convert a *float* series set to list of timestamp.
// If any series contains non-float value, the behavior is undefined.
func SeriesSetToTimeSeries(series storage.SeriesSet) ([]types.Timeseries, error) {
	result := make([]types.Timeseries, 0)

	for series.Next() {
		at := series.At()
		points, err := collectSamples(at.Iterator(nil))

		result = append(result, types.Timeseries{
			Labels: at.Labels(),
			Points: points,
		})

		if err != nil {
			return result, err
		}
	}

	return result, series.Err()
}

func collectSamples(iter chunkenc.Iterator) ([]types.MetricPoint, error) {
	result := make([]types.MetricPoint, 0)

	for iter.Next() == chunkenc.ValFloat {
		atT, atV := iter.At()
		result = append(result, types.MetricPoint{Timestamp: atT, Value: atV})
	}

	return result, iter.Err()
}
