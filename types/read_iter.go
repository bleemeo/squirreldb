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

package types

func MetricIterFromList(input []MetricData) MetricDataSet {
	return &readIter{
		input: input,
	}
}

type readIter struct {
	input      []MetricData
	nextOffset int
}

func (i *readIter) Err() error {
	return nil
}

func (i *readIter) Next() bool {
	if i.nextOffset == len(i.input) {
		return false
	}

	i.nextOffset++

	return true
}

func (i *readIter) At() MetricData {
	return i.input[i.nextOffset-1]
}

// MetricIterToList convert a MetricDataSet to a list of MetricData.
// numberOfNextCall define the maximum number of call done to Next(). 0 means unlimited.
func MetricIterToList(i MetricDataSet, numberOfNextCall int) ([]MetricData, error) {
	results := make([]MetricData, 0)

	for i.Next() {
		tmp := i.At()
		results = append(results, tmp)

		if len(results) >= numberOfNextCall && numberOfNextCall > 0 {
			break
		}
	}

	return results, i.Err()
}
