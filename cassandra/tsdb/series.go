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
	"fmt"

	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type seriesIter struct {
	c         *CassandraTSDB
	list      types.MetricDataSet
	err       error
	id2Labels map[types.MetricID]labels.Labels
	current   series
}

func (i *seriesIter) Next() bool {
	if i.list == nil {
		return false
	}

	if i.err != nil {
		return false
	}

	if !i.list.Next() {
		return false
	}

	i.current = series{
		data: i.list.At(),
	}

	var ok bool

	if i.current.labels, ok = i.id2Labels[i.current.data.ID]; !ok {
		i.err = fmt.Errorf("metric ID %d don't have labels", i.current.data.ID)

		return false
	}

	return true
}

func (i *seriesIter) At() storage.Series {
	return i.current
}

func (i *seriesIter) Err() error {
	if i.err != nil {
		return i.err
	}

	if i.list == nil {
		return nil
	}

	return i.list.Err()
}

func (i *seriesIter) Warnings() annotations.Annotations {
	return nil
}

type series struct {
	labels labels.Labels
	data   types.MetricData
}

func (s series) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (s series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return convert.NewSampleIterator(s.data.Points)
}
