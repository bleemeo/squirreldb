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

package fixedqueryable

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

func NewErrorQuerier(err error) storage.Querier {
	return querierErr{err: err}
}

func NewErrorSeriesSet(err error) storage.SeriesSet {
	return seriesSetErr{err: err}
}

func NewErrorChunkSeriesSet(err error) storage.ChunkSeriesSet {
	return chunkSeriesSetErr{err: err}
}

type querierErr struct {
	err error
}

func (q querierErr) LabelValues(
	ctx context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	return nil, nil, q.err
}

func (q querierErr) LabelNames(
	ctx context.Context,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	return nil, nil, q.err
}

func (q querierErr) Close() error {
	return q.err
}

func (q querierErr) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	return NewErrorSeriesSet(q.err)
}

type seriesSetErr struct {
	err error
}

func (s seriesSetErr) Next() bool {
	return false
}

func (s seriesSetErr) At() storage.Series {
	return nil
}

func (s seriesSetErr) Err() error {
	return s.err
}

func (s seriesSetErr) Warnings() annotations.Annotations {
	return nil
}

type chunkSeriesSetErr struct {
	err error
}

func (s chunkSeriesSetErr) Next() bool {
	return false
}

func (s chunkSeriesSetErr) At() storage.ChunkSeries {
	return nil
}

func (s chunkSeriesSetErr) Err() error {
	return s.err
}

func (s chunkSeriesSetErr) Warnings() annotations.Annotations {
	return nil
}
