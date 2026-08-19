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

package fixedappender

import (
	"context"

	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

func NewErrorAppendable(err error) storage.Appendable {
	return errAppendable{err: err}
}

func NewErrorAppendableWithOptions(err error) types.AppendableWithOptions {
	return errAppendable{err: err}
}

func NewErrorAppender(err error) storage.Appender {
	return errAppender{err: err}
}

type errAppendable struct {
	err error
}

func (app errAppendable) Appender(ctx context.Context) storage.Appender {
	return NewErrorAppender(app.err)
}

func (app errAppendable) AppenderWithOptions(ctx context.Context, _ types.AppenderOptions) storage.Appender {
	return NewErrorAppender(app.err)
}

type errAppender struct {
	err error
}

func (app errAppender) Append(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	v float64,
) (storage.SeriesRef, error) {
	return 0, app.err
}

func (app errAppender) Commit() error {
	return app.err
}

func (app errAppender) Rollback() error {
	return app.err
}

func (app errAppender) SetOptions(opts *storage.AppendOptions) {
}

func (app errAppender) AppendExemplar(
	ref storage.SeriesRef,
	l labels.Labels,
	e exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return 0, app.err
}

func (app errAppender) AppendHistogram(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, app.err
}

func (app errAppender) AppendHistogramCTZeroSample(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	ct int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, app.err
}

func (app errAppender) UpdateMetadata(
	ref storage.SeriesRef,
	l labels.Labels,
	m metadata.Metadata,
) (storage.SeriesRef, error) {
	return 0, app.err
}

func (app errAppender) AppendCTZeroSample(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	ct int64,
) (storage.SeriesRef, error) {
	return 0, app.err
}
