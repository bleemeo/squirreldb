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

package remotestorage

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

var ErrIsReadOnly = errors.New("trying to write but read-only mode is enabled")

type remoteStorageReadOnly struct{}

type appenderReadOnly struct{}

// NewReadOnly return a appended in read-only mode. That is one that return ErrIsReadOnly on all writes.
func NewReadOnly() storage.Appendable {
	return remoteStorageReadOnly{}
}

func (remoteStorageReadOnly) Appender(_ context.Context) storage.Appender {
	return appenderReadOnly{}
}

func (appenderReadOnly) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	_ = ref
	_ = l
	_ = t
	_ = v

	return 0, ErrIsReadOnly
}

func (appenderReadOnly) Commit() error {
	return ErrIsReadOnly
}

func (appenderReadOnly) Rollback() error {
	return nil
}

func (appenderReadOnly) SetOptions(*storage.AppendOptions) {}

func (appenderReadOnly) AppendExemplar(
	ref storage.SeriesRef,
	l labels.Labels,
	e exemplar.Exemplar,
) (storage.SeriesRef, error) {
	_ = ref
	_ = l
	_ = e

	return 0, ErrIsReadOnly
}

func (appenderReadOnly) AppendHistogram(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	_ = ref
	_ = l
	_ = t
	_ = h
	_ = fh

	return 0, ErrIsReadOnly
}

func (appenderReadOnly) AppendHistogramCTZeroSample(
	_ storage.SeriesRef,
	_ labels.Labels,
	_, _ int64,
	_ *histogram.Histogram,
	_ *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, ErrIsReadOnly
}

func (appenderReadOnly) UpdateMetadata(
	ref storage.SeriesRef,
	l labels.Labels,
	m metadata.Metadata,
) (storage.SeriesRef, error) {
	_ = ref
	_ = l
	_ = m

	return 0, ErrIsReadOnly
}

func (o appenderReadOnly) AppendCTZeroSample(
	_ storage.SeriesRef, _ labels.Labels, _, _ int64,
) (storage.SeriesRef, error) {
	return 0, ErrIsReadOnly
}
