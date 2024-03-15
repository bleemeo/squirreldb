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
