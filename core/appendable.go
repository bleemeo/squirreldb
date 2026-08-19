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

package core

import (
	"context"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

// dualWriteAppendable is the write path when an extra (dual-write) backend is
// configured: it fans each request out to the primary (must-succeed) and every
// extra (best-effort). It is a narrow types.AppendableWithOptions — it wraps no
// full TSDB, so it hides none of a backend's capabilities. Reads never go through
// it (they come from the primary Backend directly).
type dualWriteAppendable struct {
	primary types.AppendableWithOptions
	// extras returns the live extra TSDB(s) at request time, so an extra attached
	// after startup starts receiving writes without rebuilding this appendable.
	extras func(ctx context.Context) []types.TSDB
	logger zerolog.Logger
}

func (d *dualWriteAppendable) AppenderWithOptions(
	ctx context.Context,
	options types.AppenderOptions,
) storage.Appender {
	extras := d.extras(ctx)
	extraAppenders := make([]storage.Appender, len(extras))

	for i, e := range extras {
		if be, ok := e.(bestEffortAppendable); ok {
			extraAppenders[i] = be.BestEffortAppenderWithOptions(ctx, options)
		} else {
			extraAppenders[i] = e.AppenderWithOptions(ctx, options)
		}
	}

	return &compositeAppender{
		primary: d.primary.AppenderWithOptions(ctx, options),
		extras:  extraAppenders,
		logger:  d.logger,
	}
}

// compositeAppender fans a single remote-write request out to the primary
// appender (must succeed) and every extra appender (best-effort). Points are
// buffered in every sub-appender on Append and persisted on Commit.
type compositeAppender struct {
	primary storage.Appender
	extras  []storage.Appender
	logger  zerolog.Logger
}

func (a *compositeAppender) Append(
	_ storage.SeriesRef,
	l labels.Labels,
	t int64,
	v float64,
) (storage.SeriesRef, error) {
	ref, err := a.primary.Append(0, l, t, v)
	if err != nil {
		return 0, err
	}

	for _, e := range a.extras {
		// Best-effort: an extra rejecting a point (e.g. out-of-range timestamp)
		// must not fail the request. It is dropped and repaired later from the
		// primary; the extra backend accounts the drop itself.
		_, _ = e.Append(0, l, t, v)
	}

	return ref, nil
}

// Commit commits the primary first and propagates its error (must-succeed source
// of truth), then commits each extra best-effort: an error is logged and not
// propagated, so an extra backend being down never blocks or fails the request.
// A best-effort extra (ClickHouse) drops on WAL threshold rather than blocking
// and accounts the drop itself, so its Commit usually returns nil even when it
// is unavailable.
func (a *compositeAppender) Commit() error {
	if err := a.primary.Commit(); err != nil {
		return err
	}

	for _, e := range a.extras {
		if err := e.Commit(); err != nil {
			a.logger.Warn().Err(err).
				Msg("best-effort write to an extra backend failed; points dropped (repair from the primary)")
		}
	}

	return nil
}

func (a *compositeAppender) Rollback() error {
	err := a.primary.Rollback()

	for _, e := range a.extras {
		_ = e.Rollback()
	}

	return err
}

func (a *compositeAppender) SetOptions(opts *storage.AppendOptions) {
	a.primary.SetOptions(opts)

	for _, e := range a.extras {
		e.SetOptions(opts)
	}
}

func (a *compositeAppender) AppendExemplar(
	_ storage.SeriesRef,
	l labels.Labels,
	ex exemplar.Exemplar,
) (storage.SeriesRef, error) {
	ref, err := a.primary.AppendExemplar(0, l, ex)
	if err != nil {
		return 0, err
	}

	for _, e := range a.extras {
		_, _ = e.AppendExemplar(0, l, ex)
	}

	return ref, nil
}

func (a *compositeAppender) AppendHistogram(
	_ storage.SeriesRef,
	l labels.Labels,
	t int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	ref, err := a.primary.AppendHistogram(0, l, t, h, fh)
	if err != nil {
		return 0, err
	}

	for _, e := range a.extras {
		_, _ = e.AppendHistogram(0, l, t, h, fh)
	}

	return ref, nil
}

func (a *compositeAppender) AppendHistogramCTZeroSample(
	_ storage.SeriesRef,
	l labels.Labels,
	t int64,
	ct int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	ref, err := a.primary.AppendHistogramCTZeroSample(0, l, t, ct, h, fh)
	if err != nil {
		return 0, err
	}

	for _, e := range a.extras {
		_, _ = e.AppendHistogramCTZeroSample(0, l, t, ct, h, fh)
	}

	return ref, nil
}

func (a *compositeAppender) UpdateMetadata(
	_ storage.SeriesRef,
	l labels.Labels,
	m metadata.Metadata,
) (storage.SeriesRef, error) {
	ref, err := a.primary.UpdateMetadata(0, l, m)
	if err != nil {
		return 0, err
	}

	for _, e := range a.extras {
		_, _ = e.UpdateMetadata(0, l, m)
	}

	return ref, nil
}

func (a *compositeAppender) AppendCTZeroSample(
	_ storage.SeriesRef,
	l labels.Labels,
	t int64,
	ct int64,
) (storage.SeriesRef, error) {
	ref, err := a.primary.AppendCTZeroSample(0, l, t, ct)
	if err != nil {
		return 0, err
	}

	for _, e := range a.extras {
		_, _ = e.AppendCTZeroSample(0, l, t, ct)
	}

	return ref, nil
}
