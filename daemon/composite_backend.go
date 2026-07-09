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

package daemon

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/api"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

var errCompositeUnsupported = errors.New("operation not supported by the primary TSDB")

// bestEffortAppendable is implemented by a TSDB (ClickHouse) that can hand out a
// best-effort, non-blocking appender for dual-write. When an extra TSDB
// implements it, the composite uses it so an unavailable extra never blocks or
// fails the request; otherwise it falls back to the normal appender.
type bestEffortAppendable interface {
	BestEffortAppenderWithOptions(ctx context.Context, options types.AppenderOptions) storage.Appender
}

// compositeTSDB writes to the primary TSDB (must succeed) and every extra TSDB
// (best-effort), and reads from the primary. It is the ONLY composite layer for
// dual-write: the primary and extra Backends stay unwrapped (reads, mutable
// labels, telemetry, debug endpoints all come from the primary Backend
// directly). It satisfies types.TSDB and forwards the legacy
// api.TSDBDebugEndpoint methods (reads to the primary, flush to primary + extras).
type compositeTSDB struct {
	primary types.TSDB
	logger  zerolog.Logger

	// extras may be attached after construction (attachExtra) — a best-effort extra
	// whose backend was unreachable at startup is added once it comes up — so every
	// access is guarded by mu.
	mu     sync.RWMutex
	extras []types.TSDB
}

func newCompositeTSDB(primary types.TSDB, extras []types.TSDB, logger zerolog.Logger) *compositeTSDB {
	return &compositeTSDB{primary: primary, extras: extras, logger: logger}
}

// attachExtra adds an extra dual-write TSDB after construction (the daemon uses it
// to attach an extra whose backend only became reachable after startup).
func (t *compositeTSDB) attachExtra(extra types.TSDB) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.extras = append(t.extras, extra)
}

// extrasSnapshot returns the current extras under the read lock, so callers can
// iterate without holding it (and without racing attachExtra).
func (t *compositeTSDB) extrasSnapshot() []types.TSDB {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return slices.Clone(t.extras)
}

func (t *compositeTSDB) AppenderWithOptions(ctx context.Context, options types.AppenderOptions) storage.Appender {
	extras := t.extrasSnapshot()
	extraAppenders := make([]storage.Appender, len(extras))

	for i, e := range extras {
		if be, ok := e.(bestEffortAppendable); ok {
			extraAppenders[i] = be.BestEffortAppenderWithOptions(ctx, options)
		} else {
			extraAppenders[i] = e.AppenderWithOptions(ctx, options)
		}
	}

	return &compositeAppender{
		primary: t.primary.AppenderWithOptions(ctx, options),
		extras:  extraAppenders,
		logger:  t.logger,
	}
}

func (t *compositeTSDB) QuerierWithOptions(
	opts types.QuerierOptions,
	mint int64,
	maxt int64,
) (storage.Querier, error) {
	return t.primary.QuerierWithOptions(opts, mint, maxt)
}

func (t *compositeTSDB) ChunkQuerierWithOptions(
	opts types.QuerierOptions,
	mint int64,
	maxt int64,
) (storage.ChunkQuerier, error) {
	return t.primary.ChunkQuerierWithOptions(opts, mint, maxt)
}

func (t *compositeTSDB) Close(ctx context.Context) error {
	// The underlying backends own their TSDBs and close them via CloseAll; closing
	// here too is safe because TSDB.Close is idempotent.
	var firstErr error

	if err := t.primary.Close(ctx); err != nil {
		firstErr = err
	}

	for _, e := range t.extrasSnapshot() {
		if err := e.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Index forwards to the primary TSDB (api.TSDBDebugEndpoint).
func (t *compositeTSDB) Index() types.Index {
	if d, ok := t.primary.(api.TSDBDebugEndpoint); ok {
		return d.Index()
	}

	return nil
}

// ForcePreAggregation forwards to the primary TSDB (api.TSDBDebugEndpoint).
func (t *compositeTSDB) ForcePreAggregation(
	ctx context.Context,
	threadCount int,
	from time.Time,
	to time.Time,
	matchers []*labels.Matcher,
) error {
	if d, ok := t.primary.(api.TSDBDebugEndpoint); ok {
		return d.ForcePreAggregation(ctx, threadCount, from, to, matchers)
	}

	return errCompositeUnsupported
}

// Flush forwards to the primary and every extra TSDB that supports it, so a
// manual flush drains all write pipelines.
func (t *compositeTSDB) Flush(ctx context.Context) error {
	var firstErr error

	if d, ok := t.primary.(api.TSDBDebugEndpoint); ok {
		if err := d.Flush(ctx); err != nil {
			firstErr = err
		}
	}

	for _, e := range t.extrasSnapshot() {
		if d, ok := e.(api.TSDBDebugEndpoint); ok {
			if err := d.Flush(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
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
