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

package requestappender

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

type Options struct {
	TenantLabel         labels.Label
	IgnoreLabelCallback func(ctx context.Context, tenant string, value string) (bool, error)
	// IngestedPoints counts points forwarded to the backend (ingress); optional.
	IngestedPoints prometheus.Counter
	// CommitSeconds observes the backend Commit duration (SquirrelDB<->TSDB write); optional.
	CommitSeconds prometheus.Observer
}

var ErrInvalidLabel = errors.New("invalid labels")

// NewAppender creates a new Appender that handle a single remote write request. It will enforce
// some SquirrelDB's features, like:
// - Enforce tenant Labels when set
// - Limit concurrent writes
// This appender should be created HTTP per-request.
func NewAppender(ctx context.Context, inner storage.Appender, opts Options) storage.Appender {
	return &appender{
		inner:   inner,
		options: opts,
		ctx:     ctx,
	}
}

type appender struct {
	ctx     context.Context //nolint:containedctx
	inner   storage.Appender
	options Options
}

func (app appender) Append(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	v float64,
) (storage.SeriesRef, error) {
	// Validate the client-supplied labels up front. The processing below only
	// removes labels (empty-value drop, mutable-label drop) or injects the
	// tenant, so labels valid here stay valid; the tenant name is config-trusted.
	if err := validateLabels(l); err != nil {
		return 0, err
	}

	builder := labels.NewBuilder(l)

	// Drop empty-valued labels first (Prometheus treats an empty label as
	// absent). This MUST happen before enforcing the tenant below: otherwise a
	// request carrying the tenant label with an empty value would delete the
	// tenant we are about to enforce, bypassing tenant isolation.
	l.Range(func(label labels.Label) {
		if label.Value == "" {
			builder.Del(label.Name)
		}
	})

	if app.options.TenantLabel.Name != "" { //nolint:nestif
		builder.Set(app.options.TenantLabel.Name, app.options.TenantLabel.Value)

		if app.options.IgnoreLabelCallback != nil {
			var rangeErr error

			l.Range(func(label labels.Label) {
				isMutable, err := app.options.IgnoreLabelCallback(app.ctx, app.options.TenantLabel.Value, label.Name)
				if err != nil {
					rangeErr = err

					return
				}

				if isMutable {
					builder.Del(label.Name)
				}
			})

			if rangeErr != nil {
				return 0, rangeErr
			}
		}
	}

	l = builder.Labels()

	if offset := app.ctx.Value(types.BackdateContextKey{}); offset != nil {
		backdateOffset, ok := offset.(int64)
		if !ok {
			return 0, fmt.Errorf("invalid backdate offset type %T", offset)
		}

		t += backdateOffset
	}

	if app.options.IngestedPoints != nil {
		app.options.IngestedPoints.Inc()
	}

	return app.inner.Append(ref, l, t, v)
}

func (app appender) Commit() error {
	start := time.Now()

	err := app.inner.Commit()

	if app.options.CommitSeconds != nil {
		app.options.CommitSeconds.Observe(time.Since(start).Seconds())
	}

	return err
}

func (app appender) Rollback() error {
	return app.inner.Rollback()
}

func (app appender) SetOptions(opts *storage.AppendOptions) {
	app.inner.SetOptions(opts)
}

func (app appender) AppendExemplar(
	ref storage.SeriesRef,
	l labels.Labels,
	e exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return app.inner.AppendExemplar(ref, l, e)
}

func (app appender) AppendHistogram(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return app.inner.AppendHistogram(ref, l, t, h, fh)
}

func (app appender) AppendHistogramCTZeroSample(
	ref storage.SeriesRef,
	l labels.Labels,
	t int64,
	ct int64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return app.inner.AppendHistogramCTZeroSample(ref, l, t, ct, h, fh)
}

func (app appender) UpdateMetadata(
	ref storage.SeriesRef,
	l labels.Labels,
	m metadata.Metadata,
) (storage.SeriesRef, error) {
	return app.inner.UpdateMetadata(ref, l, m)
}

func (app appender) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	return app.inner.AppendCTZeroSample(ref, l, t, ct)
}

// validateLabels checks if the metric name and labels are valid utf-8 strings,
// while not containing the '|' character.
// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
func validateLabels(ls labels.Labels) error {
	var validationErr error

	ls.Range(func(l labels.Label) {
		if validationErr != nil {
			return
		}

		if l.Name == model.MetricNameLabel {
			if !types.PrometheusValidationScheme.IsValidMetricName(l.Value) || strings.Contains(l.Value, "|") {
				validationErr = fmt.Errorf(
					"%w: metric name '%s' should be valid utf-8, without char '|'",
					ErrInvalidLabel,
					l.Value,
				)

				return
			}
		} else {
			if !types.PrometheusValidationScheme.IsValidLabelName(l.Name) || strings.Contains(l.Name, "|") {
				validationErr = fmt.Errorf(
					"%w: label name '%s' should be valid utf-8, without char '|'",
					ErrInvalidLabel,
					l.Name,
				)

				return
			}
		}
	})

	return validationErr
}
