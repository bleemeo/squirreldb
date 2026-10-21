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

package api

import (
	"context"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// newAppendableV2 exposes a storage.Appendable as a storage.AppendableV2.
// The start timestamp, exemplars and metadata are dropped, as SquirrelDB doesn't store them.
func newAppendableV2(appendable storage.Appendable) storage.AppendableV2 {
	return appendableV2{Appendable: appendable}
}

type appendableV2 struct {
	storage.Appendable
}

func (a appendableV2) AppenderV2(ctx context.Context) storage.AppenderV2 {
	return appenderV2{inner: a.Appender(ctx)}
}

type appenderV2 struct {
	inner storage.Appender
}

func (app appenderV2) Append(
	ref storage.SeriesRef,
	l labels.Labels,
	_, t int64,
	v float64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
	_ storage.AppendV2Options,
) (storage.SeriesRef, error) {
	if h != nil || fh != nil {
		return app.inner.AppendHistogram(ref, l, t, h, fh)
	}

	return app.inner.Append(ref, l, t, v)
}

func (app appenderV2) Commit() error {
	return app.inner.Commit()
}

func (app appenderV2) Rollback() error {
	return app.inner.Rollback()
}
