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
	"errors"
	"testing"

	"github.com/bleemeo/squirreldb/config"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

func TestValidateConfigExtraWriteBackends(t *testing.T) {
	t.Parallel()

	base := config.DefaultConfig()

	cases := []struct {
		name    string
		mutate  func(*config.Config)
		wantErr bool
	}{
		{
			name:    "no extra_write_backend is fine",
			mutate:  func(*config.Config) {},
			wantErr: false,
		},
		{
			name: "dual write: clickhouse extra alongside cassandra",
			mutate: func(c *config.Config) {
				c.Backend = "cassandra"
				c.ExtraWriteBackend = "clickhouse"
			},
			wantErr: false,
		},
		{
			name: "extra equal to backend is rejected",
			mutate: func(c *config.Config) {
				c.Backend = "cassandra"
				c.ExtraWriteBackend = "cassandra"
			},
			wantErr: true,
		},
		{
			name: "unknown extra backend name",
			mutate: func(c *config.Config) {
				c.Backend = "cassandra"
				c.ExtraWriteBackend = "postgres"
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := base
			tc.mutate(&cfg)

			err := validateConfig(cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCompositeAppenderFanOut(t *testing.T) {
	t.Parallel()

	var commitOrder []string

	primary := &recordingAppender{name: "cassandra", commitOrder: &commitOrder}
	extra := &recordingAppender{name: "clickhouse", commitOrder: &commitOrder}
	app := &compositeAppender{primary: primary, extras: []storage.Appender{extra}, logger: zerolog.Nop()}

	if _, err := app.Append(0, labels.FromStrings("__name__", "cpu"), 1, 2); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if primary.appendCount != 1 || extra.appendCount != 1 {
		t.Fatalf("Append not fanned out: primary=%d extra=%d", primary.appendCount, extra.appendCount)
	}

	if err := app.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if primary.commitCount != 1 || extra.commitCount != 1 {
		t.Fatalf("both should be committed: primary=%d extra=%d", primary.commitCount, extra.commitCount)
	}
}

func TestCompositeAppenderPrimaryErrorPropagates(t *testing.T) {
	t.Parallel()

	errCommit := errors.New("primary boom")

	var commitOrder []string

	primary := &recordingAppender{name: "cassandra", commitOrder: &commitOrder, commitErr: errCommit}
	extra := &recordingAppender{name: "clickhouse", commitOrder: &commitOrder}
	app := &compositeAppender{primary: primary, extras: []storage.Appender{extra}, logger: zerolog.Nop()}

	err := app.Commit()
	if !errors.Is(err, errCommit) {
		t.Fatalf("Commit err = %v, want %v", err, errCommit)
	}

	// The primary (source of truth) failed → we must not have committed the extra,
	// so there is no partial write downstream.
	if extra.commitCount != 0 {
		t.Fatalf("extra committed despite primary failing: extra.commit=%d", extra.commitCount)
	}
}

func TestCompositeAppenderExtraErrorSwallowed(t *testing.T) {
	t.Parallel()

	var commitOrder []string

	primary := &recordingAppender{name: "cassandra", commitOrder: &commitOrder}
	extra := &recordingAppender{name: "clickhouse", commitOrder: &commitOrder, commitErr: errors.New("extra boom")}
	app := &compositeAppender{primary: primary, extras: []storage.Appender{extra}, logger: zerolog.Nop()}

	// A best-effort extra failing must NOT fail the request; the primary succeeded.
	if err := app.Commit(); err != nil {
		t.Fatalf("extra failure must not propagate, got: %v", err)
	}

	if primary.commitCount != 1 {
		t.Fatalf("primary should have committed: primary.commit=%d", primary.commitCount)
	}
}

// recordingAppender is a storage.Appender that records the order of its commits
// (into a shared slice) and can be told to fail its Commit.
type recordingAppender struct {
	name        string
	commitOrder *[]string
	commitErr   error
	appendCount int
	commitCount int
}

func (r *recordingAppender) Append(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ float64,
) (storage.SeriesRef, error) {
	r.appendCount++

	return 0, nil
}

func (r *recordingAppender) Commit() error {
	if r.commitErr != nil {
		return r.commitErr
	}

	r.commitCount++
	*r.commitOrder = append(*r.commitOrder, r.name)

	return nil
}

func (r *recordingAppender) Rollback() error { return nil }

func (r *recordingAppender) SetOptions(*storage.AppendOptions) {}

func (r *recordingAppender) AppendExemplar(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ exemplar.Exemplar,
) (storage.SeriesRef, error) {
	return 0, nil
}

func (r *recordingAppender) AppendHistogram(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ *histogram.Histogram,
	_ *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, nil
}

func (r *recordingAppender) AppendHistogramCTZeroSample(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ int64,
	_ *histogram.Histogram,
	_ *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, nil
}

func (r *recordingAppender) UpdateMetadata(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ metadata.Metadata,
) (storage.SeriesRef, error) {
	return 0, nil
}

func (r *recordingAppender) AppendCTZeroSample(
	_ storage.SeriesRef,
	_ labels.Labels,
	_ int64,
	_ int64,
) (storage.SeriesRef, error) {
	return 0, nil
}
