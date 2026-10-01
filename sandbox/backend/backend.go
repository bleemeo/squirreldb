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

// Package backend implements the "sandbox" storage backend: a fake SquirrelDB
// serving deterministic, stateless data generated from a dataset (recorded
// samples replayed in an endless loop and PromQL expressions), with no external
// dependency. Writes are accepted and discarded. Every tenant sees the same
// dataset. It is meant for demos and for testing tools that read from
// SquirrelDB, see docs/sandbox.md.
//
// It satisfies the same core.Backend interface as the Cassandra and ClickHouse
// backends, so it is selected the same way, through the "backend" config option.
package backend

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/sandbox/dataset"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

var errNotStarted = errors.New("sandbox backend is not started")

// Options carries everything the sandbox backend needs.
type Options struct {
	MetricRegistry prometheus.Registerer
	Cluster        types.Cluster
	TenantLabel    string
	// DatasetConfig is the path of the dataset definition file. Empty uses the
	// default dataset embedded in SquirrelDB.
	DatasetConfig string
}

// Backend is the sandbox core.Backend.
type Backend struct {
	options Options
	logger  zerolog.Logger
	metrics *metrics

	l                sync.Mutex
	tsdb             *sandboxTSDB
	mutableStore     mutable.Store
	mutableProvider  *mutable.Provider
	mutableProcessor *mutable.LabelProcessor
}

// New returns a new unstarted sandbox backend.
func New(_ context.Context, logger zerolog.Logger, options Options) (*Backend, error) {
	return &Backend{
		options: options,
		logger:  logger,
		metrics: newMetrics(options.MetricRegistry),
	}, nil
}

// BackendInfo tells that mutable labels are applied by core.SquirrelDB, as for the
// dummy backend: mutable labels defined (in memory) for a tenant apply to the
// sandbox series of that tenant.
func (b *Backend) BackendInfo() types.BackendInfo {
	return types.BackendInfo{
		ApplyMutableLabelInternally: false,
	}
}

// Start loads the dataset. It fails if the dataset is invalid.
func (b *Backend) Start(context.Context) error {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb != nil {
		return nil
	}

	ds, err := dataset.Load(b.options.DatasetConfig)
	if err != nil {
		return err
	}

	t, err := newTSDB(ds, b.options.TenantLabel, b.metrics, b.logger)
	if err != nil {
		return err
	}

	b.logger.Info().Int("series", len(t.series)).Msg("Sandbox dataset loaded")

	b.tsdb = t

	return nil
}

// TSDB returns the sandbox TSDB. Start must have been called.
func (b *Backend) TSDB(context.Context) (types.TSDB, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb == nil {
		return nil, errNotStarted
	}

	return b.tsdb, nil
}

// MutableProvider returns the mutable label provider, backed by an in-memory
// store (mutable labels are not persisted).
func (b *Backend) MutableProvider(ctx context.Context) (*mutable.Provider, error) {
	b.l.Lock()
	defer b.l.Unlock()

	provider, _ := b.getMutableStack(ctx)

	return provider, nil
}

// MutableProcessor returns the mutable label processor.
func (b *Backend) MutableProcessor(ctx context.Context) (*mutable.LabelProcessor, error) {
	b.l.Lock()
	defer b.l.Unlock()

	_, processor := b.getMutableStack(ctx)

	return processor, nil
}

// getMutableStack builds (once) the mutable label store, provider and processor.
// Caller must hold b.l.
func (b *Backend) getMutableStack(ctx context.Context) (*mutable.Provider, *mutable.LabelProcessor) {
	if b.mutableProvider != nil {
		return b.mutableProvider, b.mutableProcessor
	}

	if b.mutableStore == nil {
		b.mutableStore = dummy.NewMutableLabelStore(dummy.MutableLabels{})
	}

	provider, processor := mutable.NewProviderAndProcessor(
		ctx,
		b.options.MetricRegistry,
		b.options.Cluster,
		b.mutableStore,
		b.options.TenantLabel,
		b.logger.With().Str("component", "label_provider").Logger(),
	)

	b.mutableProvider = provider
	b.mutableProcessor = processor

	return provider, processor
}

// TelemetryStore returns a no-op telemetry store: the sandbox keeps no state, so
// there is nothing to coordinate.
func (b *Backend) TelemetryStore(context.Context) (telemetry.Store, error) {
	return telemetryStore{}, nil
}

// CloseAll releases the mutable label stack. The dataset stays loaded: the
// sandbox TSDB holds no resource.
func (b *Backend) CloseAll(context.Context) {
	b.l.Lock()
	defer b.l.Unlock()

	b.mutableStore = nil
	b.mutableProvider = nil
	b.mutableProcessor = nil
}

// RegisterHTTPEndpoints registers no debug endpoint: the sandbox backend has none.
func (b *Backend) RegisterHTTPEndpoints(func(method, path, description string, handler http.HandlerFunc)) {
}

// telemetryStore is a no-op telemetry.Store: it hands out fixed IDs and a no-op
// release.
type telemetryStore struct{}

func (telemetryStore) ClusterID(context.Context) (string, error) {
	return "sandbox", nil
}

func (telemetryStore) AcquireNodeID(context.Context) (func() string, func(), error) {
	return func() string { return "sandbox" }, func() {}, nil
}

// compile-time check that the sandbox backend keeps satisfying the interfaces it
// must (without importing the core package, which would be a cycle).
var (
	_ types.TSDB      = (*sandboxTSDB)(nil)
	_ telemetry.Store = telemetryStore{}
)
