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

// Package backend implements the "dummy" storage backend: it discards every
// write and serves empty reads, with no external dependency. It exists to
// isolate load-test overhead — if a write load test is slow against the dummy
// backend, the bottleneck is the test harness (or, when the batcher is enabled,
// the batcher/temporary store), not Cassandra or ClickHouse.
//
// It satisfies the same daemon.Backend interface as the Cassandra and ClickHouse
// backends, so it is selected the same way, through the "backend" config option.
package backend

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

var errNotImplemented = errors.New("not implemented")

// Options carries everything the dummy backend needs.
type Options struct {
	MetricRegistry prometheus.Registerer
	Cluster        types.Cluster
	TenantLabel    string
	// Batch and Redis are only used when Batcher is true: the batcher buffers
	// writes in a temporary store (Redis when configured, in-memory otherwise)
	// before discarding them.
	Batch config.Batch
	Redis config.Redis
	// Batcher selects whether writes go through the shared batcher (true) or are
	// discarded immediately (false).
	Batcher bool
	// StoreLabels, when true (and Batcher is true), makes the appender store each
	// series' labels in the temporary store at commit and the writer read them
	// back at flush, mirroring the ClickHouse backend's label round-trip. It has
	// no effect when Batcher is false.
	StoreLabels bool
}

// Backend is the dummy daemon.Backend.
type Backend struct {
	options Options
	logger  zerolog.Logger

	l                sync.Mutex
	tsdb             types.TSDB
	mutableStore     mutable.Store
	mutableProvider  *mutable.Provider
	mutableProcessor *mutable.LabelProcessor
}

// New returns a new unstarted dummy backend.
func New(_ context.Context, logger zerolog.Logger, options Options) (*Backend, error) {
	return &Backend{options: options, logger: logger}, nil
}

// Start is a no-op: the dummy backend has nothing to connect to.
func (b *Backend) Start(context.Context) error {
	return nil
}

// TSDB returns the (single, cached) dummy TSDB: a plain discard TSDB, or a
// batcher-backed discard TSDB when the Batcher option is set.
func (b *Backend) TSDB(ctx context.Context) (types.TSDB, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb != nil {
		return b.tsdb, nil
	}

	if !b.options.Batcher {
		b.tsdb = dummy.DiscardTSDB{}

		return b.tsdb, nil
	}

	t, err := newBatchingTSDB(
		ctx,
		b.options.MetricRegistry,
		b.options.Batch,
		b.options.Redis,
		b.options.StoreLabels,
		b.logger.With().Str("component", "tsdb").Logger(),
	)
	if err != nil {
		return nil, err
	}

	b.tsdb = t

	return b.tsdb, nil
}

// MutableProvider returns the mutable label provider, backed by an empty
// in-memory store (the dummy backend has no mutable labels to serve).
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

// TelemetryStore returns a no-op telemetry store (telemetry is typically
// disabled with the dummy backend, but the interface must be satisfied).
func (b *Backend) TelemetryStore(context.Context) (telemetry.Store, error) {
	return telemetryStore{}, nil
}

// CloseAll stops the TSDB (the batcher, when used, does a final flush and closes
// its temporary store).
func (b *Backend) CloseAll(ctx context.Context) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb != nil {
		if err := b.tsdb.Close(ctx); err != nil {
			b.logger.Warn().Msgf("unable to stop TSDB: %v", err.Error())
		}

		b.tsdb = nil
	}

	b.mutableStore = nil
	b.mutableProvider = nil
	b.mutableProcessor = nil
}

// RegisterHTTPEndpoints registers no debug endpoint: the dummy backend has none.
func (b *Backend) RegisterHTTPEndpoints(func(method, path, description string, handler http.HandlerFunc)) {
}

// telemetryStore is a no-op telemetry.Store: it hands out fixed IDs and a no-op
// release. The dummy backend keeps no state, so there is nothing to coordinate.
type telemetryStore struct{}

func (telemetryStore) ClusterID(context.Context) (string, error) {
	return "dummy", nil
}

func (telemetryStore) AcquireNodeID(context.Context) (string, func(), error) {
	return "dummy", func() {}, nil
}

// compile-time check that the dummy backend keeps satisfying the interfaces it
// must (without importing the daemon package, which would be a cycle).
var (
	_ types.TSDB      = (*batchingTSDB)(nil)
	_ telemetry.Store = telemetryStore{}
)
