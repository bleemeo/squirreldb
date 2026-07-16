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

package backend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/cassandra/index"
	"github.com/bleemeo/squirreldb/cassandra/locks"
	cassandramutable "github.com/bleemeo/squirreldb/cassandra/mutable"
	"github.com/bleemeo/squirreldb/cassandra/states"
	cassandratelemetrystore "github.com/bleemeo/squirreldb/cassandra/telemetrystore"
	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// Options carries everything the Cassandra backend needs. The store-specific
// options that used to be passed to TSDB() per-call now live here, set once at
// creation (there is a single TSDB instance per backend).
type Options struct {
	CassandraConfig           config.Cassandra
	ReadOnly                  bool
	MetricRegistry            prometheus.Registerer
	Cluster                   types.Cluster
	TenantLabel               string
	DefaultTimeToLive         time.Duration
	AggregateIntendedDuration func() time.Duration
	DisablePreAggregation     bool
	BatchSize                 time.Duration
	Redis                     config.Redis // If Redis is unset, use an in-memory store for batching.
	PreCreateShardDuration    time.Duration
	PreCreateShardFraction    int
	// DontStartIndex and InternalNowFunction are test hooks for the index.
	DontStartIndex      bool
	InternalNowFunction func() time.Time
	// TelemetryTiming overrides the telemetry store's per-ID lock timing. It is a
	// test hook (zero fields keep the defaults), not a user-facing config option.
	TelemetryTiming telemetry.StoreTiming
	// DropDataOnStartup makes Start drop the (test) keyspace before recreating it.
	// It is a test hook, not a user-facing config option, and refuses to run on any
	// keyspace other than the test one.
	DropDataOnStartup bool
}

var (
	ErrNotStarted         = errors.New("backend not started")
	ErrAlreadyStarted     = errors.New("already started")
	ErrTSDBAlreadyStarted = errors.New("cannot set index testing options after the TSDB is started")
)

// New returns a new unstarted backend.
func New(_ context.Context, logger zerolog.Logger, options Options) (*Backend, error) {
	backend := &Backend{
		options: options,
		logger:  logger,
	}

	return backend, nil
}

type Backend struct {
	connection      *connection.Connection
	keyspaceCreated bool
	options         Options
	logger          zerolog.Logger

	l                sync.Mutex
	lockFactory      *locks.CassandraLocks
	states           *states.CassandraStates
	mutableStore     mutable.Store
	mutableProvider  *mutable.Provider
	mutableProcessor *mutable.LabelProcessor
	tsdb             *tsdb.CassandraTSDB
	// cassandraIndex is the concrete index (not the mutable wrapper the TSDB
	// holds), stashed for the migration-read accessors. Set by newTSDB, so it is
	// non-nil only once TSDB() has built the store.
	cassandraIndex *index.CassandraIndex
}

func (b *Backend) Start(ctx context.Context) error {
	b.l.Lock()
	defer b.l.Unlock()

	if b.connection != nil {
		return ErrAlreadyStarted
	}

	// A test may ask for a clean slate: drop the keyspace before the connection
	// (re)creates it, so the drop happens on this very backend instead of a
	// separate one racing it.
	if b.options.DropDataOnStartup {
		if err := b.dropData(ctx); err != nil {
			return fmt.Errorf("drop data on startup: %w", err)
		}
	}

	session, keyspaceCreated, err := connection.New(
		ctx,
		b.options.CassandraConfig,
		b.options.ReadOnly,
		b.logger.With().Str("component", "connection").Logger(),
	)
	if err != nil {
		return err
	}

	b.connection = session
	b.keyspaceCreated = keyspaceCreated

	return nil
}

// StoreNodeCount returns the number of Cassandra nodes known.
func (b *Backend) StoreNodeCount(ctx context.Context) (int, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.connection == nil {
		return 0, ErrNotStarted
	}

	session, err := b.connection.Session()
	if err != nil {
		return 0, err
	}

	defer session.Close()

	count := 0
	it := session.Query("SELECT peer from system.peers").WithContext(ctx).Iter()

	for it.Scan(nil) {
		count++
	}

	if err := it.Close(); err != nil {
		return 0, err
	}

	// +1 because current Cassandra is not in peers
	return count + 1, nil
}

// CloseAll closes all components using Cassandra (TSDB, index, locks, ...).
func (b *Backend) CloseAll(ctx context.Context) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb != nil {
		if err := b.tsdb.Close(ctx); err != nil {
			b.logger.Warn().Msgf("unable to stop TSDB: %v", err.Error())
		}

		b.tsdb = nil
	}

	b.lockFactory = nil
	b.states = nil
	b.mutableStore = nil
	b.mutableProvider = nil
	b.mutableProcessor = nil

	b.connection.Close()
	b.connection = nil
}

// RegisterHTTPEndpoints registers no Cassandra-specific debug endpoints yet (the
// index debug endpoints may move here later). It satisfies daemon.Backend.
func (b *Backend) RegisterHTTPEndpoints(_ func(method, path, description string, handler http.HandlerFunc)) {
}

// dropData deletes the Cassandra test keyspace. It refuses any keyspace other
// than "squirreldb_test", and uses its own short-lived session (no keyspace
// selected), so it can run before the main connection creates the keyspace. It is
// only reached through the DropDataOnStartup test option.
func (b *Backend) dropData(ctx context.Context) error {
	if b.options.CassandraConfig.Keyspace != "squirreldb_test" {
		return fmt.Errorf("refuse to drop non-test keyspace %s", b.options.CassandraConfig.Keyspace)
	}

	session, err := b.rawSession()
	if err != nil {
		return err
	}

	defer session.Close()

	err = session.Query("DROP KEYSPACE IF EXISTS " + b.options.CassandraConfig.Keyspace).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("failed to drop keyspace: %w", err)
	}

	return nil
}

// LockFactory returns the Cassandra lock factory. It is exposed for the lock
// benchmark tool (which type-asserts for it; the ClickHouse backend has no
// equivalent and the bench self-skips); the rest of the code uses locks only
// through the backend.
func (b *Backend) LockFactory(ctx context.Context) (*locks.CassandraLocks, error) {
	b.l.Lock()
	defer b.l.Unlock()

	return b.getLockFactory(ctx)
}

// SetCassandraIndexTestingOption configures the Cassandra-index test hooks
// (don't start the index, override "now"). It must be called before the TSDB is
// built, and fails otherwise. It is Cassandra-specific: the ClickHouse backend
// does not implement it, so tests needing it are Cassandra-only by construction.
func (b *Backend) SetCassandraIndexTestingOption(dontStartIndex bool, nowFunc func() time.Time) error {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb != nil {
		return ErrTSDBAlreadyStarted
	}

	b.options.DontStartIndex = dontStartIndex
	b.options.InternalNowFunction = nowFunc

	return nil
}

// TSDB returns the (single, cached) Cassandra TSDB.
func (b *Backend) TSDB(ctx context.Context) (types.TSDB, error) {
	b.l.Lock()
	defer b.l.Unlock()

	if b.tsdb != nil {
		return b.tsdb, nil
	}

	t, err := b.newTSDB(ctx)
	if err != nil {
		return nil, err
	}

	b.tsdb = t

	return t, nil
}

// MutableProvider returns the mutable label provider (used by the API to read
// and write mutable labels).
func (b *Backend) MutableProvider(ctx context.Context) (*mutable.Provider, error) {
	b.l.Lock()
	defer b.l.Unlock()

	provider, _, err := b.getMutableStack(ctx)

	return provider, err
}

// MutableProcessor returns the mutable label processor (used by the API and the
// index to translate mutable labels).
func (b *Backend) MutableProcessor(ctx context.Context) (*mutable.LabelProcessor, error) {
	b.l.Lock()
	defer b.l.Unlock()

	_, processor, err := b.getMutableStack(ctx)

	return processor, err
}

func (b *Backend) StateStore(ctx context.Context) (types.State, error) {
	b.l.Lock()
	defer b.l.Unlock()

	return b.getStates(ctx)
}

// TelemetryStore returns the telemetry store backed by Cassandra states + locks.
func (b *Backend) TelemetryStore(ctx context.Context) (telemetry.Store, error) {
	b.l.Lock()
	defer b.l.Unlock()

	state, err := b.getStates(ctx)
	if err != nil {
		return nil, err
	}

	lockFactory, err := b.getLockFactory(ctx)
	if err != nil {
		return nil, err
	}

	return cassandratelemetrystore.New(
		state,
		lockFactory,
		b.options.ReadOnly,
		b.options.TelemetryTiming,
		b.logger.With().Str("component", "telemetry").Logger(),
	), nil
}

// rawSession opens a new connection to Cassandra and return the session.
// The returned session don't pre-selected the keyspace or created it.
// Caller is responsible for closing the connection.
func (b *Backend) rawSession() (*gocql.Session, error) {
	cluster := gocql.NewCluster(b.options.CassandraConfig.Addresses...)
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create raw session: %w", err)
	}

	return session, nil
}

func (b *Backend) getMutableLabelProvider(ctx context.Context) (mutable.Store, error) {
	if b.connection == nil {
		return nil, ErrNotStarted
	}

	if b.mutableStore != nil {
		return b.mutableStore, nil
	}

	store, err := cassandramutable.NewCassandraStore(ctx, cassandramutable.Options{
		Connection: b.connection,
		ReadOnly:   b.options.ReadOnly,
		Logger:     b.logger.With().Str("component", "mutable-labels").Logger(),
	})
	if err != nil {
		return nil, err
	}

	b.mutableStore = store

	return b.mutableStore, nil
}

// getMutableStack builds (once) the backend-agnostic mutable label provider and
// processor on top of the Cassandra store. Caller must hold b.l.
func (b *Backend) getMutableStack(ctx context.Context) (*mutable.Provider, *mutable.LabelProcessor, error) {
	if b.mutableProvider != nil {
		return b.mutableProvider, b.mutableProcessor, nil
	}

	store, err := b.getMutableLabelProvider(ctx)
	if err != nil {
		return nil, nil, err
	}

	provider, processor := mutable.NewProviderAndProcessor(
		ctx,
		// The mutable package registers backend-agnostic metric names; tag them with
		// the backend so a source and a target (migrate) can coexist in one registry.
		prometheus.WrapRegistererWith(map[string]string{"backend": "cassandra"}, b.options.MetricRegistry),
		b.options.Cluster,
		store,
		b.options.TenantLabel,
		b.logger.With().Str("component", "label_provider").Logger(),
	)

	b.mutableProvider = provider
	b.mutableProcessor = processor

	return provider, processor, nil
}

func (b *Backend) getLockFactory(ctx context.Context) (*locks.CassandraLocks, error) {
	if b.connection == nil {
		return nil, ErrNotStarted
	}

	if b.lockFactory != nil {
		return b.lockFactory, nil
	}

	factory, err := locks.New(
		ctx,
		b.options.MetricRegistry,
		b.connection,
		b.keyspaceCreated,
		b.options.ReadOnly,
		b.logger.With().Str("component", "locks").Logger(),
	)
	if err != nil {
		return nil, err
	}

	b.lockFactory = factory

	return b.lockFactory, nil
}

func (b *Backend) getSchemaLock(ctx context.Context) (types.TryLocker, error) {
	lockFactory, err := b.getLockFactory(ctx)
	if err != nil {
		return nil, err
	}

	return lockFactory.CreateLock("cassandra-schema", 10*time.Second), nil
}

func (b *Backend) getStates(ctx context.Context) (*states.CassandraStates, error) {
	if b.connection == nil {
		return nil, ErrNotStarted
	}

	if b.states != nil {
		return b.states, nil
	}

	lock, err := b.getSchemaLock(ctx)
	if err != nil {
		return nil, err
	}

	states, err := states.New(ctx, states.Options{
		Connection: b.connection,
		Lock:       lock,
		ReadOnly:   b.options.ReadOnly,
		Logger:     b.logger.With().Str("component", "states").Logger(),
	})
	if err != nil {
		return nil, err
	}

	b.states = states

	return b.states, nil
}

// newTSDB builds the Cassandra TSDB (index + store) from the backend options.
// Caller must hold b.l.
func (b *Backend) newTSDB(ctx context.Context) (*tsdb.CassandraTSDB, error) {
	if b.connection == nil {
		return nil, ErrNotStarted
	}

	lockFactory, err := b.getLockFactory(ctx)
	if err != nil {
		return nil, err
	}

	schemaLock, err := b.getSchemaLock(ctx)
	if err != nil {
		return nil, err
	}

	states, err := b.getStates(ctx)
	if err != nil {
		return nil, err
	}

	_, mutableProcessor, err := b.getMutableStack(ctx)
	if err != nil {
		return nil, err
	}

	idxOptions := index.Options{
		DefaultTimeToLive:      b.options.DefaultTimeToLive,
		PreCreateShardDuration: b.options.PreCreateShardDuration,
		PreCreateShardFraction: b.options.PreCreateShardFraction,
		TenantLabelName:        b.options.TenantLabel,
		LockFactory:            lockFactory,
		States:                 states,
		SchemaLock:             schemaLock,
		Cluster:                b.options.Cluster,
		ReadOnly:               b.options.ReadOnly,
		Logger:                 b.logger.With().Str("component", "index").Logger(),
		InternalRunOnceCalled:  b.options.DontStartIndex,
		InternalNowFunction:    b.options.InternalNowFunction,
	}

	wrappedIndex, err := index.New(
		ctx,
		b.options.MetricRegistry,
		b.connection,
		idxOptions,
		b.logger.With().Str("component", "index").Logger(),
	)
	if err != nil {
		return nil, err
	}

	if !b.options.DontStartIndex {
		if err := wrappedIndex.Start(ctx); err != nil {
			return nil, fmt.Errorf("start index task: %w", err)
		}
	}

	wrapped := mutable.NewIndexWrapper(
		wrappedIndex,
		mutableProcessor,
		b.logger.With().Str("component", "index_wrapper").Logger(),
	)

	options := tsdb.Options{
		DefaultTimeToLive:         b.options.DefaultTimeToLive,
		AggregateIntendedDuration: b.options.AggregateIntendedDuration,
		SchemaLock:                schemaLock,
		ReadOnly:                  b.options.ReadOnly,
		DisablePreAggregation:     b.options.DisablePreAggregation,
		BatchSize:                 b.options.BatchSize,
		Redis:                     b.options.Redis,
	}

	t, err := tsdb.New(
		ctx,
		b.options.MetricRegistry,
		b.connection,
		options,
		wrapped,
		lockFactory,
		states,
		b.logger.With().Str("component", "tsdb").Logger(),
	)
	if err != nil {
		if err := wrappedIndex.Stop(); err != nil {
			b.logger.Warn().Msgf("unable to stop index: %v", err.Error())
		}

		return nil, err
	}

	if err := t.Start(ctx); err != nil {
		if err := wrappedIndex.Stop(); err != nil {
			b.logger.Warn().Msgf("unable to stop index: %v", err.Error())
		}

		return nil, err
	}

	b.cassandraIndex = wrappedIndex

	return t, nil
}

// migrationIndex returns the concrete Cassandra index used by the migration
// reader (the `squirreldb migrate` subcommand) to enumerate time-shards and
// their series and to resolve id→labels. It is the raw index, not the mutable
// wrapper the TSDB uses: migration copies the labels as stored, and mutable-label
// aliasing is a query-time concern. Nil until TSDB() has built the store.
func (b *Backend) migrationIndex() *index.CassandraIndex {
	b.l.Lock()
	defer b.l.Unlock()

	return b.cassandraIndex
}
