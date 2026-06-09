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
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/cassandra/index"
	"github.com/bleemeo/squirreldb/cassandra/locks"
	"github.com/bleemeo/squirreldb/cassandra/mutable"
	"github.com/bleemeo/squirreldb/cassandra/states"
	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/types"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type Options struct {
	CassandraConfig config.Cassandra
	ReadOnly        bool
	MetricRegistry  prometheus.Registerer
}

type TSDBOption struct {
	DontStartIndex            bool
	Cluster                   types.Cluster
	States                    types.State
	TenantLabel               string
	DefaultTimeToLive         time.Duration
	AggregateIntendedDuration func() time.Duration
	DisablePreAggregation     bool
	BatchSize                 time.Duration
	Redis                     config.Redis // If Redis is unset, use an in-memory store for batching
	InternalNowFunction       func() time.Time
	PreCreateShardDuration    time.Duration
	PreCreateShardFraction    int
	MutableLabelProcessor     *mutable.LabelProcessor
}

var (
	ErrNotStarted     = errors.New("backend not started")
	ErrAlreadyStarted = errors.New("already started")
)

func New(ctx context.Context, logger zerolog.Logger, options Options) (*Backend, error) {
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

	l            sync.Mutex
	lockFactory  *locks.CassandraLocks
	states       *states.CassandraStates
	mutableStore mutable.Store
	tsdbs        []*tsdb.CassandraTSDB
}

func (b *Backend) Start(ctx context.Context) error {
	b.l.Lock()
	defer b.l.Unlock()

	if b.connection != nil {
		return ErrAlreadyStarted
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

	for _, row := range b.tsdbs {
		if err := row.Close(ctx); err != nil {
			b.logger.Warn().Msgf("unable to stop TSDB: %v", err.Error())
		}
	}

	b.tsdbs = nil

	b.lockFactory = nil
	b.states = nil
	b.mutableStore = nil

	b.connection.Close()
	b.connection = nil
}

// DropCassandraData delete the Cassandra keyspace. If forceNonTestKeyspace also drop if the
// keyspace is not "squirreldb_test".
// This method is intended for testing, where keyspace is overrided to squirreldb_test.
func (b *Backend) DropCassandraData(ctx context.Context, forceNonTestKeyspace bool) error {
	if b.options.CassandraConfig.Keyspace != "squirreldb_test" && !forceNonTestKeyspace {
		return fmt.Errorf("refuse to drop keyspace %s without forceNonTestKeyspace", b.options.CassandraConfig.Keyspace)
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

func (b *Backend) LockFactory(ctx context.Context) (*locks.CassandraLocks, error) {
	b.l.Lock()
	defer b.l.Unlock()

	return b.getLockFactory(ctx)
}

func (b *Backend) States(ctx context.Context) (*states.CassandraStates, error) {
	b.l.Lock()
	defer b.l.Unlock()

	return b.getStates(ctx)
}

func (b *Backend) TSDB(ctx context.Context, options TSDBOption) (types.TSDB, error) {
	b.l.Lock()
	defer b.l.Unlock()

	tsdb, err := b.newTSDB(ctx, options)
	if err != nil {
		return nil, err
	}

	b.tsdbs = append(b.tsdbs, tsdb)

	return tsdb, nil
}

func (b *Backend) MutableLabelProvider(ctx context.Context) (mutable.Store, error) {
	b.l.Lock()
	defer b.l.Unlock()

	return b.getMutableLabelProvider(ctx)
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

	store, err := mutable.NewCassandraStore(ctx, mutable.Options{
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

func (b *Backend) newTSDB(ctx context.Context, createOptions TSDBOption) (*tsdb.CassandraTSDB, error) {
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

	idxOptions := index.Options{
		DefaultTimeToLive:      createOptions.DefaultTimeToLive,
		PreCreateShardDuration: createOptions.PreCreateShardDuration,
		PreCreateShardFraction: createOptions.PreCreateShardFraction,
		TenantLabelName:        createOptions.TenantLabel,
		LockFactory:            lockFactory,
		States:                 createOptions.States,
		SchemaLock:             schemaLock,
		Cluster:                createOptions.Cluster,
		ReadOnly:               b.options.ReadOnly,
		Logger:                 b.logger.With().Str("component", "index").Logger(),
		InternalRunOnceCalled:  createOptions.DontStartIndex,
		InternalNowFunction:    createOptions.InternalNowFunction,
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

	if !createOptions.DontStartIndex {
		if err := wrappedIndex.Start(ctx); err != nil {
			return nil, fmt.Errorf("start index task: %w", err)
		}
	}

	index := mutable.NewIndexWrapper(
		wrappedIndex,
		createOptions.MutableLabelProcessor,
		b.logger.With().Str("component", "index_wrapper").Logger(),
	)

	options := tsdb.Options{
		DefaultTimeToLive:         createOptions.DefaultTimeToLive,
		AggregateIntendedDuration: createOptions.AggregateIntendedDuration,
		SchemaLock:                schemaLock,
		ReadOnly:                  b.options.ReadOnly,
		DisablePreAggregation:     createOptions.DisablePreAggregation,
		BatchSize:                 createOptions.BatchSize,
		Redis:                     createOptions.Redis,
	}

	tsdb, err := tsdb.New(
		ctx,
		b.options.MetricRegistry,
		b.connection,
		options,
		index,
		lockFactory,
		createOptions.States,
		b.logger.With().Str("component", "tsdb").Logger(),
	)
	if err != nil {
		if err := wrappedIndex.Stop(); err != nil {
			b.logger.Warn().Msgf("unable to stop index: %v", err.Error())
		}

		return nil, err
	}

	if err := tsdb.Start(ctx); err != nil {
		if err := wrappedIndex.Stop(); err != nil {
			b.logger.Warn().Msgf("unable to stop index: %v", err.Error())
		}

		return nil, err
	}

	return tsdb, nil
}
