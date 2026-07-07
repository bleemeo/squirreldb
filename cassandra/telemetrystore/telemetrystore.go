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

// Package telemetrystore implements the telemetry.Store backed by a types.State
// (for the values) and distributed locks (for ownership). It is the original
// telemetry mechanism, used by the Cassandra backend.
//
// Node-ID ownership IS the per-id lock: holding "telemetry-id-<id>" means owning
// the ID. The lock auto-refreshes while held and lapses on crash (its TTL), so
// keeping the claim alive needs no extra work and release simply unlocks.
package telemetrystore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	defaultIDLockTTL        = time.Minute
	defaultIDLockRetryDelay = 5 * time.Second
)

var errFailedLock = errors.New("failed to acquire lock")

// LockFactory builds the distributed locks the store relies on.
type LockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

// Store is the state+locks telemetry store.
type Store struct {
	state       types.State
	lockFactory LockFactory
	logger      zerolog.Logger

	// Lock timing, resolved from telemetry.StoreTiming at New (zero → default).
	idLockTTL        time.Duration
	idLockRetryDelay time.Duration
}

// New returns a telemetry store backed by a state and a lock factory. timing
// overrides the per-ID lock timing (zero fields keep the built-in defaults); it
// is a test hook, not a user-facing config option.
func New(state types.State, lockFactory LockFactory, timing telemetry.StoreTiming, logger zerolog.Logger) *Store {
	return &Store{
		state:            state,
		lockFactory:      lockFactory,
		logger:           logger,
		idLockTTL:        orDefault(timing.IDLockTTL, defaultIDLockTTL),
		idLockRetryDelay: orDefault(timing.IDLockRetryDelay, defaultIDLockRetryDelay),
	}
}

// orDefault returns v when it is positive, otherwise def.
func orDefault(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}

	return def
}

func (s *Store) ClusterID(ctx context.Context) (string, error) {
	var clusterID string

	lock := s.lockFactory.CreateLock("create cluster id", 5*time.Second)
	if ok := lock.TryLock(ctx, 10*time.Second); !ok {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		return "", fmt.Errorf("%w: create cluster id", errFailedLock)
	}

	defer lock.Unlock()

	hasClusterID, err := s.state.Read(ctx, "cluster_id", &clusterID)
	if err != nil {
		return "", fmt.Errorf("unable to read cluster id for telemetry: %w", err)
	}

	if !hasClusterID {
		clusterID = uuid.New().String()

		if err := s.state.Write(ctx, "cluster_id", clusterID); err != nil {
			return "", fmt.Errorf("unable to set cluster id for telemetry: %w", err)
		}

		s.logger.Debug().Msgf("Created telemetry cluster ID %s", clusterID)
	} else {
		s.logger.Debug().Msgf("Read telemetry cluster ID %s", clusterID)
	}

	return clusterID, nil
}

// AcquireNodeID finds a free ID in the state-stored list (free = its per-id lock
// is acquirable) and holds its lock; if none is free it creates a new ID, adds
// it to the list and locks it. The returned release unlocks that per-id lock
// (the lock auto-refreshes itself while held, so there is nothing else to do).
func (s *Store) AcquireNodeID(ctx context.Context) (func() string, func(), error) {
	// Use a lock to read and write to the list of telemetry IDs from the state.
	lock := s.lockFactory.CreateLock("create telemetry id", 5*time.Second)
	if ok := lock.TryLock(ctx, 10*time.Second); !ok {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		return nil, nil, fmt.Errorf("%w: create telemetry id", errFailedLock)
	}

	defer lock.Unlock()

	telemetryIDs := make([]string, 0, 1)

	if _, err := s.state.Read(ctx, "telemetry-ids", &telemetryIDs); err != nil {
		return nil, nil, fmt.Errorf("failed to read telemetry ID from state: %w", err)
	}

	// Find a free ID in the list.
	for _, id := range telemetryIDs {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		idLock := s.lockFactory.CreateLock("telemetry-id-"+id, s.idLockTTL)
		if ok := idLock.TryLock(ctx, 0); !ok {
			// The lock is taken, another SquirrelDB already uses this ID.
			continue
		}

		s.logger.Debug().Msgf("Found free telemetry ID %s", id)

		// The lock guarantees exclusive ownership, so the ID never changes here;
		// a constant getter satisfies the telemetry.Store contract.
		return func() string { return id }, releaseLock(idLock), nil
	}

	// No free ID found, create a new one, add it to the state and lock it.
	id := uuid.New().String()
	telemetryIDs = append(telemetryIDs, id)

	if err := s.state.Write(ctx, "telemetry-ids", telemetryIDs); err != nil {
		return nil, nil, fmt.Errorf("failed to write telemetry ID in state: %w", err)
	}

	idLock := s.lockFactory.CreateLock("telemetry-id-"+id, s.idLockTTL)
	if ok := idLock.TryLock(ctx, s.idLockRetryDelay); !ok {
		return nil, nil, fmt.Errorf("%w: telemetry-id-%s", errFailedLock, id)
	}

	s.logger.Debug().Msgf("Created telemetry ID %s", id)

	return func() string { return id }, releaseLock(idLock), nil
}

// releaseLock returns an idempotent release function that unlocks the held lock.
func releaseLock(lock types.TryLocker) func() {
	var once sync.Once

	return func() {
		once.Do(lock.Unlock)
	}
}
