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

package tsdb

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// memWALClient is an in-process walClient used when no Redis is configured. It
// keeps the WAL entirely in this process's memory, which means REDUCED DURABILITY:
// a crash or restart loses whatever is buffered but not yet drained to ClickHouse
// (with Redis, an ACKed write survives a SquirrelDB crash). It is also NOT shared
// across nodes — each SquirrelDB drains only its own in-memory WAL — so there is no
// cross-node HA. This mirrors the Cassandra backend, which likewise falls back to
// an in-memory batching store when Redis is absent.
//
// It faithfully reproduces the Redis list and lease semantics the WAL relies on,
// keyed by string so walStore/walDrainer are agnostic to the backing store. The
// lease always grants to the sole in-process owner (one drainer per TSDB), so this
// process is always the leader of its own WAL.
type memWALClient struct {
	mu     sync.Mutex
	lists  map[string][]string
	leases map[string]memLease
}

type memLease struct {
	owner  string
	expiry time.Time
}

func newMemWALClient() *memWALClient {
	return &memWALClient{
		lists:  make(map[string][]string),
		leases: make(map[string]memLease),
	}
}

// RPush appends the values to the tail of the list and returns its new length.
// Entries are the encoded WAL entries, pushed as []byte; stored as string so
// LRange can hand them back unchanged.
func (m *memWALClient) RPush(_ context.Context, key string, values ...any) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, v := range values {
		switch b := v.(type) {
		case []byte:
			m.lists[key] = append(m.lists[key], string(b))
		case string:
			m.lists[key] = append(m.lists[key], b)
		default:
			return 0, fmt.Errorf("memWALClient: unsupported RPush value type %T", v)
		}
	}

	return int64(len(m.lists[key])), nil
}

func (m *memWALClient) LLen(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return int64(len(m.lists[key])), nil
}

// LRange returns the [start, stop] slice (inclusive, negative indices count from
// the end), matching Redis LRANGE.
func (m *memWALClient) LRange(_ context.Context, key string, start, stop int64) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	list := m.lists[key]

	lo, hi := normalizeRange(start, stop, int64(len(list)))
	if lo > hi {
		return []string{}, nil
	}

	out := make([]string, hi-lo+1)
	copy(out, list[lo:hi+1])

	return out, nil
}

// LTrim keeps only the [start, stop] slice, matching Redis LTRIM (an empty result
// removes the key).
func (m *memWALClient) LTrim(_ context.Context, key string, start, stop int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	list := m.lists[key]

	lo, hi := normalizeRange(start, stop, int64(len(list)))
	if lo > hi {
		delete(m.lists, key)

		return nil
	}

	kept := make([]string, hi-lo+1)
	copy(kept, list[lo:hi+1])
	m.lists[key] = kept

	return nil
}

// LeaseAcquire acquires or refreshes the lease. In-process there is a single owner
// per lease key, so it grants unless another live owner somehow holds it (kept for
// faithful semantics; never happens with one drainer per TSDB).
func (m *memWALClient) LeaseAcquire(_ context.Context, key, owner string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	if cur, ok := m.leases[key]; ok && cur.owner != owner && now.Before(cur.expiry) {
		return false, nil
	}

	m.leases[key] = memLease{owner: owner, expiry: now.Add(ttl)}

	return true, nil
}

func (m *memWALClient) LeaseRelease(_ context.Context, key, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cur, ok := m.leases[key]; ok && cur.owner == owner {
		delete(m.leases, key)
	}

	return nil
}

// Close is a no-op: there is no connection to release.
func (m *memWALClient) Close() {}

// normalizeRange converts Redis LRANGE/LTRIM [start, stop] indices (inclusive,
// negatives count from the end) into a clamped [lo, hi] slice range over a list of
// length n. It returns lo > hi when the range is empty.
func normalizeRange(start, stop, n int64) (lo, hi int64) {
	if n == 0 {
		return 0, -1
	}

	if start < 0 {
		start += n
	}

	if stop < 0 {
		stop += n
	}

	if start < 0 {
		start = 0
	}

	if stop >= n {
		stop = n - 1
	}

	return start, stop
}
