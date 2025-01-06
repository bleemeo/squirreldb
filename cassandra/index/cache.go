// Copyright 2015-2025 Bleemeo
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

package index

import (
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/prometheus/model/labels"
)

// labelsLookupCache provide cache for metricID to labels.
// It's eviction policy is random (Golang map order).
type labelsLookupCache struct {
	cache map[types.MetricID]labelsEntry
	l     sync.Mutex
}

// postingsCache provide a cache for postings queries by label=value.
// It is only used for query and not metric creation, mostly because creation only
// occure once thus a cache is not useful.
// Invalidation occur from TTL and on metric creation/deletion: a message is sent to all
// SquirrelDB and posting that match the created metric are invalidated.
type postingsCache struct {
	// cache map shard => label name => label value => postingEntry
	nowFunc func() time.Time
	cache   map[postingsCacheKey]postingEntry
	l       sync.Mutex
}

type postingsCacheKey struct {
	Name  string
	Value string
	Shard int32
}

type postingEntry struct {
	expire time.Time
	value  *roaring.Bitmap
	count  uint64
}

type labelsEntry struct {
	cassandraExpire time.Time
	value           labels.Labels
}

const (
	labelCacheMaxSize    = 10000
	postingsCacheMaxSize = 10000
	postingsCacheTTL     = 15 * time.Minute
)

// Get return the non-expired cache entry or a nil list.
func (c *labelsLookupCache) Get(now time.Time, id types.MetricID) labels.Labels {
	c.l.Lock()
	defer c.l.Unlock()

	return c.get(now, id)
}

// MGet return the non-expired cache entry or an nil list for multiple IDs.
func (c *labelsLookupCache) MGet(now time.Time, ids []types.MetricID) []labels.Labels {
	c.l.Lock()
	defer c.l.Unlock()

	result := make([]labels.Labels, len(ids))
	for i, id := range ids {
		result[i] = c.get(now, id)
	}

	return result
}

// Set add entry to the cache. Return the current cache size.
func (c *labelsLookupCache) Set(
	now time.Time,
	id types.MetricID,
	value labels.Labels,
	cassandraExpiration time.Time,
) int {
	c.l.Lock()
	defer c.l.Unlock()

	if len(c.cache) > labelCacheMaxSize {
		// First drop expired entry
		for k, v := range c.cache {
			if v.cassandraExpire.Before(now) {
				delete(c.cache, k)
			}
		}

		// we want to evict at least 50%, to avoid doing set/evict one/set/evict one/...
		if len(c.cache) > labelCacheMaxSize/2 {
			toDelete := len(c.cache) - labelCacheMaxSize/2

			for k := range c.cache {
				delete(c.cache, k)

				toDelete--

				if toDelete <= 0 {
					break
				}
			}
		}
	}

	c.cache[id] = labelsEntry{
		value:           value.Copy(),
		cassandraExpire: cassandraExpiration,
	}

	return len(c.cache)
}

// Drop delete entries from cache. Return the cache size.
func (c *labelsLookupCache) Drop(ids []uint64) int {
	c.l.Lock()
	defer c.l.Unlock()

	for _, id := range ids {
		delete(c.cache, types.MetricID(id)) //nolint:gosec
	}

	return len(c.cache)
}

func (c *labelsLookupCache) get(now time.Time, id types.MetricID) labels.Labels {
	entry := c.cache[id]
	if entry.cassandraExpire.IsZero() {
		return nil
	}

	if entry.cassandraExpire.Before(now) {
		delete(c.cache, id)

		return nil
	}

	return entry.value.Copy()
}

// Get return the non-expired cache entry or nil.
func (c *postingsCache) Get(shard int32, name string, value string) *roaring.Bitmap {
	return c.get(c.nowFunc(), shard, name, value)
}

// Invalidate drop entry that are impacted by given labels. Return the cache size.
func (c *postingsCache) Invalidate(entries []postingsCacheKey) int {
	c.l.Lock()
	defer c.l.Unlock()

	for _, k := range entries {
		delete(c.cache, k)
	}

	return len(c.cache)
}

// Set add an entry. Return the cache size.
func (c *postingsCache) Set(shard int32, name string, value string, bitmap *roaring.Bitmap) int {
	now := c.nowFunc()

	return c.set(now, shard, name, value, bitmap.Freeze())
}

func (c *postingsCache) set(now time.Time, shard int32, name string, value string, bitmap *roaring.Bitmap) int {
	c.l.Lock()
	defer c.l.Unlock()

	if len(c.cache) > postingsCacheMaxSize {
		// First drop expired entry
		for k, v := range c.cache {
			if v.expire.Before(now) {
				delete(c.cache, k)
			}
		}

		// we want to evict at least 50%, to avoid doing set/evict one/set/evict one/...
		if len(c.cache) > postingsCacheMaxSize/2 {
			toDelete := len(c.cache) - postingsCacheMaxSize/2

			for k := range c.cache {
				delete(c.cache, k)

				toDelete--

				if toDelete <= 0 {
					break
				}
			}
		}
	}

	key := postingsCacheKey{
		Shard: shard,
		Name:  name,
		Value: value,
	}

	c.cache[key] = postingEntry{
		expire: now.Add(postingsCacheTTL),
		value:  bitmap,
		count:  bitmap.Count(),
	}

	return len(c.cache)
}

func (c *postingsCache) get(now time.Time, shard int32, name string, value string) *roaring.Bitmap {
	c.l.Lock()
	defer c.l.Unlock()

	key := postingsCacheKey{
		Shard: shard,
		Name:  name,
		Value: value,
	}

	entry := c.cache[key]

	if entry.expire.IsZero() {
		return nil
	}

	if entry.value.Count() != entry.count {
		panic("fuck...")
	}

	if entry.expire.Before(now) {
		delete(c.cache, key)

		return nil
	}

	return entry.value
}

// shardExpirationCache provides a cache for shard expirations.
// Currently we only update the expiration of the current shard,
// so we only need to cache one shard.
type shardExpirationCache struct {
	shard      int32
	expiration time.Time
	metricID   types.MetricID
}

// Get the cached expiration for a shard, the metric ID where the shard
// expiration is stored, and whether the shard was found in the cache.
func (s *shardExpirationCache) Get(shard int32) (time.Time, types.MetricID, bool) {
	// Shard 0 is considered invalid because we don't know if the cache is set.
	if s.shard != 0 && s.shard == shard {
		return s.expiration, s.metricID, true
	}

	return time.Time{}, 0, false
}

// Set the expiration and metric ID for a shard.
func (s *shardExpirationCache) Set(shard int32, expiration time.Time, metricID types.MetricID) {
	s.shard = shard
	s.expiration = expiration
	s.metricID = metricID
}
