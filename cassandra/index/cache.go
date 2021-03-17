package index

import (
	"squirreldb/types"
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

// labelsLookupCache provide cache for metricID to labels.
// It's eviction policy is random (Golang map order).
type labelsLookupCache struct {
	l     sync.Mutex
	cache map[types.MetricID]labelsEntry
}

type labelsEntry struct {
	value           labels.Labels
	cassandraExpire time.Time
}

const (
	labelCacheMaxSize = 10000
)

// Get return the non-expired cache entry or an nil list.
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

// Set add entry to the cache.
func (c *labelsLookupCache) Set(now time.Time, id types.MetricID, value labels.Labels, cassandraExpiration time.Time) {
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
		value:           value,
		cassandraExpire: cassandraExpiration,
	}
}

// Drop delete entries from cache.
func (c *labelsLookupCache) Drop(ids []uint64) {
	c.l.Lock()
	defer c.l.Unlock()

	for _, id := range ids {
		delete(c.cache, types.MetricID(id))
	}
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

	return entry.value
}
