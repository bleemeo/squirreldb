package index

import (
	"squirreldb/types"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/pkg/labels"
)

// labelsLookupCache provide cache for metricID to labels.
// It's eviction policy is random (Golang map order).
type labelsLookupCache struct {
	l     sync.Mutex
	cache map[types.MetricID]labelsEntry
}

// searchCache provide cache for Search() query.
// This cache use TTL and max-size.
// It's eviction policy is random (Golang map order).
// A TTL is used because there is no strong guarantee that cluster message don't get lost.
// But in the nominal case, the invalidation is correct:
// * new metrics invalidate all search cache that match the given metrics
// * on deletion, on access the cache get invalidated.
type searchCache struct {
	l     sync.Mutex
	salt  []byte
	cache map[uint64]searchEntry
}

type labelsEntry struct {
	value           labels.Labels
	cassandraExpire time.Time
}

type searchEntry struct {
	value    []types.MetricID
	matchers []*labels.Matcher
	expire   time.Time
}

const (
	labelCacheMaxSize  = 10000
	searchCacheMaxSize = 10000
	searchCacheTTL     = 15 * time.Minute
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

// Get return the non-expired cache entry or an nil list.
func (c *searchCache) Get(shards []int32, matchers []*labels.Matcher) []types.MetricID {
	return c.get(time.Now(), shards, matchers)
}

// Drop delete an entry.
func (c *searchCache) Drop(shards []int32, matchers []*labels.Matcher) {
	key := c.key(shards, matchers)

	c.l.Lock()
	defer c.l.Unlock()

	delete(c.cache, key)
}

// Invalidate drop entry that are impacted by given metrics.
func (c *searchCache) Invalidate(metrics []labels.Labels) {
	c.l.Lock()
	defer c.l.Unlock()

	for k, entry := range c.cache {
		for _, lbls := range metrics {
			if matcherMatches(entry.matchers, lbls) {
				delete(c.cache, k)

				break
			}
		}
	}
}

// Set add an entry.
func (c *searchCache) Set(shards []int32, matchers []*labels.Matcher, ids []types.MetricID) {
	now := time.Now()
	c.set(now, shards, matchers, ids)
}

func (c *searchCache) set(now time.Time, shards []int32, matchers []*labels.Matcher, ids []types.MetricID) {
	key := c.key(shards, matchers)

	c.l.Lock()
	defer c.l.Unlock()

	if len(c.cache) > searchCacheMaxSize {
		// First drop expired entry
		for k, v := range c.cache {
			if v.expire.Before(now) {
				delete(c.cache, k)
			}
		}

		// we want to evict at least 50%, to avoid doing set/evict one/set/evict one/...
		if len(c.cache) > searchCacheMaxSize/2 {
			toDelete := len(c.cache) - searchCacheMaxSize/2

			for k := range c.cache {
				delete(c.cache, k)

				toDelete--

				if toDelete <= 0 {
					break
				}
			}
		}
	}

	c.cache[key] = searchEntry{
		value:    ids,
		matchers: matchers,
		expire:   now.Add(searchCacheTTL),
	}
}

func (c *searchCache) get(now time.Time, shards []int32, matchers []*labels.Matcher) []types.MetricID {
	key := c.key(shards, matchers)

	c.l.Lock()
	defer c.l.Unlock()

	entry := c.cache[key]

	if entry.expire.IsZero() {
		return nil
	}

	if entry.expire.Before(now) {
		delete(c.cache, key)

		return nil
	}

	// validate the matchers are the correct one (e.g. no key collision).
	equal := len(matchers) == len(entry.matchers)

	for i, m := range matchers {
		other := entry.matchers[i]
		if m.Type != other.Type || m.Name != other.Name || m.Value != other.Value {
			equal = false
			break
		}
	}

	if !equal {
		delete(c.cache, key)

		return nil
	}

	return entry.value
}

// return a key from shards & matchers.
// Currently collision are not handled, but because index.go do explicit labels
// check & invalidate if it don't match, this should be an issue.
func (c *searchCache) key(shards []int32, matchers []*labels.Matcher) uint64 {
	h := xxhash.New()

	// salt from a randomly choose prefix on SquirrelDB startup to make
	// harder for an attacker to craft request that cause collision.
	// Once more collision should yield invalid result, but will increase cache miss.
	_, _ = h.Write(c.salt)

	for _, s := range shards {
		_, _ = h.WriteString(strconv.FormatInt(int64(s), 10))
	}

	for _, lbl := range matchers {
		_, _ = h.WriteString(lbl.Name)
		_, _ = h.WriteString(lbl.Value)
		_, _ = h.WriteString(lbl.Type.String())
	}

	return h.Sum64()
}
