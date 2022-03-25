package mutable

import (
	"bytes"
	"context"
	"encoding/gob"
	"squirreldb/debug"
	"squirreldb/types"
	"sync"
)

const (
	topicInvalidateAssociatedNames  = "invalidate-associated-names"
	topicInvalidateAssociatedValues = "invalidate-associated-values"
)

type cache struct {
	cluster types.Cluster

	l sync.Mutex
	// The name cache must always either contain no key at all, or all names
	// present in Cassandra. It must be reassigned entirely when modified.
	nameCache map[string]string
	// map[tenant, name][value] contains the non mutable associated values.
	// The map[string][]string must always be reassigned entirely.
	valuesCache map[cacheKey]map[string][]string
}

type cacheKey struct {
	Tenant string
	Name   string
}

func newCache(cluster types.Cluster) *cache {
	c := cache{
		cluster:     cluster,
		nameCache:   make(map[string]string),
		valuesCache: make(map[cacheKey]map[string][]string),
	}

	c.cluster.Subscribe(topicInvalidateAssociatedValues, c.invalidateAssociatedValuesListener)
	c.cluster.Subscribe(topicInvalidateAssociatedNames, c.invalidateAssociatedNamesListener)

	return &c
}

// SetAllAssociatedValues sets mutable label values in cache.
// associatedValuesByValue must contain all possible values for this tenant and label name.
func (c *cache) SetAllAssociatedValues(tenant, name string, associatedValuesByValue map[string][]string) {
	c.l.Lock()
	defer c.l.Unlock()

	key := cacheKey{
		Tenant: tenant,
		Name:   name,
	}

	c.valuesCache[key] = associatedValuesByValue
}

// AssociatedValues returns the label values associated to a mutable label name and value of a tenant.
func (c *cache) AssociatedValues(tenant, name, value string) (associatedValues []string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	key := cacheKey{
		Tenant: tenant,
		Name:   name,
	}

	values, found := c.valuesCache[key]
	if !found {
		return nil, false
	}

	associatedValues, found = values[value]

	return
}

// InvalidateAssociatedValues invalidates the non mutable values associated to a mutable label,
// and tells other SquirrelDBs in the cluster to do the same.
func (c *cache) InvalidateAssociatedValues(ctx context.Context, keys []cacheKey) {
	// Notify the cluster, we also notify ourselves so the cache will be deleted by the listener.
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)

	if err := encoder.Encode(keys); err != nil {
		logger.Printf("unable to serialize new metrics. Cache invalidation on other name may fail: %v", err)
	}

	err := c.cluster.Publish(ctx, topicInvalidateAssociatedValues, buffer.Bytes())
	if err != nil {
		logger.Printf("Failed to publish a message, the associated values cache won't be invalidated: %v", err)
	}
}

// invalidateAssociatedValuesListener listens for messages from the cluster to invalidate the cache.
func (c *cache) invalidateAssociatedValuesListener(buffer []byte) {
	debug.Print(1, logger, "Invalidating mutable labels associated values cache")

	var keys []cacheKey

	decoder := gob.NewDecoder(bytes.NewReader(buffer))

	if err := decoder.Decode(&keys); err != nil {
		logger.Printf("Unable to decode associated values cache keys, the cache won't be invalidated: %v", err)

		return
	}

	c.l.Lock()
	defer c.l.Unlock()

	for _, key := range keys {
		delete(c.valuesCache, key)
	}
}

// Values returns all the non mutable label values associated to a tenant and a label name.
func (c *cache) Values(tenant, name string) (values []string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	key := cacheKey{
		Tenant: tenant,
		Name:   name,
	}

	valuesMap, found := c.valuesCache[key]
	if !found {
		return nil, false
	}

	// When not empty, the values cache for a key always contains all rows from Cassandra.
	for value := range valuesMap {
		values = append(values, value)
	}

	return values, true
}

// SetAllAssociatedNames sets all mutable label names and their associated non mutable names in cache.
// associatedNames must contain all possible mutable label names.
func (c *cache) SetAllAssociatedNames(associatedNames map[string]string) {
	c.l.Lock()
	defer c.l.Unlock()

	c.nameCache = associatedNames
}

// AssociatedName returns the non mutable label name associated to a name.
func (c *cache) AssociatedName(name string) (associatedName string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.nameCache == nil {
		return "", false
	}

	associatedName, found = c.nameCache[name]

	return
}

// InvalidateAssociatedNames invalidates the mutable labels names cache and tells other
// SquirrelDBs in the cluster to do the same.
func (c *cache) InvalidateAssociatedNames(ctx context.Context) {
	// Notify the cluster, we also notify ourselves so the cache will be deleted by the listener.
	err := c.cluster.Publish(ctx, topicInvalidateAssociatedNames, []byte("ok"))
	if err != nil {
		logger.Printf("Failed to publish a message, the associated names cache won't be invalidated: %v", err)
	}
}

// invalidateAssociatedNamesListener listens for messages from the cluster to invalidate the cache.
func (c *cache) invalidateAssociatedNamesListener(buffer []byte) {
	debug.Print(1, logger, "Invalidating mutable labels associated names cache")

	if string(buffer) != "ok" {
		logger.Printf("Wrong message received to invalidate the associated names cache")

		return
	}

	c.l.Lock()
	defer c.l.Unlock()

	c.nameCache = nil
}

// MutableLabelNames returns all the mutable label names in cache.
func (c *cache) MutableLabelNames() (names []string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	if len(c.nameCache) == 0 {
		return nil, false
	}

	// When not empty, the name cache always contains all rows from Cassandra.
	for name := range c.nameCache {
		names = append(names, name)
	}

	return names, true
}
