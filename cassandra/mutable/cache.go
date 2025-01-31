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

package mutable

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

const (
	topicInvalidateAssociatedNames  = "invalidate-associated-names"
	topicInvalidateAssociatedValues = "invalidate-associated-values"
	topicInvalidateMutableLabels    = "invalidate-mutable-labels"

	cacheCleanupInterval = 24 * time.Hour
	cacheExpirationDelay = time.Hour
)

type cache struct {
	cluster types.Cluster
	metrics *metrics
	logger  zerolog.Logger

	l sync.Mutex
	// Name cache indexed by tenant.
	nameCache   map[string]nameEntry
	valuesCache map[LabelKey]valuesEntry
	// mutableLabelsCache contains the mutable labels associated to a non mutable label.
	mutableLabelsCache map[Label]mutableLabelsEntry
}

type valuesEntry struct {
	lastAccess time.Time
	// associatedValues[value] contains the non mutable associated values.
	// This map must always be reassigned entirely.
	// Values() assumes it always contains all possible values.
	associatedValues map[string][]string
}

type nameEntry struct {
	lastAccess time.Time
	// associatedNames[mutable name] contains the associated non mutable name.
	// The map must always either contain no key at all, or all names
	// present in Cassandra. It must be reassigned entirely when modified.
	associatedNames map[string]string
}

type mutableLabelsEntry struct {
	lastAccess time.Time
	labels     labels.Labels
}

func newCache(
	ctx context.Context,
	reg prometheus.Registerer,
	cluster types.Cluster,
	logger zerolog.Logger,
) *cache {
	c := cache{
		cluster:            cluster,
		metrics:            newMetrics(reg),
		nameCache:          make(map[string]nameEntry),
		valuesCache:        make(map[LabelKey]valuesEntry),
		mutableLabelsCache: make(map[Label]mutableLabelsEntry),
		logger:             logger,
	}

	c.cluster.Subscribe(topicInvalidateAssociatedValues, c.invalidateAssociatedValuesListener)
	c.cluster.Subscribe(topicInvalidateAssociatedNames, c.invalidateAssociatedNamesListener)
	c.cluster.Subscribe(topicInvalidateMutableLabels, c.invalidateMutableLabelsListener)

	go c.cleanupScheduler(ctx)

	return &c
}

func (c *cache) cleanupScheduler(ctx context.Context) {
	cleanupTicker := time.NewTicker(cacheCleanupInterval)

	for ctx.Err() == nil {
		select {
		case <-cleanupTicker.C:
			c.cleanup()
		case <-ctx.Done():
			cleanupTicker.Stop()

			return
		}
	}

	cleanupTicker.Stop()
}

func (c *cache) cleanup() {
	c.l.Lock()
	defer c.l.Unlock()

	now := time.Now()

	// Delete expired entries in values cache.
	for key, entry := range c.valuesCache {
		if entry.lastAccess.Add(cacheExpirationDelay).Before(now) {
			c.logger.Debug().Msgf("Deleting expired mutable values cache entry %#v", key)

			delete(c.valuesCache, key)
		}
	}

	c.metrics.CacheSize.WithLabelValues("values").Set(float64(len(c.valuesCache)))

	// Delete expired entries in name cache.
	for key, entry := range c.nameCache {
		if entry.lastAccess.Add(cacheExpirationDelay).Before(now) {
			c.logger.Debug().Msgf("Deleting expired mutable name cache entry %#v", key)

			delete(c.nameCache, key)
		}
	}

	c.metrics.CacheSize.WithLabelValues("names").Set(float64(len(c.nameCache)))

	// Delete expired entries in mutable labels cache.
	for key, entry := range c.mutableLabelsCache {
		if entry.lastAccess.Add(cacheExpirationDelay).Before(now) {
			c.logger.Debug().Msgf("Deleting expired mutable labels cache entry %#v", key)

			delete(c.mutableLabelsCache, key)
		}
	}
}

// SetAllAssociatedValues sets mutable label values in cache.
// associatedValuesByValue must contain all possible values for this tenant and label name.
func (c *cache) SetAllAssociatedValues(tenant, name string, associatedValuesByValue map[string][]string) {
	c.l.Lock()
	defer c.l.Unlock()

	key := LabelKey{
		Tenant: tenant,
		Name:   name,
	}

	c.valuesCache[key] = valuesEntry{
		lastAccess:       time.Now(),
		associatedValues: associatedValuesByValue,
	}

	c.metrics.CacheSize.WithLabelValues("values").Set(float64(len(c.valuesCache)))
}

// AssociatedValues returns the label values associated to a mutable label name and value of a tenant.
// It can return an empty slice if the cache is up to date butno associated values exist for this
// tenant, name and value.
func (c *cache) AssociatedValues(tenant, name, value string) (associatedValues []string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		c.metrics.CacheAccess.WithLabelValues("values", metricStatus(found)).Inc()
	}()

	key := LabelKey{
		Tenant: tenant,
		Name:   name,
	}

	entry, found := c.valuesCache[key]
	if !found {
		return nil, false
	}

	// Update last access.
	entry.lastAccess = time.Now()
	c.valuesCache[key] = entry

	associatedValues = entry.associatedValues[value]

	// Always return true here because the cache for a tenant and label name is always in sync
	// with Cassandra. If the associated values were not found, it means they are not present
	// in Cassandra either, and there is no need to refresh the cache.
	return associatedValues, true
}

// InvalidateAssociatedValues invalidates the non mutable values associated to a mutable label,
// and tells other SquirrelDBs in the cluster to do the same.
func (c *cache) InvalidateAssociatedValues(ctx context.Context, keys []LabelKey) {
	// Notify the cluster, we also notify ourselves so the cache will be deleted by the listener.
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)

	if err := encoder.Encode(keys); err != nil {
		c.logger.Warn().Err(err).Msg("Failed encode message, the associated values cache won't be invalidated")
	}

	err := c.cluster.Publish(ctx, topicInvalidateAssociatedValues, buffer.Bytes())
	if err != nil {
		c.logger.Warn().Err(err).Msg("Failed to publish a message, the associated values cache won't be invalidated")
	}
}

// invalidateAssociatedValuesListener listens for messages from the cluster to invalidate the cache.
func (c *cache) invalidateAssociatedValuesListener(buffer []byte) {
	c.logger.Debug().Msg("Invalidating mutable labels associated values cache")

	var keys []LabelKey

	decoder := gob.NewDecoder(bytes.NewReader(buffer))

	if err := decoder.Decode(&keys); err != nil {
		c.logger.Warn().Err(err).Msg("Unable to decode associated values cache keys, the values cache won't be invalidated")

		return
	}

	c.l.Lock()
	defer c.l.Unlock()

	for _, key := range keys {
		delete(c.valuesCache, key)
	}

	c.metrics.CacheSize.WithLabelValues("values").Set(float64(len(c.valuesCache)))
}

// Values returns all the mutable label values associated to a tenant and a label name.
func (c *cache) Values(tenant, name string) (values []string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		c.metrics.CacheAccess.WithLabelValues("values", metricStatus(found)).Inc()
	}()

	key := LabelKey{
		Tenant: tenant,
		Name:   name,
	}

	entry, found := c.valuesCache[key]
	if !found {
		return nil, false
	}

	// Update last access.
	entry.lastAccess = time.Now()
	c.valuesCache[key] = entry

	// When not empty, the values cache for a key always contains all rows from Cassandra.
	for value := range entry.associatedValues {
		values = append(values, value)
	}

	return values, true
}

// SetAllAssociatedNames sets all mutable label names and their associated non mutable names in cache.
// associatedNames must contain all possible mutable label names for this tenant.
func (c *cache) SetAllAssociatedNames(tenant string, associatedNames map[string]string) {
	c.l.Lock()
	defer c.l.Unlock()

	c.nameCache[tenant] = nameEntry{
		lastAccess:      time.Now(),
		associatedNames: associatedNames,
	}

	c.metrics.CacheSize.WithLabelValues("names").Set(float64(len(c.nameCache)))
}

// NonMutableName returns the non mutable label name associated to a mutable name and a tenant.
// It can return an empty name if the cache is up to date but the associated name for this
// tenant and name simply doesn't exist.
func (c *cache) NonMutableName(tenant, name string) (nonMutableName string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		c.metrics.CacheAccess.WithLabelValues("names", metricStatus(found)).Inc()
	}()

	if c.nameCache == nil {
		return "", false
	}

	entry, found := c.nameCache[tenant]
	if !found {
		return "", false
	}

	// Update last access.
	entry.lastAccess = time.Now()
	c.nameCache[tenant] = entry

	nonMutableName = entry.associatedNames[name]

	// Always return true here because the cache for a tenant is always in sync with Cassandra.
	// If the associated name was not found, it means it's not present in Cassandra either,
	// and there is no need to refresh the cache.
	return nonMutableName, true
}

// InvalidateAssociatedNames invalidates the mutable labels names cache and tells other
// SquirrelDBs in the cluster to do the same.
func (c *cache) InvalidateAssociatedNames(ctx context.Context, tenants []string) {
	// Notify the cluster, we also notify ourselves so the cache will be deleted by the listener.
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)

	if err := encoder.Encode(tenants); err != nil {
		c.logger.Warn().Err(err).Msg("Failed encode message, the associated names cache won't be invalidated")
	}

	err := c.cluster.Publish(ctx, topicInvalidateAssociatedNames, buffer.Bytes())
	if err != nil {
		c.logger.Warn().Err(err).Msg("Failed to publish a message, the associated names cache won't be invalidated")
	}
}

// invalidateAssociatedNamesListener listens for messages from the cluster to invalidate the cache.
func (c *cache) invalidateAssociatedNamesListener(buffer []byte) {
	c.logger.Debug().Msg("Invalidating mutable labels associated names cache")

	var tenants []string

	decoder := gob.NewDecoder(bytes.NewReader(buffer))

	if err := decoder.Decode(&tenants); err != nil {
		c.logger.Warn().Err(err).Msg("Unable to decode tenants, the name cache won't be invalidated")

		return
	}

	c.l.Lock()
	defer c.l.Unlock()

	for _, tenant := range tenants {
		delete(c.nameCache, tenant)
	}

	c.metrics.CacheSize.WithLabelValues("names").Set(float64(len(c.valuesCache)))
}

// MutableLabelNames returns the mutable label names associated to a name and a tenant.
func (c *cache) MutableLabelNames(tenant, name string) (mutableNames []string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		c.metrics.CacheAccess.WithLabelValues("names", metricStatus(found)).Inc()
	}()

	entry, found := c.nameCache[tenant]
	if !found {
		return nil, false
	}

	// Update last access.
	entry.lastAccess = time.Now()
	c.nameCache[tenant] = entry

	for mutableName, nonMutableName := range entry.associatedNames {
		if name == nonMutableName {
			mutableNames = append(mutableNames, mutableName)
		}
	}

	return mutableNames, true
}

// AllMutableLabelNames returns all the mutable label names in cache for a tenant.
func (c *cache) AllMutableLabelNames(tenant string) (names []string, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		c.metrics.CacheAccess.WithLabelValues("names", metricStatus(found)).Inc()
	}()

	entry, found := c.nameCache[tenant]
	if !found {
		return nil, false
	}

	// Update last access.
	entry.lastAccess = time.Now()
	c.nameCache[tenant] = entry

	// When not nil, the name cache for a tenant always contains all rows from Cassandra.
	for name := range entry.associatedNames {
		names = append(names, name)
	}

	return names, true
}

// MutableLabels returns the mutable labels corresponding to a non mutable label name and value.
func (c *cache) MutableLabels(tenant, name, value string) (lbls labels.Labels, found bool) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		c.metrics.CacheAccess.WithLabelValues("labels", metricStatus(found)).Inc()
	}()

	key := Label{
		Tenant: tenant,
		Name:   name,
		Value:  value,
	}

	entry, found := c.mutableLabelsCache[key]
	if !found {
		return nil, false
	}

	// Update last access.
	entry.lastAccess = time.Now()
	c.mutableLabelsCache[key] = entry

	return entry.labels, true
}

// SetMutableLabels sets the mutable labels corresponding to a non mutable label name and value.
func (c *cache) SetMutableLabels(tenant, name, value string, lbls labels.Labels) {
	c.l.Lock()
	defer c.l.Unlock()

	key := Label{
		Tenant: tenant,
		Name:   name,
		Value:  value,
	}

	c.mutableLabelsCache[key] = mutableLabelsEntry{
		lastAccess: time.Now(),
		labels:     lbls,
	}

	c.metrics.CacheSize.WithLabelValues("labels").Set(float64(len(c.mutableLabelsCache)))
}

// InvalidateMutableLabels invalidates the mutable labels cache and tells other
// SquirrelDBs in the cluster to do the same.
func (c *cache) InvalidateMutableLabels(ctx context.Context, tenants []string) {
	// Notify the cluster, we also notify ourselves so the cache will be deleted by the listener.
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)

	if err := encoder.Encode(tenants); err != nil {
		c.logger.Warn().Err(err).Msg("Failed encode message, the associated names cache won't be invalidated")
	}

	err := c.cluster.Publish(ctx, topicInvalidateMutableLabels, buffer.Bytes())
	if err != nil {
		c.logger.Warn().Err(err).Msg("Failed to publish a message, the associated names cache won't be invalidated")
	}
}

// invalidateMutableLabelsListener listens for messages from the cluster to invalidate the cache.
func (c *cache) invalidateMutableLabelsListener(buffer []byte) {
	c.logger.Debug().Msg("Invalidating mutable labels cache")

	var tenants []string

	decoder := gob.NewDecoder(bytes.NewReader(buffer))

	if err := decoder.Decode(&tenants); err != nil {
		c.logger.Warn().Err(err).Msg("Unable to decode tenants, the mutable labels cache won't be invalidated")

		return
	}

	c.l.Lock()
	defer c.l.Unlock()

	for _, tenant := range tenants {
		for key := range c.mutableLabelsCache {
			if key.Tenant == tenant {
				delete(c.mutableLabelsCache, key)
			}
		}
	}

	c.metrics.CacheSize.WithLabelValues("names").Set(float64(len(c.valuesCache)))
}
