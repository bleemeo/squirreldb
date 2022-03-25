package mutable

import "sync"

type cache struct {
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

func newCache() *cache {
	c := cache{
		nameCache:   make(map[string]string),
		valuesCache: make(map[cacheKey]map[string][]string),
	}

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

// InvalidateAssociatedValues invalidates the cache for a tenant and a mutable label name.
func (c *cache) InvalidateAssociatedValues(tenant, name string) {
	c.l.Lock()
	defer c.l.Unlock()

	key := cacheKey{
		Tenant: tenant,
		Name:   name,
	}

	// TODO: Notify other squirreldbs to invalidate the cache.
	delete(c.valuesCache, key)
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

// InvalidateAssociatedNames invalidates the associated name cache.
func (c *cache) InvalidateAssociatedNames() {
	c.l.Lock()
	defer c.l.Unlock()

	// TODO: Notify other squirreldbs to invalidate the cache.
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
