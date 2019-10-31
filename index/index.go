package index

import (
	"crypto/md5"
	gouuid "github.com/gofrs/uuid"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

type Storage interface {
	Request() (map[types.MetricUUID]types.MetricLabels, error)
	Save(uuid types.MetricUUID, labels types.MetricLabels) error
}

type Index struct {
	storage Storage

	pairs map[types.MetricUUID]types.MetricLabels
	mutex sync.Mutex
}

// New creates a new Index object
func New(storage Storage) *Index {
	var pairs map[types.MetricUUID]types.MetricLabels

	retry.Do(func() error {
		var err error
		pairs, err = storage.Request()

		return err
	}, "index", "New",
		"Can't get uuid labels pairs from the storage",
		"Resolved: Get uuid labels pairs from the storage",
		retry.NewBackOff(30*time.Second))

	index := &Index{
		storage: storage,
		pairs:   pairs,
	}

	return index
}

// UUID returns UUID generated from the labels and save the index
func (i *Index) UUID(labels types.MetricLabels) types.MetricUUID {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	uuid := labelsToUUID(labels)

	if _, exists := i.pairs[uuid]; !exists {
		i.pairs[uuid] = labels

		retry.Do(func() error {
			return i.storage.Save(uuid, labels)
		}, "index", "New",
			"Can't save uuid labels pair in the storage",
			"Resolved: Save uuid labels pair in the storage",
			retry.NewBackOff(30*time.Second))
	}

	return uuid
}

// UUIDs returns UUIDs that matches with the label set
func (i *Index) UUIDs(search types.MetricLabels) map[types.MetricUUID]types.MetricLabels {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	matchers := make(map[types.MetricUUID]types.MetricLabels)

forLoop:
	for uuid, labels := range i.pairs {
		for _, label := range search {
			value, exists := labels.Value(label.Name)

			if !exists || (value != label.Value) {
				continue forLoop
			}
		}

		matchers[uuid] = labels
	}

	if len(matchers) == 0 {
		i.mutex.Unlock()
		i.UUID(search)
		i.mutex.Lock()
	}

	return matchers
}

// Returns generated UUID from labels
// If a UUID label is specified, the UUID will be generated from its value
func labelsToUUID(labels types.MetricLabels) types.MetricUUID {
	var uuid types.MetricUUID
	uuidString, exists := labels.Value("__bleemeo_uuid__")

	if exists {
		uuid.UUID = gouuid.FromStringOrNil(uuidString)
	} else {
		canonical := labels.Canonical()

		uuid.UUID = md5.Sum([]byte(canonical))
	}

	return uuid
}
