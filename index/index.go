package index

import (
	"crypto/md5"
	gouuid "github.com/gofrs/uuid"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

type Storer interface {
	Retrieve() (map[types.MetricUUID]types.MetricLabels, error)
	Save(uuid types.MetricUUID, labels types.MetricLabels) error
}

type Index struct {
	store Storer

	pairs map[types.MetricUUID]types.MetricLabels
	mutex sync.Mutex
}

// New creates a new Index object
func New(storage Storer) *Index {
	var pairs map[types.MetricUUID]types.MetricLabels

	retry.Do(func() error {
		var err error
		pairs, err = storage.Retrieve()

		return err
	}, "index", "New",
		"Error: Can't get uuid-labels pairs from the store",
		"Resolved: Get uuid-labels pairs from the store",
		retry.NewBackOff(30*time.Second))

	index := &Index{
		store: storage,
		pairs: pairs,
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
			return i.store.Save(uuid, labels)
		}, "index", "UUID",
			"Error: Can't save uuid-labels pair in the store",
			"Resolved: Save uuid-labels pair in the store",
			retry.NewBackOff(30*time.Second))
	}

	return uuid
}

// UUIDs returns UUIDs that matches with the label set
func (i *Index) UUIDs(matchers types.MetricLabels) map[types.MetricUUID]types.MetricLabels {
	pairs := make(map[types.MetricUUID]types.MetricLabels)

	i.mutex.Lock()

forLoop:
	for uuid, labels := range i.pairs {
		for _, label := range matchers {
			value, exists := labels.Value(label.Name)

			if !exists || (value != label.Value) {
				continue forLoop
			}
		}

		pairs[uuid] = labels
	}

	i.mutex.Unlock()

	if len(pairs) == 0 {
		i.UUID(matchers)
	}

	return pairs
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
