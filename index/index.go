package index

import (
	"github.com/cenkalti/backoff"
	"log"
	"os"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"
)

var logger = log.New(os.Stdout, "[index] ", log.LstdFlags)

type Index struct {
	indexerTable types.MetricIndexerTable

	pairs map[types.MetricUUID]types.MetricLabels
	mutex sync.Mutex
}

// New creates a new Index object
func New(indexerTable types.MetricIndexerTable) *Index {
	var pairs map[types.MetricUUID]types.MetricLabels

	_ = backoff.Retry(func() error {
		var err error
		pairs, err = indexerTable.Request()

		if err != nil {
			logger.Println("UUID: Can't request uuid labels pairs from the index table (", err, ")")
		}

		return err
	}, retry.NewBackOff(30*time.Second))

	index := &Index{
		indexerTable: indexerTable,
		pairs:        pairs,
	}

	return index
}

// UUID returns UUID generated from the labels and save the index
func (i *Index) UUID(labels types.MetricLabels) types.MetricUUID {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	uuid := labels.UUID()

	if _, exists := i.pairs[uuid]; !exists {
		i.pairs[uuid] = labels

		_ = backoff.Retry(func() error {
			err := i.indexerTable.Save(uuid, labels)

			if err != nil {
				logger.Println("UUID: Can't save uuid labels pair in the index table (", err, ")")
			}

			return err
		}, retry.NewBackOff(30*time.Second))
	}

	return uuid
}

// UUIDs returns UUIDs that matches with the label set
func (i *Index) UUIDs(labelSet types.MetricLabels) map[types.MetricUUID]types.MetricLabels {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	matchers := make(map[types.MetricUUID]types.MetricLabels)

forLoop:
	for uuid, labels := range i.pairs {
		for _, label := range labelSet {
			value, exists := labels.Value(label.Name)

			if !exists || (value != label.Value) {
				continue forLoop
			}
		}

		matchers[uuid] = labels
	}

	if len(matchers) == 0 {
		uuid := labelSet.UUID()

		matchers[uuid] = labelSet
	}

	return matchers
}
