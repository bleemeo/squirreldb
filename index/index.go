package index

import "squirreldb/types"

type Index struct {
	Pairs map[types.MetricUUID]types.MetricLabels
}

// New creates a new Index object
func New() *Index {
	return &Index{
		Pairs: make(map[types.MetricUUID]types.MetricLabels),
	}
}

// UUID returns UUID generated from the labels and save the index
func (m *Index) UUID(labels types.MetricLabels) types.MetricUUID {
	uuid := labels.UUID()

	m.Pairs[uuid] = labels

	return uuid
}

// UUIDs returns UUIDs that matches with the label set
func (m *Index) UUIDs(labelSet types.MetricLabels) map[types.MetricUUID]types.MetricLabels {
	matchers := make(map[types.MetricUUID]types.MetricLabels)

	if len(matchers) == 0 {
		uuid := labelSet.UUID()

		matchers[uuid] = labelSet
	}

	return matchers
}
