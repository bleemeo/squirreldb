package match

import "squirreldb/types"

type Match struct {
	Matchers map[types.MetricUUID]types.MetricLabels
}

func NewMatch() *Match {
	return &Match{
		Matchers: make(map[types.MetricUUID]types.MetricLabels),
	}
}

// UUID returns UUID generated from the labels and save the match
func (m *Match) UUID(labels types.MetricLabels) types.MetricUUID {
	uuid := labels.UUID()

	m.Matchers[uuid] = labels

	return uuid
}

// UUIDs returns UUIDs that matches with the label set
func (m *Match) UUIDs(labelSet types.MetricLabels) map[types.MetricUUID]types.MetricLabels {
	matchers := make(map[types.MetricUUID]types.MetricLabels)

	if len(matchers) == 0 {
		uuid := labelSet.UUID()

		matchers[uuid] = labelSet
	}

	return matchers
}
