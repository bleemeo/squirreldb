package match

import "squirreldb/types"

type Match struct {
	UUIDLabels map[types.MetricUUID]types.MetricLabels
}

func NewMatch() *Match {
	return &Match{}
}

// Match ...
func (m *Match) Match(labels types.MetricLabels) (types.MetricUUID, error) {
	uuid := labels.UUID()

	m.UUIDLabels[uuid] = labels

	return uuid, nil
}

// Matches ...
func (m *Match) Matches(searchLabels types.MetricLabels) ([]types.MetricUUID, error) {
	var uuids []types.MetricUUID

forLoop:
	for uuid, labels := range m.UUIDLabels {
		for _, label := range searchLabels {
			_, contains := labels.Value(label.Name)

			if !contains {
				continue forLoop
			}
		}

		uuids = append(uuids, uuid)
	}

	if len(uuids) == 0 {
		uuid := searchLabels.UUID()

		uuids = append(uuids, uuid)
	}

	return uuids, nil
}
