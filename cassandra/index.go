package cassandra

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"
	"squirreldb/types"
	"strings"
)

// Retrieve reads all the uuid-labels pairs from the index table
func (c *Cassandra) Retrieve() (map[types.MetricUUID]types.MetricLabels, error) {
	iterator := c.readIndex()
	var metricUUID string
	var metricLabels map[string]string
	pairs := make(map[types.MetricUUID]types.MetricLabels)

	for iterator.Scan(&metricUUID, &metricLabels) {
		uuid := types.MetricUUID{
			UUID: gouuid.FromStringOrNil(metricUUID),
		}
		labelsItem := types.LabelsFromMap(metricLabels)

		pairs[uuid] = labelsItem
	}

	return pairs, nil
}

// Save writes an uuid-labels pair in the index table
func (c *Cassandra) Save(uuid types.MetricUUID, labels types.MetricLabels) error {
	if err := c.writeIndex(gocql.UUID(uuid.UUID), labels.Map()); err != nil {
		return err
	}

	return nil
}

// Reads from the index table
func (c *Cassandra) readIndex() *gocql.Iter {
	iteratorReplacer := strings.NewReplacer("$INDEX_TABLE", c.options.indexTable)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT metric_uuid, labels FROM $INDEX_TABLE
		ALLOW FILTERING
	`)).Iter()

	return iterator
}

// Writes in the index table
func (c *Cassandra) writeIndex(uuid gocql.UUID, labels map[string]string) error {
	insertReplacer := strings.NewReplacer("$INDEX_TABLE", c.options.indexTable)
	insert := c.session.Query(insertReplacer.Replace(`
		INSERT INTO $INDEX_TABLE (metric_uuid, labels)
		VALUES (?, ?)
		IF NOT EXISTS
	`), uuid, labels)

	if err := insert.Exec(); err != nil {
		return err
	}

	return nil
}
