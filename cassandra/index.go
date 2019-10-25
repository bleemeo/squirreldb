package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/gofrs/uuid"
	"squirreldb/types"
	"strings"
)

func (c *Cassandra) Request() (map[types.MetricUUID]types.MetricLabels, error) {
	iterator := c.readIndex()
	var metricUUID string
	var metricLabels map[string]string
	pairs := make(map[types.MetricUUID]types.MetricLabels)

	for iterator.Scan(&metricUUID, &metricLabels) {
		uuidItem := types.MetricUUID{
			UUID: uuid.FromStringOrNil(metricUUID),
		}
		labelsItem := types.LabelsFromMap(metricLabels)

		pairs[uuidItem] = labelsItem
	}

	return pairs, nil
}

func (c *Cassandra) Save(uuid types.MetricUUID, labels types.MetricLabels) error {
	if err := c.writeIndex(gocql.UUID(uuid.UUID), labels.Map()); err != nil {
		return err
	}

	return nil
}

func (c *Cassandra) readIndex() *gocql.Iter {
	iteratorReplacer := strings.NewReplacer("$INDEX_TABLE", c.options.indexTable)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT metric_uuid, labels FROM $INDEX_TABLE
		ALLOW FILTERING
	`)).Iter()

	return iterator
}

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
