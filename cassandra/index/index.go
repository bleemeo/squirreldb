package index

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"log"
	"os"
	"squirreldb/retry"
	"squirreldb/types"
	"strings"
	"sync"
	"time"
)

const tableName = "index"

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[index] ", log.LstdFlags)

type CassandraIndex struct {
	session    *gocql.Session
	indexTable string

	pairs map[types.MetricUUID][]types.MetricLabel
	mutex sync.Mutex
}

// New created a new CassandraIndex object
func New(session *gocql.Session, keyspace string) (*CassandraIndex, error) {
	indexTable := keyspace + "." + "\"" + tableName + "\""
	indexTableCreateQuery := indexTableCreateQuery(session, indexTable)

	if err := indexTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	pairs := loadPairs(session, indexTable)
	index := &CassandraIndex{
		session:    session,
		indexTable: indexTable,
		pairs:      pairs,
	}

	return index, nil
}

// Labels returns a MetricLabel list corresponding to the specified MetricUUID
func (c *CassandraIndex) Labels(reference types.MetricUUID) []types.MetricLabel {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for uuid, labels := range c.pairs {
		if uuid == reference {
			return labels
		}
	}

	return nil
}

// UUID returns a MetricUUID corresponding to the specified MetricLabel list
func (c *CassandraIndex) UUID(labels []types.MetricLabel) types.MetricUUID {
	if len(labels) == 0 {
		return types.MetricUUID{}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	reference := types.LabelsSort(labels)

	for uuid, labels := range c.pairs {
		if types.LabelsEqual(labels, reference) {
			return uuid
		}
	}

	uuid := uuidFromLabels(reference)

	retry.Print(func() error {
		return c.writePair(uuid, reference)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't write uuid-labels pair in the index table",
		"Resolved: Write uuid-labels pair in the index table")

	return uuid
}

// UUIDs returns a MetricUUIDs list corresponding to the specified MetricLabelMatcher
func (c *CassandraIndex) UUIDs(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID {
	if (len(matchers) == 0) && !all {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	var uuids []types.MetricUUID

forLoop:
	for uuid, labels := range c.pairs {
		if all {
			uuids = append(uuids, uuid)

			continue forLoop
		}

		for _, matcher := range matchers {
			contains, value := types.LabelsContains(labels, matcher.Name)

			if !contains || (value != matcher.Value) {
				continue forLoop
			}
		}

		uuids = append(uuids, uuid)
	}

	return uuids
}

// Returns index table insert pair Query
func (c *CassandraIndex) indexTableInsertPairQuery(uuid string, labels map[string]string) *gocql.Query {
	replacer := strings.NewReplacer("$INDEX_TABLE", c.indexTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $INDEX_TABLE (metric_uuid, labels)
		VALUES (?, ?)
		IF NOT EXISTS
	`), uuid, labels)

	return query
}

// Writes uuid-labels pair in the index table
func (c *CassandraIndex) writePair(uuid types.MetricUUID, labels []types.MetricLabel) error {
	uuidString := uuid.String()
	labelsMap := types.MapFromLabels(labels)
	writePairQuery := c.indexTableInsertPairQuery(uuidString, labelsMap)

	if err := writePairQuery.Exec(); err != nil {
		return err
	}

	c.pairs[uuid] = labels

	return nil
}

// Returns index table create Query
func indexTableCreateQuery(session *gocql.Session, indexTable string) *gocql.Query {
	replacer := strings.NewReplacer("$INDEX_TABLE", indexTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $INDEX_TABLE (
			metric_uuid uuid,
			labels map<text, text>,
			PRIMARY KEY (metric_uuid)
		)
	`))

	return query
}

// Returns index table select pair Iter
func indexTableSelectPairIter(session *gocql.Session, indexTable string) *gocql.Iter {
	replacer := strings.NewReplacer("$INDEX_TABLE", indexTable)
	query := session.Query(replacer.Replace(`
		SELECT metric_uuid, labels FROM $INDEX_TABLE
		ALLOW FILTERING
	`))
	iter := query.Iter()

	return iter
}

// Returns all uuid-labels pairs from the index table
func loadPairs(session *gocql.Session, indexTable string) map[types.MetricUUID][]types.MetricLabel {
	pairs := make(map[types.MetricUUID][]types.MetricLabel)
	loadPairIter := indexTableSelectPairIter(session, indexTable)

	var (
		uuidString string
		labelsMap  map[string]string
	)

	for loadPairIter.Scan(&uuidString, &labelsMap) {
		uuid, err := types.UUIDFromString(uuidString)

		if err != nil {
			logger.Printf("Error: Can't generate UUID from string (%v)", err)
		}

		labels := types.LabelsFromMap(labelsMap)

		pairs[uuid] = labels
	}

	return pairs
}

// Returns a MetricUUID generated from a MetricLabels list
func uuidFromLabels(labels []types.MetricLabel) types.MetricUUID {
	contains, uuidString := types.LabelsContains(labels, "__bleemeo_uuid__")

	var (
		uuid types.MetricUUID
		err  error
	)

	if contains {
		uuid, err = types.UUIDFromString(uuidString)
	} else {
		uuid.UUID, err = gouuid.NewV4()
	}

	if err != nil {
		logger.Printf("Error: Can't generate UUID from string (%v)", err)
	}

	return uuid
}
