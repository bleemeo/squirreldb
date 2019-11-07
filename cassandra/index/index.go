package index

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"
	"log"
	"os"
	"sort"
	"squirreldb/compare"
	"squirreldb/retry"
	"squirreldb/types"
	"strings"
	"sync"
	"time"
)

const (
	Table = "index"
)

var logger = log.New(os.Stdout, "[index] ", log.LstdFlags)

type CassandraIndex struct {
	session    *gocql.Session
	indexTable string

	pairs map[types.MetricUUID]types.MetricLabels
	mutex sync.Mutex
}

// New creates a new CassandraIndex object
func New(session *gocql.Session, keyspace string) (*CassandraIndex, error) {
	indexTable := keyspace + "." + "\"" + Table + "\""

	createIndexTableQuery := createIndexTableQuery(session, indexTable)

	if err := createIndexTableQuery.Exec(); err != nil {
		session.Close()
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

// Pairs returns all uuid-labels pairs matching with the specified matchers
func (c *CassandraIndex) Pairs(matchers types.MetricLabels) map[types.MetricUUID]types.MetricLabels {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	pairs := make(map[types.MetricUUID]types.MetricLabels)

forLoop:
	for uuid, labels := range c.pairs {
		for _, label := range matchers {
			value, exists := labels.Value(label.Name)

			if !exists || (value != label.Value) {
				continue forLoop
			}
		}

		pairs[uuid] = labels
	}

	return pairs
}

// UUID returns the UUID associated with the specified labels
func (c *CassandraIndex) UUID(labels types.MetricLabels) types.MetricUUID {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	for uuid, value := range c.pairs {
		if compare.LabelsEqual(value, labels) {
			return uuid
		}
	}

	uuid := uuidFromLabels(labels)

	retry.Print(func() error {
		return c.savePair(uuid, labels)
	}, retry.NewBackOff(30*time.Second), logger,
		"Error: Can't save pair in the index table",
		"Resolved: Save pair in the index table")

	return uuid
}

// UUIDs returns all UUIDs
func (c *CassandraIndex) UUIDs() types.MetricUUIDs {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	uuids := make(types.MetricUUIDs, 0, len(c.pairs))

	for uuid := range c.pairs {
		uuids = append(uuids, uuid)
	}

	return uuids
}

// Saves pair in the index
func (c *CassandraIndex) savePair(uuid types.MetricUUID, labels types.MetricLabels) error {
	uuidString := uuid.String()
	uuidCQL, _ := gocql.ParseUUID(uuidString)
	labelsMap := labels.Map()

	writeIndexTableQuery := c.writeIndexTableQuery(uuidCQL, labelsMap)

	if err := writeIndexTableQuery.Exec(); err != nil {
		return err
	}

	c.pairs[uuid] = labels

	return nil
}

// Returns index table insert query
func (c *CassandraIndex) writeIndexTableQuery(uuid gocql.UUID, labels map[string]string) *gocql.Query {
	replacer := strings.NewReplacer("$INDEX_TABLE", c.indexTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $INDEX_TABLE (metric_uuid, labels)
		VALUES (?, ?)
		IF NOT EXISTS
	`), uuid.String(), labels)

	return query
}

// Returns index table create query
func createIndexTableQuery(session *gocql.Session, indexTable string) *gocql.Query {
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

// Returns index table load iterator
func loadIndexTableIterator(session *gocql.Session, indexTable string) *gocql.Iter {
	replacer := strings.NewReplacer("$INDEX_TABLE", indexTable)
	query := session.Query(replacer.Replace(`
		SELECT metric_uuid, labels FROM $INDEX_TABLE
		ALLOW FILTERING
	`))
	iterator := query.Iter()

	return iterator
}

// Returns all uuid-labels pairs
func loadPairs(session *gocql.Session, indexTable string) map[types.MetricUUID]types.MetricLabels {
	pairs := make(map[types.MetricUUID]types.MetricLabels)

	loadIndexTableIterator := loadIndexTableIterator(session, indexTable)

	var uuidString string
	var labelsMap map[string]string

	for loadIndexTableIterator.Scan(&uuidString, &labelsMap) {
		uuid := types.UUIDFromString(uuidString)
		labels := types.LabelsFromMap(labelsMap)

		pairs[uuid] = labels
	}

	return pairs
}

// Returns UUID generated from labels
// If the labels contains a UUID label, the value of the latter is used, otherwise a random UUID is generated
func uuidFromLabels(labels types.MetricLabels) types.MetricUUID {
	var uuid types.MetricUUID
	uuidString, exists := labels.Value("__bleemeo_uuid__")

	if exists {
		uuid = types.UUIDFromString(uuidString)
	} else {
		uuid.UUID, _ = gouuid.NewV4()
	}

	return uuid
}
