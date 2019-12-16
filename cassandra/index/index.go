package index

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"context"
	"log"
	"os"
	"regexp"
	"squirreldb/types"
	"strings"
	"sync"
	"time"
)

const (
	labelsTableName   = "labels"
	postingsTableName = "postings"
	uuidsTableName    = "uuids"
)

const expiratorInterval = 60

const timeToLive = 300

const (
	uuidLabelName = "__uuid__"
)

const (
	matcherTypeEq  = 0
	matcherTypeNeq = 1
	matcherTypeRe  = 2
	matcherTypeNre = 3
)

const (
	targetTypeKeyUndefined = 0
	targetTypeKeyDefined   = 1
	targetTypeValueEqual   = 2
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[index] ", log.LstdFlags)

type labelsData struct {
	labels              []types.MetricLabel
	expirationTimestamp int64
}

type uuidData struct {
	uuid                types.MetricUUID
	expirationTimestamp int64
}

type CassandraIndex struct {
	session       *gocql.Session
	labelsTable   string
	postingsTable string
	uuidsTable    string

	labelsToUUID  map[string]uuidData
	ltuMutex      sync.Mutex
	uuidsToLabels map[types.MetricUUID]labelsData
	utlMutex      sync.Mutex
}

// New creates a new CassandraIndex object
func New(session *gocql.Session, keyspace string) (*CassandraIndex, error) {
	labelsTable := keyspace + "." + "\"" + labelsTableName + "\""
	labelsTableCreateQuery := labelsTableCreateQuery(session, labelsTable)

	if err := labelsTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	postingsTable := keyspace + "." + "\"" + postingsTableName + "\""
	postingsTableCreateQuery := postingsTableCreateQuery(session, postingsTable)

	if err := postingsTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	uuidsTable := keyspace + "." + "\"" + uuidsTableName + "\""
	uuidsTableCreateQuery := uuidsTableCreateQuery(session, uuidsTable)

	if err := uuidsTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	index := &CassandraIndex{
		session:       session,
		labelsTable:   labelsTable,
		postingsTable: postingsTable,
		uuidsTable:    uuidsTable,
		labelsToUUID:  make(map[string]uuidData),
		uuidsToLabels: make(map[types.MetricUUID]labelsData),
	}

	return index, nil
}

// Run starts all Cassandra Index services
func (c *CassandraIndex) Run(ctx context.Context) {
	c.runExpirator(ctx)
}

// Starts the expirator service
// If a stop signal is received, the service is stopped
func (c *CassandraIndex) runExpirator(ctx context.Context) {
	interval := expiratorInterval * time.Second
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			c.expire(time.Now())
		case <-ctx.Done():
			logger.Println("Expirator service stopped")
			return
		}
	}
}

// AllUUIDs returns all UUIDs stored in the UUIDs index
func (c *CassandraIndex) AllUUIDs() ([]types.MetricUUID, error) {
	uuidsTableSelectUUIDsQuery := c.uuidsTableSelectUUIDsQuery()
	uuidsTableSelectUUIDsIter := uuidsTableSelectUUIDsQuery.Iter()

	var (
		uuids   []types.MetricUUID
		cqlUUID gocql.UUID
	)

	for uuidsTableSelectUUIDsIter.Scan(&cqlUUID) {
		uuid := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

		uuids = append(uuids, uuid)
	}

	if err := uuidsTableSelectUUIDsIter.Close(); err != nil {
		return nil, err
	}

	return uuids, nil
}

// Labels returns a MetricLabel list corresponding to the specified MetricUUID
func (c *CassandraIndex) Labels(uuid types.MetricUUID, withUUID bool) ([]types.MetricLabel, error) {
	start := time.Now()

	c.utlMutex.Lock()
	defer c.utlMutex.Unlock()

	now := time.Now()

	labelsData, inCache := c.uuidsToLabels[uuid]

	if !inCache {
		selectLabelsQuery := c.uuidsTableSelectLabelsQuery(uuid.String())

		var labelsMap map[string]string

		if err := selectLabelsQuery.Scan(&labelsMap); (err != nil) && (err != gocql.ErrNotFound) {
			return nil, err
		}

		labelsData.labels = types.LabelsFromMap(labelsMap)
	}

	labelsData.expirationTimestamp = now.Unix() + timeToLive
	c.uuidsToLabels[uuid] = labelsData

	labels := types.CopyLabels(labelsData.labels)

	if withUUID {
		label := types.MetricLabel{
			Name:  uuidLabelName,
			Value: uuid.String(),
		}

		labels = append(labels, label)
	}

	requestsSecondsLabels.Observe(time.Since(start).Seconds())

	return labels, nil
}

// UUID returns a MetricUUID corresponding to the specified MetricLabel list
func (c *CassandraIndex) UUID(labels []types.MetricLabel) (types.MetricUUID, error) {
	if len(labels) == 0 {
		return types.MetricUUID{}, nil
	}

	start := time.Now()

	c.ltuMutex.Lock()
	defer c.ltuMutex.Unlock()

	now := time.Now()

	labelsString := types.StringFromLabels(labels)

	uuidData, inCache := c.labelsToUUID[labelsString]

	var inPersistent bool

	if !inCache {
		selectUUIDQuery := c.labelsTableSelectUUIDQuery(labelsString)

		var cqlUUID gocql.UUID

		if err := selectUUIDQuery.Scan(&cqlUUID); err == nil {
			uuid := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

			uuidData.uuid = uuid
			inPersistent = true
		} else if err != gocql.ErrNotFound {
			return types.MetricUUID{}, err
		}
	}

	if !inCache && !inPersistent {
		cqlUUID, err := gocql.RandomUUID()

		if err != nil {
			return types.MetricUUID{}, err
		}

		indexBatch := c.session.NewBatch(gocql.LoggedBatch)
		insertUUIDQueryString := c.labelsTableInsertUUIDQueryString()
		uuid := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

		indexBatch.Query(insertUUIDQueryString, labelsString, uuid.String())

		insertLabelsQueryString := c.uuidsTableInsertLabelsQueryString()
		labelsMap := types.MapFromLabels(labels)

		indexBatch.Query(insertLabelsQueryString, uuid.String(), labelsMap)

		for _, label := range labels {
			updateUUIDsQueryString := c.postingsTableUpdateUUIDsQueryString(uuid.String())

			indexBatch.Query(updateUUIDsQueryString, label.Name, label.Value)
		}

		if err := c.session.ExecuteBatch(indexBatch); err != nil {
			return types.MetricUUID{}, err
		}

		uuidData.uuid = uuid
	}

	uuidData.expirationTimestamp = now.Unix() + timeToLive
	c.labelsToUUID[labelsString] = uuidData

	requestsSecondsUUID.Observe(time.Since(start).Seconds())

	return uuidData.uuid, nil
}

// UUIDs returns a MetricUUID list corresponding to the specified MetricLabelMatcher list
func (c *CassandraIndex) UUIDs(matchers []types.MetricLabelMatcher) ([]types.MetricUUID, error) {
	if len(matchers) == 0 {
		return nil, nil
	}

	start := time.Now()

	targetLabels, err := c.targetLabels(matchers)

	if err != nil {
		return nil, err
	}

	uuidMatches, err := c.uuidMatches(targetLabels)

	if err != nil {
		return nil, err
	}

	var uuids []types.MetricUUID

	for uuid, matches := range uuidMatches {
		if matches == len(matchers) {
			uuids = append(uuids, uuid)
		}
	}

	requestsSecondsUUIDs.Observe(time.Since(start).Seconds())

	return uuids, nil
}

func (c *CassandraIndex) expire(now time.Time) {
	c.ltuMutex.Lock()
	c.utlMutex.Lock()
	defer c.ltuMutex.Unlock()
	defer c.utlMutex.Unlock()

	for labelsString, uuidData := range c.labelsToUUID {
		if uuidData.expirationTimestamp < now.Unix() {
			delete(c.labelsToUUID, labelsString)
		}
	}

	for uuid, labelsData := range c.uuidsToLabels {
		if labelsData.expirationTimestamp < now.Unix() {
			delete(c.uuidsToLabels, uuid)
		}
	}
}

// Returns labels by target type
func (c *CassandraIndex) targetLabels(matchers []types.MetricLabelMatcher) (map[int][]types.MetricLabel, error) {
	if len(matchers) == 0 {
		return nil, nil
	}

	targetLabels := make(map[int][]types.MetricLabel)

	for _, matcher := range matchers {
		targetLabel := types.MetricLabel{
			Name: matcher.Name,
		}

		if matcher.Value == "" {
			targetLabel.Value = ""

			switch matcher.Type {
			case matcherTypeEq, matcherTypeRe:
				targetLabels[targetTypeKeyUndefined] = append(targetLabels[targetTypeKeyUndefined], targetLabel)
			case matcherTypeNeq, matcherTypeNre:
				targetLabels[targetTypeKeyDefined] = append(targetLabels[targetTypeKeyDefined], targetLabel)
			}
		} else {
			selectValueQuery := c.postingsTableSelectValueQuery(matcher.Name)
			selectValueIter := selectValueQuery.Iter()

			var (
				values []string
				value  string
			)

			for selectValueIter.Scan(&value) {
				values = append(values, value)
			}

			if err := selectValueIter.Close(); err != nil {
				return nil, err
			}

			var regex *regexp.Regexp

			if (matcher.Type == matcherTypeRe) || (matcher.Type == matcherTypeNre) {
				var err error
				regex, err = regexp.Compile("^(?:" + matcher.Value + ")$")

				if err != nil {
					return nil, err
				}
			}

			for _, value := range values {
				targetLabel.Value = value

				if ((matcher.Type == matcherTypeEq) && (matcher.Value == value)) ||
					((matcher.Type == matcherTypeNeq) && (matcher.Value != value)) ||
					((matcher.Type == matcherTypeRe) && regex.MatchString(value)) ||
					((matcher.Type == matcherTypeNre) && !regex.MatchString(value)) {
					targetLabels[targetTypeValueEqual] = append(targetLabels[targetTypeValueEqual], targetLabel)
				}
			}
		}
	}

	return targetLabels, nil
}

// Returns a list of uuid associated with the number of times it has corresponded to a targeted label
func (c *CassandraIndex) uuidMatches(targetLabels map[int][]types.MetricLabel) (map[types.MetricUUID]int, error) {
	if len(targetLabels) == 0 {
		return nil, nil
	}

	uuidMatches := make(map[types.MetricUUID]int)

	for _, label := range targetLabels[targetTypeValueEqual] {
		selectUUIDsQuery := c.postingsTableSelectUUIDsFocusQuery(label.Name, label.Value)
		selectUUIDsIter := selectUUIDsQuery.Iter()

		var (
			labelUUIDs []types.MetricUUID
			cqlUUIDs   []gocql.UUID
		)

		for selectUUIDsIter.Scan(&cqlUUIDs) {
			for _, cqlUUID := range cqlUUIDs {
				labelUUID := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

				labelUUIDs = append(labelUUIDs, labelUUID)
			}
		}

		if err := selectUUIDsIter.Close(); err != nil {
			return nil, err
		}

		for _, uuid := range labelUUIDs {
			uuidMatches[uuid]++
		}
	}

	for _, label := range targetLabels[targetTypeKeyDefined] {
		selectUUIDsQuery := c.postingsTableSelectUUIDsQuery(label.Name)
		selectUUIDsIter := selectUUIDsQuery.Iter()

		var (
			labelUUIDs []types.MetricUUID
			cqlUUIDs   []gocql.UUID
		)

		for selectUUIDsIter.Scan(&cqlUUIDs) {
			for _, cqlUUID := range cqlUUIDs {
				labelUUID := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

				labelUUIDs = append(labelUUIDs, labelUUID)
			}
		}

		if err := selectUUIDsIter.Close(); err != nil {
			return nil, err
		}

		for _, uuid := range labelUUIDs {
			uuidMatches[uuid]++
		}
	}

	for _, label := range targetLabels[targetTypeKeyUndefined] {
		selectUUIDsQuery := c.postingsTableSelectUUIDsQuery(label.Name)
		selectUUIDsIter := selectUUIDsQuery.Iter()

		var (
			labelUUIDs []types.MetricUUID
			cqlUUIDs   []gocql.UUID
		)

		for selectUUIDsIter.Scan(&cqlUUIDs) {
			for _, cqlUUID := range cqlUUIDs {
				labelUUID := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

				labelUUIDs = append(labelUUIDs, labelUUID)
			}
		}

		if err := selectUUIDsIter.Close(); err != nil {
			return nil, err
		}

		for uuid := range uuidMatches {
			if !containsUUIDs(labelUUIDs, uuid) {
				uuidMatches[uuid]++
			}
		}
	}

	return uuidMatches, nil
}

// Returns labels table insert uuid Query
func (c *CassandraIndex) labelsTableInsertUUIDQueryString() string {
	replacer := strings.NewReplacer("$LABELS_TABLE", c.labelsTable)
	queryString := replacer.Replace(`
		INSERT INTO $LABELS_TABLE (labels, uuid)
		VALUES (?, ?)
	`)

	return queryString
}

// Returns labels table select uuid Query
func (c *CassandraIndex) labelsTableSelectUUIDQuery(labels string) *gocql.Query {
	replacer := strings.NewReplacer("$LABELS_TABLE", c.labelsTable)
	query := c.session.Query(replacer.Replace(`
		SELECT uuid FROM $LABELS_TABLE
		WHERE labels = ?
	`), labels)

	return query
}

// Returns postings table select uuids Query
func (c *CassandraIndex) postingsTableSelectUUIDsQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable)

	query := c.session.Query(replacer.Replace(`
		SELECT uuids FROM $POSTINGS_TABLE
		WHERE name = ?
	`), name)

	return query
}

// Returns postings table select uuids with name focus Query
func (c *CassandraIndex) postingsTableSelectUUIDsFocusQuery(name, value string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable)

	query := c.session.Query(replacer.Replace(`
		SELECT uuids FROM $POSTINGS_TABLE
		WHERE name = ? AND value = ?
	`), name, value)

	return query
}

// Returns postings table select value Query
func (c *CassandraIndex) postingsTableSelectValueQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable)
	query := c.session.Query(replacer.Replace(`
		SELECT value FROM $POSTINGS_TABLE
		WHERE name = ?
	`), name)

	return query
}

// Returns postings table update uuids Query as string
func (c *CassandraIndex) postingsTableUpdateUUIDsQueryString(uuid string) string {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable, "$UUID", uuid)
	queryString := replacer.Replace(`
		UPDATE $POSTINGS_TABLE
		SET uuids = uuids + {$UUID}
		WHERE name = ? AND value = ?
	`)

	return queryString
}

// Returns uuids table insert labels Query as string
func (c *CassandraIndex) uuidsTableInsertLabelsQueryString() string {
	replacer := strings.NewReplacer("$UUIDS_TABLE", c.uuidsTable)
	queryString := replacer.Replace(`
		INSERT INTO $UUIDS_TABLE (uuid, labels)
		VALUES (?, ?)
	`)

	return queryString
}

// Returns uuids table select labels Query
func (c *CassandraIndex) uuidsTableSelectLabelsQuery(uuid string) *gocql.Query {
	replacer := strings.NewReplacer("$UUIDS_TABLE", c.uuidsTable)
	query := c.session.Query(replacer.Replace(`
		SELECT labels FROM $UUIDS_TABLE
		WHERE uuid = ?
	`), uuid)

	return query
}

// Returns uuids table select labels all Query
func (c *CassandraIndex) uuidsTableSelectUUIDsQuery() *gocql.Query {
	replacer := strings.NewReplacer("$UUIDS_TABLE", c.uuidsTable)
	query := c.session.Query(replacer.Replace(`
		SELECT uuid FROM $UUIDS_TABLE
		ALLOW FILTERING
	`))

	return query
}

// Returns labels table create Query
func labelsTableCreateQuery(session *gocql.Session, labelsTable string) *gocql.Query {
	replacer := strings.NewReplacer("$LABELS_TABLE", labelsTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $LABELS_TABLE (
			labels text,
			uuid uuid,
			PRIMARY KEY (labels)
		)
	`))

	return query
}

// Returns postings table create Query
func postingsTableCreateQuery(session *gocql.Session, postingsTable string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", postingsTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $POSTINGS_TABLE (
			name text,
			value text,
			uuids set<uuid>,
			PRIMARY KEY (name, value)
		)
	`))

	return query
}

// Returns uuids table create Query
func uuidsTableCreateQuery(session *gocql.Session, uuidsTable string) *gocql.Query {
	replacer := strings.NewReplacer("$UUIDS_TABLE", uuidsTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $UUIDS_TABLE (
			uuid uuid,
			labels map<text, text>,
			PRIMARY KEY (uuid)
		)
	`))

	return query
}

// Returns a boolean if the uuid list contains the target uuid or not
func containsUUIDs(list []types.MetricUUID, target types.MetricUUID) bool {
	if len(list) == 0 {
		return false
	}

	for _, uuid := range list {
		if uuid == target {
			return true
		}
	}

	return false
}
