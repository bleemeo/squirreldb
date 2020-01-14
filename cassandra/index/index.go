package index

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"context"
	"log"
	"os"
	"regexp"
	"squirreldb/debug"
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

const cacheExpirationDelay = 300

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
	uuid                gouuid.UUID
	expirationTimestamp int64
}

type Options struct {
	IncludeUUID bool
}

type CassandraIndex struct {
	session       *gocql.Session
	options       Options
	labelsTable   string
	postingsTable string
	uuidsTable    string

	labelsToUUID  map[string]uuidData
	ltuMutex      sync.Mutex
	uuidsToLabels map[gouuid.UUID]labelsData
	utlMutex      sync.Mutex
}

// New creates a new CassandraIndex object
func New(session *gocql.Session, keyspace string, options Options) (*CassandraIndex, error) {
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
		options:       options,
		labelsTable:   labelsTable,
		postingsTable: postingsTable,
		uuidsTable:    uuidsTable,
		labelsToUUID:  make(map[string]uuidData),
		uuidsToLabels: make(map[gouuid.UUID]labelsData),
	}

	return index, nil
}

// Run starts all Cassandra Index services
func (c *CassandraIndex) Run(ctx context.Context) {
	interval := expiratorInterval * time.Second
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			c.expire(time.Now())
		case <-ctx.Done():
			debug.Print(2, logger, "Cassandra index service stopped")
			return
		}
	}
}

// AllUUIDs returns all UUIDs stored in the UUIDs index
func (c *CassandraIndex) AllUUIDs() ([]gouuid.UUID, error) {
	uuidsTableSelectUUIDsQuery := c.uuidsTableSelectUUIDsQuery()
	uuidsTableSelectUUIDsIter := uuidsTableSelectUUIDsQuery.Iter()

	var (
		uuids   []gouuid.UUID
		cqlUUID gocql.UUID
	)

	for uuidsTableSelectUUIDsIter.Scan(&cqlUUID) {
		uuids = append(uuids, gouuid.UUID(cqlUUID))
	}

	if err := uuidsTableSelectUUIDsIter.Close(); err != nil {
		return nil, err
	}

	return uuids, nil
}

// LookupLabels returns a MetricLabel list corresponding to the specified UUID
func (c *CassandraIndex) LookupLabels(uuid gouuid.UUID) ([]types.MetricLabel, error) {
	start := time.Now()

	c.utlMutex.Lock()
	defer c.utlMutex.Unlock()

	labelsData, found := c.uuidsToLabels[uuid]

	if !found {
		selectLabelsQuery := c.uuidsTableSelectLabelsQuery(gocql.UUID(uuid))

		if err := selectLabelsQuery.Scan(&labelsData.labels); (err != nil) && (err != gocql.ErrNotFound) {
			lookupLabelsSeconds.Observe(time.Since(start).Seconds())

			return nil, err
		}
	}

	now := time.Now()

	labelsData.expirationTimestamp = now.Unix() + cacheExpirationDelay
	c.uuidsToLabels[uuid] = labelsData

	labels := types.CopyLabels(labelsData.labels)

	if c.options.IncludeUUID {
		label := types.MetricLabel{
			Name:  uuidLabelName,
			Value: uuid.String(),
		}

		labels = append(labels, label)
	}

	lookupLabelsSeconds.Observe(time.Since(start).Seconds())

	return labels, nil
}

// LookupUUID returns a UUID corresponding to the specified MetricLabel list
func (c *CassandraIndex) LookupUUID(labels []types.MetricLabel) (gouuid.UUID, error) {
	start := time.Now()

	defer func() {
		lookupUUIDSeconds.Observe(time.Since(start).Seconds())
	}()

	if len(labels) == 0 {
		return gouuid.UUID{}, nil
	}

	c.ltuMutex.Lock()
	defer c.ltuMutex.Unlock()

	var (
		uuidData uuidData
		found    bool
	)

	if c.options.IncludeUUID {
		var uuidStr string
		uuidStr, found = types.GetLabelsValue(labels, uuidLabelName)

		if found {
			var err error
			uuidData.uuid, err = gouuid.FromString(uuidStr)

			if err != nil {
				return gouuid.UUID{}, nil
			}
		}
	}

	var labelsKey string

	if !found {
		labelsKey = keyFromLabels(labels)

		uuidData, found = c.labelsToUUID[labelsKey]
	}

	var (
		sortedLabelsString string
		sortedLabels       []types.MetricLabel
	)

	if !found {
		lookupUUIDMisses.Inc()

		sortedLabels = types.SortLabels(labels)

		sortedLabelsString = types.StringFromLabels(sortedLabels)

		selectUUIDQuery := c.labelsTableSelectUUIDQuery(sortedLabelsString)

		var cqlUUID gocql.UUID

		if err := selectUUIDQuery.Scan(&cqlUUID); err == nil {
			uuidData.uuid = gouuid.UUID(cqlUUID)
			found = true
		} else if err != gocql.ErrNotFound {
			return gouuid.UUID{}, err
		}
	}

	if !found {
		lookupUUIDNew.Inc()

		cqlUUID, err := gocql.RandomUUID()

		if err != nil {
			return gouuid.UUID{}, err
		}

		indexBatch := c.session.NewBatch(gocql.LoggedBatch)
		insertUUIDQueryString := c.labelsTableInsertUUIDQueryString()
		uuid := gouuid.UUID(cqlUUID)

		indexBatch.Query(insertUUIDQueryString, sortedLabelsString, gocql.UUID(uuid))

		insertLabelsQueryString := c.uuidsTableInsertLabelsQueryString()

		indexBatch.Query(insertLabelsQueryString, gocql.UUID(uuid), sortedLabels)

		for _, label := range sortedLabels {
			updateUUIDsQueryString := c.postingsTableUpdateUUIDsQueryString()

			indexBatch.Query(updateUUIDsQueryString, []gocql.UUID{gocql.UUID(uuid)}, label.Name, label.Value)
		}

		if err := c.session.ExecuteBatch(indexBatch); err != nil {
			return gouuid.UUID{}, err
		}

		uuidData.uuid = uuid
	}

	now := time.Now()

	uuidData.expirationTimestamp = now.Unix() + cacheExpirationDelay
	c.labelsToUUID[labelsKey] = uuidData

	return uuidData.uuid, nil
}

// Search returns a list of UUID corresponding to the specified MetricLabelMatcher list
func (c *CassandraIndex) Search(matchers []types.MetricLabelMatcher) ([]gouuid.UUID, error) {
	start := time.Now()

	defer func() {
		searchMetricsSeconds.Observe(time.Since(start).Seconds())
	}()

	if len(matchers) == 0 {
		return nil, nil
	}

	var (
		uuids []gouuid.UUID
		found bool
	)

	if c.options.IncludeUUID {
		var uuidStr string
		uuidStr, found = types.GetMatchersValue(matchers, uuidLabelName)

		if found {
			uuid, err := gouuid.FromString(uuidStr)

			if err != nil {
				return nil, nil
			}

			uuids = append(uuids, uuid)
		}
	}

	if !found {
		targetLabels, err := c.targetLabels(matchers)

		if err != nil {
			return nil, err
		}

		uuidMatches, err := c.uuidMatches(targetLabels)

		if err != nil {
			return nil, err
		}

		for uuid, matches := range uuidMatches {
			if matches == len(matchers) {
				uuids = append(uuids, uuid)
			}
		}
	}

	searchMetricsTotal.Add(float64(len(uuids)))

	return uuids, nil
}

// Deletes all expired cache entries
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
func (c *CassandraIndex) targetLabels(matchers []types.MetricLabelMatcher) (map[int][]types.MetricLabel, error) { // nolint:gocognit
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
func (c *CassandraIndex) uuidMatches(targetLabels map[int][]types.MetricLabel) (map[gouuid.UUID]int, error) { // nolint:gocognit
	if len(targetLabels) == 0 {
		return nil, nil
	}

	uuidMatches := make(map[gouuid.UUID]int)

	for _, label := range targetLabels[targetTypeValueEqual] {
		selectUUIDsQuery := c.postingsTableSelectUUIDsFocusQuery(label.Name, label.Value)
		selectUUIDsIter := selectUUIDsQuery.Iter()

		var (
			labelUUIDs []gouuid.UUID
			cqlUUIDs   []gocql.UUID
		)

		for selectUUIDsIter.Scan(&cqlUUIDs) {
			for _, cqlUUID := range cqlUUIDs {
				labelUUIDs = append(labelUUIDs, gouuid.UUID(cqlUUID))
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
			labelUUIDs []gouuid.UUID
			cqlUUIDs   []gocql.UUID
		)

		for selectUUIDsIter.Scan(&cqlUUIDs) {
			for _, cqlUUID := range cqlUUIDs {
				labelUUID := gouuid.UUID(cqlUUID)

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
			labelUUIDs []gouuid.UUID
			cqlUUIDs   []gocql.UUID
		)

		for selectUUIDsIter.Scan(&cqlUUIDs) {
			for _, cqlUUID := range cqlUUIDs {
				labelUUIDs = append(labelUUIDs, gouuid.UUID(cqlUUID))
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
func (c *CassandraIndex) postingsTableUpdateUUIDsQueryString() string {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable)
	queryString := replacer.Replace(`
		UPDATE $POSTINGS_TABLE
		SET uuids = uuids + ?
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
func (c *CassandraIndex) uuidsTableSelectLabelsQuery(uuid gocql.UUID) *gocql.Query {
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
			labels frozen<list<tuple<text, text>>>,
			PRIMARY KEY (uuid)
		)
	`))

	return query
}

// Returns a boolean if the uuid list contains the target uuid or not
func containsUUIDs(list []gouuid.UUID, target gouuid.UUID) bool {
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

// keyFromLabels returns a string key generated from a MetricLabel list
func keyFromLabels(labels []types.MetricLabel) string {
	if len(labels) == 0 {
		return ""
	}

	strLabels := make([]string, len(labels)*2)

	for i, label := range labels {
		strLabels[i*2] = label.Name
		strLabels[i*2+1] = label.Value
	}

	str := strings.Join(strLabels, "\x00")

	return str
}
