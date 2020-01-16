package index

import (
	"fmt"
	"strconv"

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

const expiratorInterval = 60

const cacheExpirationDelay = 300

// Update TTL of index entries in Cassandra every update delay.
// The actual TTL used in Cassanra is the metric data TTL + update delay.
const (
	cassandraTTLUpdateDelay = time.Hour
	cassandraTTLSafeMargin  = 10 * time.Minute
)

const (
	timeToLiveLabelName = "__ttl__"
	uuidLabelName       = "__uuid__"
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
	uuid                     gouuid.UUID
	cassandraEntryExpiration time.Time
	expirationTimestamp      int64
}

type Options struct {
	DefaultTimeToLive int64
	IncludeUUID       bool
}

type CassandraIndex struct {
	session *gocql.Session
	options Options

	labelsToUUID  map[string]uuidData
	ltuMutex      sync.Mutex
	uuidsToLabels map[gouuid.UUID]labelsData
	utlMutex      sync.Mutex
}

// New creates a new CassandraIndex object
func New(session *gocql.Session, options Options) (*CassandraIndex, error) {
	index := &CassandraIndex{
		session:       session,
		options:       options,
		labelsToUUID:  make(map[string]uuidData),
		uuidsToLabels: make(map[gouuid.UUID]labelsData),
	}

	if err := index.createTables(); err != nil {
		return nil, err
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
	iter := c.queryAllUUIDs().Iter()

	var (
		uuids   []gouuid.UUID
		cqlUUID gocql.UUID
	)

	for iter.Scan(&cqlUUID) {
		uuids = append(uuids, gouuid.UUID(cqlUUID))
	}

	if err := iter.Close(); err != nil {
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
		query := c.queryLabelsFromUUID(gocql.UUID(uuid))

		if err := query.Scan(&labelsData.labels); (err != nil) && (err != gocql.ErrNotFound) {
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
// It also return the metric TTL
func (c *CassandraIndex) LookupUUID(labels []types.MetricLabel) (gouuid.UUID, int64, error) { //nolint: gocognit
	start := time.Now()
	now := time.Now()

	defer func() {
		lookupUUIDSeconds.Observe(time.Since(start).Seconds())
	}()

	if len(labels) == 0 {
		return gouuid.UUID{}, 0, nil
	}

	c.ltuMutex.Lock()
	defer c.ltuMutex.Unlock()

	var (
		sortedLabels       []types.MetricLabel
		sortedLabelsString string
	)

	ttl := timeToLiveFromLabels(&labels)
	if ttl == 0 {
		ttl = c.options.DefaultTimeToLive
	}

	labelsKey := keyFromLabels(labels)
	uuidData, found := c.labelsToUUID[labelsKey]

	if !found {
		lookupUUIDMisses.Inc()

		uuidStr, _ := types.GetLabelsValue(labels, uuidLabelName)
		if c.options.IncludeUUID && uuidStr != "" {
			uuid, err := gouuid.FromString(uuidStr)
			if err != nil {
				return uuid, 0, err
			}

			sortedLabels, err = c.LookupLabels(uuid)

			if err != nil {
				return uuid, 0, err
			}

			sortedLabelsString = types.StringFromLabels(sortedLabels)
		} else {
			sortedLabels = types.SortLabels(labels)
			sortedLabelsString = types.StringFromLabels(sortedLabels)
		}

		selectUUIDQuery := c.queryUUIDFromLabels(sortedLabelsString)

		var (
			cqlUUID      gocql.UUID
			cassandraTTL int64
		)

		if err := selectUUIDQuery.Scan(&cqlUUID, &cassandraTTL); err == nil {
			uuidData.uuid = gouuid.UUID(cqlUUID)
			uuidData.cassandraEntryExpiration = now.Add(time.Duration(cassandraTTL) * time.Second)
			found = true
		} else if err != gocql.ErrNotFound {
			return gouuid.UUID{}, 0, err
		}

		if !found && c.options.IncludeUUID && uuidStr != "" {
			return gouuid.UUID{}, 0, fmt.Errorf("label __uuid__ (value is %s) is provided but the metric does not exists", uuidStr)
		}
	}

	if !found {
		lookupUUIDNew.Inc()

		cqlUUID, err := gocql.RandomUUID()

		if err != nil {
			return gouuid.UUID{}, 0, err
		}

		uuidData.uuid = gouuid.UUID(cqlUUID)
	}

	wantedEntryExpiration := now.Add(time.Duration(ttl) * time.Second)
	needTTLUpdate := uuidData.cassandraEntryExpiration.Before(wantedEntryExpiration)

	if !found || needTTLUpdate {
		if needTTLUpdate && found {
			lookupUUIDRefresh.Inc()
		}

		if sortedLabelsString == "" {
			sortedLabels = types.SortLabels(labels)
			sortedLabelsString = types.StringFromLabels(sortedLabels)
		}
		// The Cassandra TTL is a little longer because we only update the TTL
		// every cassandraTTLUpdateDelay. By adding cassandraTTLUpdateDelay to the TTL
		// we won't drop the metric from the index even for value receive in
		// cassandraTTLUpdateDelay - 1 seconds
		cassandraTTL := ttl + int64(cassandraTTLUpdateDelay.Seconds())
		actualTTL := cassandraTTL + int64(cassandraTTLSafeMargin.Seconds())

		indexBatch := c.session.NewBatch(gocql.LoggedBatch)

		c.batchAddEntry(indexBatch, gocql.UUID(uuidData.uuid), sortedLabelsString, sortedLabels, actualTTL)

		if err := c.session.ExecuteBatch(indexBatch); err != nil {
			return gouuid.UUID{}, 0, err
		}

		uuidData.cassandraEntryExpiration = now.Add(time.Duration(cassandraTTL) * time.Second)
	}

	uuidData.expirationTimestamp = now.Unix() + cacheExpirationDelay
	c.labelsToUUID[labelsKey] = uuidData

	return uuidData.uuid, ttl, nil
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
			selectValueQuery := c.queryLabelValues(matcher.Name)
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
		selectUUIDsQuery := c.queryUUIDsFromLabel(label.Name, label.Value)
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
		selectUUIDsQuery := c.queryUUIDsFromLabelName(label.Name)
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
		selectUUIDsQuery := c.queryUUIDsFromLabelName(label.Name)
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

// Returns uuids table insert labels Query as string
func (c *CassandraIndex) batchAddEntry(batch *gocql.Batch, uuid gocql.UUID, labelsString string, labels []types.MetricLabel, ttl int64) {
	batch.Query(`
		INSERT INTO index_labels2uuid (labels, uuid)
		VALUES (?, ?)
		USING TTL ?
	`, labelsString, uuid, ttl)
	batch.Query(`
		INSERT INTO index_uuid2labels (uuid, labels)
		VALUES (?, ?)
		USING TTL ?
	`, uuid, labels, ttl)

	for _, label := range labels {
		batch.Query(`
			UPDATE index_postings
			USING TTL ?
			SET uuids = uuids + ?
			WHERE name = ? AND value = ?
		`, ttl, []gocql.UUID{uuid}, label.Name, label.Value)
	}
}

// queryUUIDForLabel query UUID for stringified labels list
func (c *CassandraIndex) queryUUIDFromLabels(labels string) *gocql.Query {
	query := c.session.Query(`
		SELECT uuid, ttl(uuid) FROM index_labels2uuid
		WHERE labels = ?
	`, labels)

	return query
}

// queryUUIDsForLabelName query UUIDs for given label name
func (c *CassandraIndex) queryUUIDsFromLabelName(name string) *gocql.Query {
	query := c.session.Query(`
		SELECT uuids FROM index_postings
		WHERE name = ?
	`, name)

	return query
}

// queryUUIDsForLabel query UUIDs for given label name + value
func (c *CassandraIndex) queryUUIDsFromLabel(name, value string) *gocql.Query {
	query := c.session.Query(`
		SELECT uuids FROM index_postings
		WHERE name = ? AND value = ?
	`, name, value)

	return query
}

// queryLabelValues query values for given label name
func (c *CassandraIndex) queryLabelValues(name string) *gocql.Query {
	query := c.session.Query(`
		SELECT value FROM index_postings
		WHERE name = ?
	`, name)

	return query
}

// queryLabelsFromUUID query labels of one uuid
func (c *CassandraIndex) queryLabelsFromUUID(uuid gocql.UUID) *gocql.Query {
	query := c.session.Query(`
		SELECT labels FROM index_uuid2labels
		WHERE uuid = ?
	`, uuid)

	return query
}

// queryAllUUIDs query all UUIDs of metrics
func (c *CassandraIndex) queryAllUUIDs() *gocql.Query {
	query := c.session.Query(`
		SELECT uuid FROM index_uuid2labels
		ALLOW FILTERING
	`)

	return query
}

// createTables create all Cassandra tables
func (c *CassandraIndex) createTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS index_labels2uuid (
			labels text,
			uuid uuid,
			PRIMARY KEY (labels)
		)`,
		`CREATE TABLE IF NOT EXISTS index_postings (
			name text,
			value text,
			uuids set<uuid>,
			PRIMARY KEY (name, value)
		)`,
		`CREATE TABLE IF NOT EXISTS index_uuid2labels (
			uuid uuid,
			labels frozen<list<tuple<text, text>>>,
			PRIMARY KEY (uuid)
		)`,
	}

	for _, query := range queries {
		if err := c.session.Query(query).Exec(); err != nil {
			return err
		}
	}

	return nil
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

// Returns and delete time to live from a MetricLabel list
func timeToLiveFromLabels(labels *[]types.MetricLabel) int64 {
	value, exists := types.PopLabelsValue(labels, timeToLiveLabelName)

	var timeToLive int64

	if exists {
		var err error
		timeToLive, err = strconv.ParseInt(value, 10, 64)

		if err != nil {
			logger.Printf("Warning: Can't get time to live from labels (%v), using default", err)
			return 0
		}
	}

	return timeToLive
}
