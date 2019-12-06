package index

import (
	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"

	"regexp"
	"squirreldb/types"
	"strings"
	"sync"
)

const (
	labelsTableName   = "labels"
	postingsTableName = "postings"
	uuidsTableName    = "uuids"
)

const (
	matcherTypeEq  = 0
	matcherTypeNeq = 1
	matcherTypeRe  = 2
	matcherTypeNre = 3
)

const (
	targetTypeUndefined = 0
	targetTypeDefined   = 1
	targetTypeFocus     = 2
)

type CassandraIndex struct {
	session       *gocql.Session
	labelsTable   string
	postingsTable string
	uuidsTable    string

	pairs map[types.MetricUUID][]types.MetricLabel
	mutex sync.Mutex
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
		pairs:         make(map[types.MetricUUID][]types.MetricLabel),
	}

	return index, nil
}

// Labels returns a MetricLabel list corresponding to the specified MetricUUID
func (c *CassandraIndex) Labels(uuid types.MetricUUID) ([]types.MetricLabel, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for uuidIt, labels := range c.pairs {
		if uuidIt == uuid {
			return labels, nil
		}
	}

	selectLabelsQuery := c.uuidsTableSelectLabelsQuery(uuid.String())

	var m map[string]string

	if err := selectLabelsQuery.Scan(&m); (err != nil) && (err != gocql.ErrNotFound) {
		return nil, err
	}

	labels := types.LabelsFromMap(m)

	c.pairs[uuid] = labels

	return labels, nil
}

// UUID returns a MetricUUID corresponding to the specified MetricLabel list
func (c *CassandraIndex) UUID(labels []types.MetricLabel) (types.MetricUUID, error) {
	if len(labels) == 0 {
		return types.MetricUUID{}, nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	sortedLabels := types.SortLabels(labels)

	for uuid, label := range c.pairs {
		if types.EqualLabels(label, sortedLabels) {
			return uuid, nil
		}
	}

	str := types.StringFromLabels(sortedLabels)
	selectUUIDQuery := c.labelsTableSelectUUIDQuery(str)

	var cqlUUID gocql.UUID

	if err := selectUUIDQuery.Scan(&cqlUUID); (err != nil) && (err != gocql.ErrNotFound) {
		return types.MetricUUID{}, err
	} else if err != gocql.ErrNotFound {
		uuid := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

		c.pairs[uuid] = sortedLabels

		return uuid, nil
	}

	cqlUUID, err := gocql.RandomUUID()

	if err != nil {
		return types.MetricUUID{}, err
	}

	uuid := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}
	m := types.MapFromLabels(sortedLabels)

	updateLabelsQuery := c.uuidsTableUpdateLabelsQuery(uuid.String(), m)

	if err := updateLabelsQuery.Exec(); err != nil {
		return types.MetricUUID{}, err
	}

	updateUUIDQuery := c.labelsTableUpdateUUIDQuery(str, uuid.String())

	if err := updateUUIDQuery.Exec(); err != nil {
		return types.MetricUUID{}, err
	}

	for _, label := range sortedLabels {
		updateUUIDsQuery := c.postingsTableUpdateUUIDsQuery(label.Name, label.Value, uuid.String())

		if err := updateUUIDsQuery.Exec(); err != nil {
			return types.MetricUUID{}, err
		}
	}

	c.pairs[uuid] = sortedLabels

	return uuid, nil
}

// UUIDs returns a MetricUUIDs list corresponding to the specified MetricLabelMatcher list
func (c *CassandraIndex) UUIDs(matchers []types.MetricLabelMatcher, all bool) ([]types.MetricUUID, error) {
	if len(matchers) == 0 {
		return nil, nil
	}

	targetLabels := c.targetLabels(matchers)

	if len(targetLabels) == 0 {
		return nil, nil
	}

	uuidsWeight := c.uuidsWeight(targetLabels)

	if len(uuidsWeight) == 0 {
		return nil, nil
	}

	var uuids []types.MetricUUID

	for uuid, weight := range uuidsWeight {
		if weight == len(matchers) {
			uuids = append(uuids, uuid)
		}
	}

	return uuids, nil
}

// Returns labels by target type
func (c *CassandraIndex) targetLabels(matchers []types.MetricLabelMatcher) map[int][]types.MetricLabel {
	targetLabels := make(map[int][]types.MetricLabel)

	for _, matcher := range matchers {
		targetLabel := types.MetricLabel{
			Name: matcher.Name,
		}

		if matcher.Value == "" {
			targetLabel.Value = ""

			switch matcher.Type {
			case matcherTypeEq, matcherTypeRe:
				targetLabels[targetTypeUndefined] = append(targetLabels[targetTypeUndefined], targetLabel)
			case matcherTypeNeq, matcherTypeNre:
				targetLabels[targetTypeDefined] = append(targetLabels[targetTypeDefined], targetLabel)
			}
		} else {
			regex, _ := regexp.Compile("^(?:" + matcher.Value + ")$")

			selectValueQuery := c.postingsTableSelectValueQuery(matcher.Name)
			selectValueIter := selectValueQuery.Iter()

			var (
				values []string
				value  string
			)

			for selectValueIter.Scan(&value) {
				values = append(values, value)
			}

			for _, value := range values {
				targetLabel.Value = value

				if ((matcher.Type == matcherTypeEq) && (matcher.Value == value)) ||
					((matcher.Type == matcherTypeNeq) && (matcher.Value != value)) ||
					((matcher.Type == matcherTypeRe) && regex.MatchString(value)) ||
					((matcher.Type == matcherTypeNre) && !regex.MatchString(value)) {
					targetLabels[targetTypeFocus] = append(targetLabels[targetTypeFocus], targetLabel)
				}
			}
		}
	}

	return targetLabels
}

// Returns uuids with weight
func (c *CassandraIndex) uuidsWeight(targetLabels map[int][]types.MetricLabel) map[types.MetricUUID]int {
	uuidsWeight := make(map[types.MetricUUID]int)

	for _, label := range targetLabels[targetTypeFocus] {
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

		for _, uuid := range labelUUIDs {
			uuidsWeight[uuid]++
		}
	}

	for _, label := range targetLabels[targetTypeDefined] {
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

		for _, uuid := range labelUUIDs {
			uuidsWeight[uuid]++
		}
	}

	for _, label := range targetLabels[targetTypeUndefined] {
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

		for uuid := range uuidsWeight {
			if !containsUUIDs(labelUUIDs, uuid) {
				uuidsWeight[uuid]++
			}
		}
	}

	return uuidsWeight
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

// Returns labels table update uuid Query
func (c *CassandraIndex) labelsTableUpdateUUIDQuery(labels string, uuid string) *gocql.Query {
	replacer := strings.NewReplacer("$LABELS_TABLE", c.labelsTable)
	query := c.session.Query(replacer.Replace(`
		UPDATE $LABELS_TABLE
		SET uuid = ?
		WHERE labels = ?
	`), uuid, labels)

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

// Returns postings table update uuids Query
func (c *CassandraIndex) postingsTableUpdateUUIDsQuery(name, value, uuid string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable, "$UUID", uuid)
	query := c.session.Query(replacer.Replace(`
		UPDATE $POSTINGS_TABLE
		SET uuids = uuids + {$UUID}
		WHERE name = ? AND value = ?
	`), name, value)

	return query
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

// Returns uuids table update labels Query
func (c *CassandraIndex) uuidsTableUpdateLabelsQuery(uuid string, labels map[string]string) *gocql.Query {
	replacer := strings.NewReplacer("$UUIDS_TABLE", c.uuidsTable)
	query := c.session.Query(replacer.Replace(`
		UPDATE $UUIDS_TABLE
		SET labels = ?
		WHERE uuid = ?
	`), labels, uuid)

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
