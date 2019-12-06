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
	targetTypeAll    = 0
	targetTypeNotAll = 1
	targetTypeFocus  = 2
)

type CassandraIndex struct {
	session       *gocql.Session
	labelsTable   string
	postingsTable string
	uuidsTable    string

	pairs map[types.MetricUUID][]types.MetricLabel
	mutex sync.Mutex
}

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

func (c *CassandraIndex) Labels(uuid types.MetricUUID) ([]types.MetricLabel, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for uuidIt, labels := range c.pairs {
		if uuidIt == uuid {
			return labels, nil
		}
	}

	selectLabelsQuery := c.selectLabelsQuery(uuid.String())

	var m map[string]string

	if err := selectLabelsQuery.Scan(&m); (err != nil) && (err != gocql.ErrNotFound) {
		return nil, err
	}

	labels := types.LabelsFromMap(m)

	c.pairs[uuid] = labels

	return labels, nil
}

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

	str := types.StringFromLabels(labels)
	selectUUIDQuery := c.selectUUIDQuery(str)

	var cqlUUID gocql.UUID

	if err := selectUUIDQuery.Scan(&cqlUUID); (err != nil) && (err != gocql.ErrNotFound) {
		return types.MetricUUID{}, err
	} else if err != gocql.ErrNotFound {
		uuid := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

		c.pairs[uuid] = labels

		return uuid, nil
	}

	cqlUUID, err := gocql.RandomUUID()

	if err != nil {
		return types.MetricUUID{}, err
	}

	uuid := types.MetricUUID{UUID: gouuid.UUID(cqlUUID)}

	m := types.MapFromLabels(labels)

	updateLabelsQuery := c.updateLabelsQuery(uuid.String(), m)

	if err := updateLabelsQuery.Exec(); err != nil {
		return types.MetricUUID{}, err
	}

	updateUUIDQuery := c.updateUUIDQuery(str, uuid.String())

	if err := updateUUIDQuery.Exec(); err != nil {
		return types.MetricUUID{}, err
	}

	for _, label := range labels {
		updateUUIDsQuery := c.updateUUIDsQuery(label.Name, label.Value, uuid.String())

		if err := updateUUIDsQuery.Exec(); err != nil {
			return types.MetricUUID{}, err
		}
	}

	c.pairs[uuid] = labels

	return uuid, nil
}

func (c *CassandraIndex) UUIDs(matchers []types.MetricLabelMatcher, all bool) ([]types.MetricUUID, error) {
	if len(matchers) == 0 {
		return nil, nil
	}

	targetLabels, err := c.findTargetLabels(matchers)

	if err != nil {
		return nil, err
	}

	if len(targetLabels) == 0 {
		return nil, nil
	}

	uuidsWeight, err := c.calculateUUIDsWeight(targetLabels)

	if err != nil {
		return nil, err
	}

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

func (c *CassandraIndex) findTargetLabels(matchers []types.MetricLabelMatcher) (map[int][]types.MetricLabel, error) {
	targetLabels := make(map[int][]types.MetricLabel)

	for _, matcher := range matchers {
		if matcher.Value == "" {
			label := types.MetricLabel{
				Name:  matcher.Name,
				Value: "",
			}

			switch matcher.Type {
			case matcherTypeEq, matcherTypeRe:
				targetLabels[targetTypeAll] = append(targetLabels[targetTypeAll], label)
			case matcherTypeNeq, matcherTypeNre:
				targetLabels[targetTypeNotAll] = append(targetLabels[targetTypeNotAll], label)
			}
		} else {
			regex, err := regexp.Compile("^(?:" + matcher.Value + ")$")

			if err != nil {
				return nil, nil
			}

			selectValuesQuery := c.selectValuesQuery(matcher.Name)
			selectValuesIter := selectValuesQuery.Iter()

			var (
				values []string
				value  string
			)

			for selectValuesIter.Scan(&value) {
				values = append(values, value)
			}

			for _, value := range values {
				label := types.MetricLabel{
					Name:  matcher.Name,
					Value: value,
				}

				if ((matcher.Type == matcherTypeEq) && (matcher.Value == value)) ||
					((matcher.Type == matcherTypeNeq) && (matcher.Value != value)) ||
					((matcher.Type == matcherTypeRe) && regex.MatchString(value)) ||
					((matcher.Type == matcherTypeNre) && !regex.MatchString(value)) {
					targetLabels[targetTypeFocus] = append(targetLabels[targetTypeFocus], label)
				}
			}
		}
	}

	return targetLabels, nil
}

func (c *CassandraIndex) calculateUUIDsWeight(targetLabels map[int][]types.MetricLabel) (map[types.MetricUUID]int, error) {
	uuidsWeight := make(map[types.MetricUUID]int)

	for _, label := range targetLabels[targetTypeFocus] {
		selectUUIDsQuery := c.selectUUIDsFocusQuery(label.Name, label.Value)
		selectUUIDsIter := selectUUIDsQuery.Iter()

		var (
			scanUUIDs  []gocql.UUID
			labelUUIDs []types.MetricUUID
		)

		for selectUUIDsIter.Scan(&scanUUIDs) {
			for _, scanUUID := range scanUUIDs {
				labelUUID := types.MetricUUID{UUID: gouuid.UUID(scanUUID)}

				labelUUIDs = append(labelUUIDs, labelUUID)
			}
		}

		for _, uuid := range labelUUIDs {
			uuidsWeight[uuid] += 1
		}
	}

	for _, label := range targetLabels[targetTypeNotAll] {
		selectUUIDsQuery := c.selectUUIDsQuery(label.Name)
		selectUUIDsIter := selectUUIDsQuery.Iter()

		var (
			scanUUIDs  []gocql.UUID
			labelUUIDs []types.MetricUUID
		)

		for selectUUIDsIter.Scan(&scanUUIDs) {
			for _, scanUUID := range scanUUIDs {
				labelUUID := types.MetricUUID{UUID: gouuid.UUID(scanUUID)}

				labelUUIDs = append(labelUUIDs, labelUUID)
			}
		}

		for _, uuid := range labelUUIDs {
			uuidsWeight[uuid] += 1
		}
	}

	for _, label := range targetLabels[targetTypeAll] {
		selectUUIDsQuery := c.selectUUIDsQuery(label.Name)
		selectUUIDsIter := selectUUIDsQuery.Iter()

		var (
			scanUUIDs  []gocql.UUID
			labelUUIDs []types.MetricUUID
		)

		for selectUUIDsIter.Scan(&scanUUIDs) {
			for _, scanUUID := range scanUUIDs {
				labelUUID := types.MetricUUID{UUID: gouuid.UUID(scanUUID)}

				labelUUIDs = append(labelUUIDs, labelUUID)
			}
		}

		for uuid := range uuidsWeight {
			if !containsUUIDs(labelUUIDs, uuid) {
				uuidsWeight[uuid] += 1
			}
		}
	}

	return uuidsWeight, nil
}

func (c *CassandraIndex) selectLabelsQuery(uuid string) *gocql.Query {
	replacer := strings.NewReplacer("$UUIDS_TABLE", c.uuidsTable)
	query := c.session.Query(replacer.Replace(`
		SELECT labels FROM $UUIDS_TABLE
		WHERE uuid = ?
	`), uuid)

	return query
}

func (c *CassandraIndex) selectUUIDQuery(labels string) *gocql.Query {
	replacer := strings.NewReplacer("$LABELS_TABLE", c.labelsTable)
	query := c.session.Query(replacer.Replace(`
		SELECT uuid FROM $LABELS_TABLE
		WHERE labels = ?
	`), labels)

	return query
}

func (c *CassandraIndex) selectUUIDsQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable)

	var query *gocql.Query

	query = c.session.Query(replacer.Replace(`
		SELECT uuids FROM $POSTINGS_TABLE
		WHERE name = ?
	`), name)

	return query
}

func (c *CassandraIndex) selectUUIDsFocusQuery(name, value string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable)

	var query *gocql.Query

	query = c.session.Query(replacer.Replace(`
		SELECT uuids FROM $POSTINGS_TABLE
		WHERE name = ? AND value = ?
	`), name, value)

	return query
}

func (c *CassandraIndex) selectValuesQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable)
	query := c.session.Query(replacer.Replace(`
		SELECT value FROM $POSTINGS_TABLE
		WHERE name = ?
	`), name)

	return query
}

func (c *CassandraIndex) updateLabelsQuery(uuid string, labels map[string]string) *gocql.Query {
	replacer := strings.NewReplacer("$UUIDS_TABLE", c.uuidsTable)
	query := c.session.Query(replacer.Replace(`
		UPDATE $UUIDS_TABLE
		SET labels = ?
		WHERE uuid = ?
	`), labels, uuid)

	return query
}

func (c *CassandraIndex) updateUUIDQuery(labels string, uuid string) *gocql.Query {
	replacer := strings.NewReplacer("$LABELS_TABLE", c.labelsTable)
	query := c.session.Query(replacer.Replace(`
		UPDATE $LABELS_TABLE
		SET uuid = ?
		WHERE labels = ?
	`), uuid, labels)

	return query
}

func (c *CassandraIndex) updateUUIDsQuery(name, value, uuid string) *gocql.Query {
	replacer := strings.NewReplacer("$POSTINGS_TABLE", c.postingsTable, "$UUID", uuid)
	query := c.session.Query(replacer.Replace(`
		UPDATE $POSTINGS_TABLE
		SET uuids = uuids + {$UUID}
		WHERE name = ? AND value = ?
	`), name, value)

	return query
}

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
