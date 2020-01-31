package index

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"github.com/gocql/gocql"
	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"

	"context"
	"log"
	"os"
	"regexp"
	"squirreldb/debug"
	"strings"
	"sync"
	"time"
)

const expiratorInterval = 60

const cacheExpirationDelay = 300 * time.Second

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

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[index] ", log.LstdFlags)

type labelsData struct {
	labels         []*prompb.Label
	expirationTime time.Time
}

type uuidData struct {
	uuid                     gouuid.UUID
	cassandraEntryExpiration time.Time
	expirationTime           time.Time
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

type Index interface {
	LabelValues(name string) ([]string, error)
	Postings(name string, value string) ([]gouuid.UUID, error)
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

	sort.Slice(uuids, func(i, j int) bool {
		return uuidIsLess(uuids[i], uuids[j])
	})

	return uuids, nil
}

// LabelValues return values for given label name
func (c *CassandraIndex) LabelValues(name string) ([]string, error) {
	selectValueQuery := c.queryLabelValues(name)
	selectValueIter := selectValueQuery.Iter()

	var (
		values []string
		value  string
	)

	for selectValueIter.Scan(&value) {
		values = append(values, value)
	}

	err := selectValueIter.Close()

	return values, err
}

// Postings result uuids matching give Label name & value
// If value is the empty string, it match any values (but the label must be set)
// If name is the empty string, it match *ALL* UUID of the index
// The list of UUIDs is sorted
func (c *CassandraIndex) Postings(name string, value string) ([]gouuid.UUID, error) {
	if name == "" {
		return c.AllUUIDs()
	}

	if value == "" {
		selectUUIDsQuery := c.queryUUIDsFromLabelName(name)
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

		err := selectUUIDsIter.Close()

		sort.Slice(labelUUIDs, func(i, j int) bool {
			return uuidIsLess(labelUUIDs[i], labelUUIDs[j])
		})

		return labelUUIDs, err
	}

	selectUUIDsQuery := c.queryUUIDsFromLabel(name, value)
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

	sort.Slice(labelUUIDs, func(i, j int) bool {
		return uuidIsLess(labelUUIDs[i], labelUUIDs[j])
	})

	return labelUUIDs, nil
}

// LookupLabels returns a prompb.Label list corresponding to the specified UUID
func (c *CassandraIndex) LookupLabels(uuid gouuid.UUID) ([]*prompb.Label, error) {
	return c.lookupLabels(uuid, c.options.IncludeUUID)
}

func (c *CassandraIndex) lookupLabels(uuid gouuid.UUID, addUUID bool) ([]*prompb.Label, error) {
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

	labelsData.expirationTime = now.Add(cacheExpirationDelay)
	c.uuidsToLabels[uuid] = labelsData

	labels := make([]*prompb.Label, len(labelsData.labels))
	for i, v := range labelsData.labels {
		labels[i] = &prompb.Label{
			Name:  v.Name,
			Value: v.Value,
		}
	}

	if addUUID {
		label := &prompb.Label{
			Name:  uuidLabelName,
			Value: uuid.String(),
		}

		labels = append(labels, label)
	}

	lookupLabelsSeconds.Observe(time.Since(start).Seconds())

	return labels, nil
}

// LookupUUID returns a UUID corresponding to the specified prompb.Label list
// It also return the metric TTL
func (c *CassandraIndex) LookupUUID(labels []*prompb.Label) (gouuid.UUID, int64, error) { //nolint: gocognit
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
		sortedLabels       []*prompb.Label
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

		uuidStr, _ := getLabelsValue(labels, uuidLabelName)
		if c.options.IncludeUUID && uuidStr != "" {
			uuid, err := gouuid.FromString(uuidStr)
			if err != nil {
				return uuid, 0, err
			}

			sortedLabels, err = c.lookupLabels(uuid, false)

			if err != nil {
				return uuid, 0, err
			}

			sortedLabelsString = stringFromLabels(sortedLabels)
		} else {
			sortedLabels = sortLabels(labels)
			sortedLabelsString = stringFromLabels(sortedLabels)
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
			sortedLabels = sortLabels(labels)
			sortedLabelsString = stringFromLabels(sortedLabels)
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

	uuidData.expirationTime = now.Add(cacheExpirationDelay)
	c.labelsToUUID[labelsKey] = uuidData

	return uuidData.uuid, ttl, nil
}

// Search returns a list of UUID corresponding to the specified MetricLabelMatcher list
//
// It implement a revered index (as used in full-text search). The idea is that word
// are a couple LabelName=LabelValue. As in a revered index, it use this "word" to
// query a posting table which return the list of document ID (metric UUID here) that
// has this "word".
//
// In normal full-text search, the document(s) with the most match will be return, here
// it return the "document" (metric UUID) that exactly match all matchers
//
// Finally since Matcher could be other thing than LabelName=LabelValue (like not equal or using regular expression)
// there is a fist pass that convert them to something that works with the revered index: it query all values for the
// given label name then for each value, it the value match the filter convert to an simple equal matcher. Then this
// simple equal matcher could be used with the posting of the reversed index (e.g. name!="cpu" will be converted in
// something like name="memory" || name="disk" || name="...")
//
// There is still two additional special case: when label should be defined (regardless of the value, e.g. name!="") or
// when the label should NOT be defined (e.g. name="").
// In those case, it use the ability of our posting table to query for metric UUID that has a LabelName regardless of the values.
// * For label must be defined, it increament the number of Matcher satified if the metric has the label. In the principe it's the
//   same as if it expanded it to all possible values (e.g. with name!="" it avoid expanding to name="memory" || name="disk" and directly
//   ask for name=*)
// * For label must NOT be defined, it query for all metric UUIDs that has this label, then increament the number of Matcher satified if
//   currently found metrics are not in the list of metrics having this label.
//   Note: this means that it must already have found some metrics (and that this filter is applied at the end) but PromQL forbid to only
//   have label-not-defined matcher, so some other matcher must exists.
func (c *CassandraIndex) Search(matchers []*prompb.LabelMatcher) ([]gouuid.UUID, error) {
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
		uuidStr, found = getMatchersValue(matchers, uuidLabelName)

		if found {
			uuid, err := gouuid.FromString(uuidStr)

			if err != nil {
				return nil, nil
			}

			uuids = append(uuids, uuid)
		}
	}

	if !found {
		var err error
		uuids, err = postingsForMatchers(c, matchers)

		if err != nil {
			return nil, err
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
		if uuidData.expirationTime.Before(now) {
			delete(c.labelsToUUID, labelsString)
		}
	}

	for uuid, labelsData := range c.uuidsToLabels {
		if labelsData.expirationTime.Before(now) {
			delete(c.uuidsToLabels, uuid)
		}
	}
}

func matchValues(matcher *prompb.LabelMatcher, re *regexp.Regexp, value string) bool {
	var match bool

	if re != nil {
		match = re.MatchString(value)
	} else {
		match = (value == matcher.Value)
	}

	if matcher.Type == prompb.LabelMatcher_NEQ || matcher.Type == prompb.LabelMatcher_NRE {
		return !match
	}

	return match
}

func inverseMatcher(m *prompb.LabelMatcher) *prompb.LabelMatcher {
	mInv := prompb.LabelMatcher{
		Name:  m.Name,
		Value: m.Value,
	}

	switch m.Type {
	case prompb.LabelMatcher_EQ:
		mInv.Type = prompb.LabelMatcher_NEQ
	case prompb.LabelMatcher_NEQ:
		mInv.Type = prompb.LabelMatcher_EQ
	case prompb.LabelMatcher_RE:
		mInv.Type = prompb.LabelMatcher_NRE
	case prompb.LabelMatcher_NRE:
		mInv.Type = prompb.LabelMatcher_RE
	}

	return &mInv
}

// postingsForMatchers return metric UUID matching given matcher.
// The logic is taken from Prometheus PostingsForMatchers (in querier.go)
func postingsForMatchers(index Index, matchers []*prompb.LabelMatcher) (uuids []gouuid.UUID, err error) { //nolint: gocognit
	re := make([]*regexp.Regexp, len(matchers))
	labelMustBeSet := make(map[string]bool, len(matchers))

	for i, m := range matchers {
		if m.Type == prompb.LabelMatcher_RE || m.Type == prompb.LabelMatcher_NRE {
			re[i], err = regexp.Compile("^(?:" + m.Value + ")$")
		}

		if !matchValues(m, re[i], "") {
			labelMustBeSet[m.Name] = true
		}
	}

	var its, notIts [][]gouuid.UUID

	for i, m := range matchers {
		if labelMustBeSet[m.Name] {
			matchesEmpty := matchValues(m, re[i], "")
			isNot := m.Type == prompb.LabelMatcher_NEQ || m.Type == prompb.LabelMatcher_NRE

			// nolint: gocritic
			if isNot && matchesEmpty { // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse := inverseMatcher(m)
				it, err := postingsForMatcher(index, inverse, re[i])

				if err != nil {
					return nil, err
				}

				notIts = append(notIts, it)
			} else if isNot && !matchesEmpty { // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse := inverseMatcher(m)

				it, err := inversePostingsForMatcher(index, inverse, re[i])
				if err != nil {
					return nil, err
				}
				its = append(its, it)
			} else { // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it, err := postingsForMatcher(index, m, re[i])

				if err != nil {
					return nil, err
				}

				its = append(its, it)
			}
		} else { // l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			it, err := inversePostingsForMatcher(index, m, re[i])

			if err != nil {
				return nil, err
			}

			notIts = append(notIts, it)
		}
	}

	// If there's nothing to subtract from, add in everything and remove the notIts later.
	if len(its) == 0 && len(notIts) != 0 && false {
		allPostings, err := index.Postings("", "")

		if err != nil {
			return nil, err
		}

		its = append(its, allPostings)
	}

	it := intersectResult(its...)

	it = substractResult(it, notIts...)

	return it, err
}

// postingsForMatcher return uuid that match one matcher.
// This method will not return postings for missing labels.
func postingsForMatcher(index Index, m *prompb.LabelMatcher, re *regexp.Regexp) ([]gouuid.UUID, error) {
	if m.Type == prompb.LabelMatcher_EQ {
		return index.Postings(m.Name, m.Value)
	}

	values, err := index.LabelValues(m.Name)

	if err != nil {
		return nil, err
	}

	var res []string

	for _, val := range values {
		if matchValues(m, re, val) {
			res = append(res, val)
		}
	}

	workSets := make([][]gouuid.UUID, len(res))
	for i, v := range res {
		workSets[i], err = index.Postings(m.Name, v)

		if err != nil {
			return nil, err
		}
	}

	return unionResult(workSets...), nil
}

func inversePostingsForMatcher(index Index, m *prompb.LabelMatcher, re *regexp.Regexp) ([]gouuid.UUID, error) {
	values, err := index.LabelValues(m.Name)

	if err != nil {
		return nil, err
	}

	var res []string

	for _, val := range values {
		if !matchValues(m, re, val) {
			res = append(res, val)
		}
	}

	workSets := make([][]gouuid.UUID, len(res))

	for i, v := range res {
		workSets[i], err = index.Postings(m.Name, v)

		if err != nil {
			return nil, err
		}
	}

	return unionResult(workSets...), nil
}

func uuidIsLess(x, y gouuid.UUID) bool {
	return bytes.Compare(x[:], y[:]) == -1
}

// intersectResults do the intersection multiple UUID list.
func intersectResult(lists ...[]gouuid.UUID) []gouuid.UUID {
	if len(lists) == 0 {
		return nil
	}

	if len(lists) == 1 {
		return lists[0]
	}

	currentIndex := make([]int, len(lists))
	results := make([]gouuid.UUID, 0, len(lists[0]))

mainLoop:
	for idx0 := 0; idx0 < len(lists[0]); idx0++ {
		candidate := lists[0][idx0]
		for idxList, l := range lists[1:] {
			idx := currentIndex[idxList]
			for idx < len(l) && uuidIsLess(l[idx], candidate) {
				idx++
			}
			currentIndex[idxList] = idx
			if idx == len(l) {
				break mainLoop
			}
			if l[idx] != candidate { // i.e. l[idx] > candidate
				if currentIndex[idxList] > 0 {
					currentIndex[idxList]--
				}
				continue mainLoop
			}
		}
		results = append(results, candidate)
	}

	return results
}

// intersectResults do the intersection multiple UUID list.
func unionResult(lists ...[]gouuid.UUID) []gouuid.UUID { // nolint: gocognit
	if len(lists) == 0 {
		return nil
	}

	if len(lists) == 1 {
		return lists[0]
	}

	currentIndex := make([]int, len(lists))
	results := make([]gouuid.UUID, 0, len(lists[0]))

mainLoop:
	for {
		for idxList, l := range lists {
			var countFinished int
			smallest := gouuid.UUID([16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

			for idxList2, l2 := range lists {
				if idxList2 == idxList {
					continue
				}

				idx := currentIndex[idxList2]

				if idx >= len(l2) {
					countFinished++
					continue
				}

				if uuidIsLess(l2[idx], smallest) {
					smallest = l2[idx]
				}
			}

			idx := currentIndex[idxList]

			for idx < len(l) && uuidIsLess(l[idx], smallest) {
				results = append(results, l[idx])
				idx++
			}

			if idx < len(l) && l[idx] == smallest {
				idx++
			}

			if idx >= len(l) {
				countFinished++
			}

			if countFinished == len(lists) {
				break mainLoop
			}

			currentIndex[idxList] = idx
		}
	}

	return results
}

// substractResult remove from main all UUID found in on lists
func substractResult(main []gouuid.UUID, lists ...[]gouuid.UUID) []gouuid.UUID {
	if len(lists) == 0 {
		return main
	}

	currentIndex := make([]int, len(lists))
	results := make([]gouuid.UUID, 0, len(main))

mainLoop:
	for idx0 := 0; idx0 < len(main); idx0++ {
		candidate := main[idx0]
		for idxList, l := range lists {
			idx := currentIndex[idxList]
			for idx < len(l) && uuidIsLess(l[idx], candidate) {
				idx++
			}
			currentIndex[idxList] = idx
			if idx == len(l) {
				continue
			}
			if l[idx] == candidate {
				continue mainLoop
			}
			if currentIndex[idxList] > 0 {
				currentIndex[idxList]--
			}
		}
		results = append(results, candidate)
	}

	return results
}

// Returns uuids table insert labels Query as string
func (c *CassandraIndex) batchAddEntry(batch *gocql.Batch, uuid gocql.UUID, labelsString string, labels []*prompb.Label, ttl int64) {
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

// keyFromLabels returns a string key generated from a prompb.Label list
func keyFromLabels(labels []*prompb.Label) string {
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

// popLabelsValue get and delete value via its name from a prompb.Label list
func popLabelsValue(labels *[]*prompb.Label, key string) (string, bool) {
	for i, label := range *labels {
		if label.Name == key {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			return label.Value, true
		}
	}

	return "", false
}

// getLabelsValue gets value via its name from a prompb.Label list
func getLabelsValue(labels []*prompb.Label, name string) (string, bool) {
	for _, label := range labels {
		if label.Name == name {
			return label.Value, true
		}
	}

	return "", false
}

// getMatchersValue gets value via its name from a prompb.LabelMatcher list
func getMatchersValue(matchers []*prompb.LabelMatcher, name string) (string, bool) {
	for _, matcher := range matchers {
		if matcher.Name == name {
			return matcher.Value, true
		}
	}

	return "", false
}

// sortLabels returns the prompb.Label list sorted by name
func sortLabels(labels []*prompb.Label) []*prompb.Label {
	if len(labels) == 0 {
		return nil
	}

	sortedLabels := make([]*prompb.Label, len(labels))
	for i, v := range labels {
		sortedLabels[i] = &prompb.Label{
			Name:  v.Name,
			Value: v.Value,
		}
	}

	sort.Slice(sortedLabels, func(i, j int) bool {
		return sortedLabels[i].Name < sortedLabels[j].Name
	})

	return sortedLabels
}

// stringFromLabels returns a string generated from a prompb.Label list
func stringFromLabels(labels []*prompb.Label) string {
	if len(labels) == 0 {
		return ""
	}

	strLabels := make([]string, 0, len(labels))
	quoter := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

	for _, label := range labels {
		str := label.Name + "=\"" + quoter.Replace(label.Value) + "\""

		strLabels = append(strLabels, str)
	}

	str := strings.Join(strLabels, ",")

	return str
}

// Returns and delete time to live from a prompb.Label list
func timeToLiveFromLabels(labels *[]*prompb.Label) int64 {
	value, exists := popLabelsValue(labels, timeToLiveLabelName)

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
