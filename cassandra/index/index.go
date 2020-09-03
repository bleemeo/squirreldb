package index

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"

	"github.com/gocql/gocql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/prometheus/pkg/labels"
	"golang.org/x/sync/errgroup"

	"context"
	"log"
	"os"
	"regexp"
	"squirreldb/debug"
	"squirreldb/types"
	"sync"
	"time"
)

const backgroundCheckInterval = time.Minute

const cacheExpirationDelay = 300 * time.Second

const concurrentInsert = 20

const (
	metricCreationLockTimeToLive  = 15 * time.Second
	metricExpiratorLockTimeToLive = 10 * time.Minute
)

// Update TTL of index entries in Cassandra every update delay.
// The actual TTL used in Cassanra is the metric data TTL + update delay.
// With long delay, the will be less updates on Cassandra, but entry will stay
// longer before being expired.
// This delay will have between 0 and cassandraTTLUpdateJitter added to avoid
// all update to happen at the same time.
const (
	cassandraTTLUpdateDelay  = 24 * time.Hour
	cassandraTTLUpdateJitter = time.Hour
)

const (
	newMetricLockName     = "index-new-metric"
	expireMetricLockName  = "index-ttl-metric"
	expireMetricStateName = "index-expired-until"
	expireBatchSize       = 10000
)

const (
	timeToLiveLabelName = "__ttl__"
	idLabelName         = "__metric_id__"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[index] ", log.LstdFlags)

type labelsData struct {
	labels         labels.Labels
	expirationTime time.Time
}

type idData struct {
	id                       types.MetricID
	unsortedLabels           labels.Labels
	cassandraEntryExpiration time.Time
	cacheExpirationTime      time.Time
}

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type Options struct {
	DefaultTimeToLive time.Duration
	IncludeID         bool
	LockFactory       lockFactory
	States            types.State
	SchemaLock        sync.Locker
}

type CassandraIndex struct {
	store   storeImpl
	options Options

	lookupIDMutex            sync.Mutex
	newMetricLock            types.TryLocker
	expirationUpdateRequests map[time.Time]expirationUpdateRequest
	labelsToID               map[uint64][]idData

	searchMutex sync.Mutex
	idsToLabels map[types.MetricID]labelsData
}

func (c *CassandraIndex) getIDData(key uint64, unsortedLabels labels.Labels) (idData, bool) {
	list := c.labelsToID[key]

	for _, r := range list {
		if labels.Equal(r.unsortedLabels, unsortedLabels) {
			return r, true
		}
	}

	return idData{}, false
}

func (c *CassandraIndex) setIDData(key uint64, value idData) {
	list := c.labelsToID[key]

	for i, r := range list {
		if labels.Equal(r.unsortedLabels, value.unsortedLabels) {
			list[i] = value
			c.labelsToID[key] = list

			return
		}
	}

	list = append(list, value)
	c.labelsToID[key] = list
}

type storeImpl interface {
	Init() error
	SelectLabels2ID(sortedLabelsString string) (types.MetricID, error)
	SelectID2Labels(id types.MetricID) (labels.Labels, error)
	SelectExpiration(day time.Time) ([]byte, error)
	SelectID2LabelsExpiration(id types.MetricID) (time.Time, error)
	SelectPostingByName(name string) bytesIter
	SelectPostingByNameValue(name string, value string) ([]byte, error)
	SelectValueForName(name string) ([]string, error)
	InsertPostings(name string, value string, bitset []byte) error
	InsertID2Labels(id types.MetricID, sortedLabels labels.Labels, expiration time.Time) error
	InsertLabels2ID(sortedLabelsString string, id types.MetricID) error
	InsertExpiration(day time.Time, bitset []byte) error
	UpdateID2LabelsExpiration(id types.MetricID, expiration time.Time) error
	DeleteLabels2ID(sortedLabelsString string) error
	DeleteID2Labels(id types.MetricID) error
	DeleteExpiration(day time.Time) error
	DeletePostings(name string, value string) error
}

const (
	allPostingLabelName  = "__all|metrics__" // we use the "|" since it's invalid for prometheus label name
	allPostingLabelValue = "__all|metrics__"
)

// New creates a new CassandraIndex object.
func New(session *gocql.Session, options Options) (*CassandraIndex, error) {
	return new(
		cassandraStore{
			session:    session,
			schemaLock: options.SchemaLock,
		},
		options,
	)
}

func new(store storeImpl, options Options) (*CassandraIndex, error) {
	index := &CassandraIndex{
		store:                    store,
		options:                  options,
		labelsToID:               make(map[uint64][]idData),
		idsToLabels:              make(map[types.MetricID]labelsData),
		expirationUpdateRequests: make(map[time.Time]expirationUpdateRequest),
		newMetricLock:            options.LockFactory.CreateLock(newMetricLockName, metricCreationLockTimeToLive),
	}

	if err := index.store.Init(); err != nil {
		return nil, err
	}

	return index, nil
}

// Run starts all Cassandra Index services.
func (c *CassandraIndex) Run(ctx context.Context) {
	ticker := time.NewTicker(backgroundCheckInterval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			c.RunOnce(ctx)
		case <-ctx.Done():
			debug.Print(2, logger, "Cassandra index service stopped")
			return
		}
	}
}

// RunOnce run the tasks scheduled by Run.
// Prefer using Run() than calling RunOnce multiple time. RunOnce is mostly here
// for squirreldb-cassandra-index-bench program.
func (c *CassandraIndex) RunOnce(ctx context.Context) {
	c.expire(time.Now())
	c.applyExpirationUpdateRequests()
	c.cassandraExpire(time.Now())
}

func (c *CassandraIndex) deleteIDsFromCache(deleteIDs []uint64) {
	if len(deleteIDs) > 0 {
		c.lookupIDMutex.Lock()
		c.searchMutex.Lock()

		// Since we don't force sorting labels on input, we don't known the key used
		// for c.labelsToID (it's likely to be keyFromLabels(sortedLabels))
		deleteIDsMap := make(map[types.MetricID]bool, len(deleteIDs))

		for _, id := range deleteIDs {
			deleteIDsMap[types.MetricID(id)] = true

			delete(c.idsToLabels, types.MetricID(id))
		}

		c.searchMutex.Unlock()

		for key, idsData := range c.labelsToID {
			for _, v := range idsData {
				if deleteIDsMap[v.id] {
					// This may delete too many entry, but:
					// 1) normally only 1 entry match the hash
					// 2) it's a cache, we don't loss data
					delete(c.labelsToID, key)
					break
				}
			}
		}
		c.lookupIDMutex.Unlock()
	}
}

// Verify perform some verification of the indexes health.
func (c *CassandraIndex) Verify(ctx context.Context, w io.Writer, doFix bool, acquireLock bool) error { // nolint: gocognit,gocyclo
	bulkDeleter := newBulkDeleter(c)

	if doFix && !acquireLock {
		return errors.New("doFix require acquire lock")
	}

	if acquireLock {
		c.newMetricLock.Lock()
		defer c.newMetricLock.Unlock()
	}

	allGoodIds := roaring.NewBTreeBitmap()

	allPosting, err := c.postings(allPostingLabelName, allPostingLabelValue)
	if err != nil {
		return err
	}

	count := 0
	countOk := 0
	it := allPosting.Iterator()

	labelNames := make(map[string]interface{})

	for ctx.Err() == nil {
		id, eof := it.Next()
		if eof {
			break
		}

		metricID := types.MetricID(id)

		count++

		_, _ = allGoodIds.AddN(id)

		lbls, err := c.store.SelectID2Labels(metricID)
		if err == gocql.ErrNotFound {
			fmt.Fprintf(w, "ID %10d does not exists in ID2Labels, partial write ?\n", metricID)

			if doFix {
				bulkDeleter.PrepareDelete(metricID, nil, false)
			}

			continue
		}

		if err != nil {
			return err
		}

		for _, l := range lbls {
			labelNames[l.Name] = nil
		}

		expiration, err := c.store.SelectID2LabelsExpiration(metricID)
		if err == gocql.ErrNotFound {
			return fmt.Errorf("ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify", metricID, lbls.String())
		}

		if err != nil {
			return err
		}

		metricID2, err := c.store.SelectLabels2ID(lbls.String())
		if err == gocql.ErrNotFound {
			fmt.Fprintf(w, "ID %10d (%v) not found in Labels2ID, partial write ?\n", metricID, lbls.String())

			if doFix {
				bulkDeleter.PrepareDelete(metricID, lbls, false)
			}

			continue
		}

		if err != nil {
			return err
		}

		if metricID != metricID2 {
			lbls2, err := c.store.SelectID2Labels(metricID2)
			if err != nil && err != gocql.ErrNotFound {
				return err
			}

			if err != nil {
				lbls2 = nil
			}

			expiration2, err := c.store.SelectID2LabelsExpiration(metricID2)
			if err != nil && err != gocql.ErrNotFound {
				return err
			}

			if err == gocql.ErrNotFound && lbls2 != nil {
				return fmt.Errorf("ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify", metricID2, lbls2.String())
			}

			switch {
			case lbls2 == nil:
				fmt.Fprintf(
					w,
					"ID %10d (%v) conflict with ID %d (which is a partial write! THIS SHOULD NOT HAPPEN.)\n",
					metricID,
					lbls.String(),
					metricID2,
				)

				if doFix {
					// well, the only solution is to delete *both* ID.
					bulkDeleter.PrepareDelete(metricID2, lbls, false)
					bulkDeleter.PrepareDelete(metricID, lbls, false)
				}
			case !allPosting.Contains(uint64(metricID2)):
				fmt.Fprintf(
					w,
					"ID %10d (%v) conflict with ID %d (which isn't listed in all posting! THIS SHOULD NOT HAPPEN.)\n",
					metricID,
					lbls.String(),
					metricID2,
				)

				if doFix {
					// well, the only solution is to delete *both* ID.
					bulkDeleter.PrepareDelete(metricID2, lbls2, false)
					bulkDeleter.PrepareDelete(metricID, lbls, false)
				}
			default:
				fmt.Fprintf(
					w,
					"ID %10d (%v) conflict with ID %d (%v). first expire at %v, second at %v\n",
					metricID,
					lbls.String(),
					metricID2,
					lbls2.String(),
					expiration,
					expiration2,
				)
				// Assume that metric2 is better. It has id2labels, labels2id and in all postings
				if doFix {
					bulkDeleter.PrepareDelete(metricID, lbls, true)
				}
			}

			continue
		}

		if time.Now().Add(24 * time.Hour).After(expiration) {
			fmt.Fprintf(w, "ID %10d (%v) should have expired on %v\n", metricID, lbls.String(), expiration)

			if doFix {
				bulkDeleter.PrepareDelete(metricID, lbls, false)
			}

			continue
		}

		countOk++
	}

	fmt.Fprintf(w, "Index contains %d metrics and %d ok. There is %d label names\n", count, countOk, len(labelNames))

	if doFix {
		fmt.Fprintf(w, "Applying fix...")

		if err := bulkDeleter.Delete(); err != nil {
			return err
		}
	}

	for name := range labelNames {
		if ctx.Err() != nil {
			break
		}

		fmt.Fprintf(w, "check postings for %s\n", name)

		iter := c.store.SelectPostingByName(name)
		for iter.HasNext() {
			tmp := roaring.NewBTreeBitmap()

			err := tmp.UnmarshalBinary(iter.Next())
			if err != nil {
				return err
			}

			tmp = tmp.Difference(allGoodIds)
			it := tmp.Iterator()

			for ctx.Err() == nil {
				id, eof := it.Next()
				if eof {
					break
				}

				fmt.Fprintf(
					w,
					"Posting for name %s has ID %d which is not in all posting!\n",
					name,
					id,
				)
			}
		}
	}

	return ctx.Err()
}

// AllIDs returns all ids stored in the index.
func (c *CassandraIndex) AllIDs() ([]types.MetricID, error) {
	bitmap, err := c.postings(allPostingLabelName, allPostingLabelName)

	if err != nil {
		return nil, err
	}

	return bitsetToIDs(bitmap), nil
}

// labelValues return values for given label name.
func (c *CassandraIndex) labelValues(name string) ([]string, error) {
	return c.store.SelectValueForName(name)
}

// postings return ids matching give Label name & value
// If value is the empty string, it match any values (but the label must be set).
func (c *CassandraIndex) postings(name string, value string) (*roaring.Bitmap, error) {
	if name == allPostingLabelName {
		value = allPostingLabelValue
	}

	result := roaring.NewBTreeBitmap()

	if name != allPostingLabelName && value == "" {
		iter := c.store.SelectPostingByName(name)

		for iter.HasNext() {
			tmp := roaring.NewBTreeBitmap()

			err := tmp.UnmarshalBinary(iter.Next())
			if err != nil {
				return nil, err
			}

			result.UnionInPlace(tmp)
		}

		err := iter.Err()

		if err == gocql.ErrNotFound {
			err = nil
		}

		return result, err
	}

	buffer, err := c.store.SelectPostingByNameValue(name, value)

	if err == gocql.ErrNotFound {
		err = nil
	} else if err == nil {
		err = result.UnmarshalBinary(buffer)
	}

	return result, err
}

// LookupLabels returns a Label list corresponding to the specified ID.
func (c *CassandraIndex) LookupLabels(id types.MetricID) (labels.Labels, error) {
	return c.lookupLabels(id, c.options.IncludeID, time.Now())
}

func (c *CassandraIndex) lookupLabels(id types.MetricID, addID bool, now time.Time) (labels.Labels, error) {
	start := time.Now()

	c.searchMutex.Lock()

	labelsData, found := c.idsToLabels[id]

	c.searchMutex.Unlock()

	if !found {
		var err error

		labelsData.labels, err = c.store.SelectID2Labels(id)

		if err != nil && err != gocql.ErrNotFound {
			lookupLabelsSeconds.Observe(time.Since(start).Seconds())

			return nil, err
		}
	}

	labelsData.expirationTime = now.Add(cacheExpirationDelay)

	c.searchMutex.Lock()

	c.idsToLabels[id] = labelsData

	c.searchMutex.Unlock()

	labelList := make(labels.Labels, len(labelsData.labels))
	for i, v := range labelsData.labels {
		labelList[i] = labels.Label{
			Name:  v.Name,
			Value: v.Value,
		}
	}

	if addID {
		label := labels.Label{
			Name:  idLabelName,
			Value: strconv.FormatInt(int64(id), 10),
		}

		labelList = append(labelList, label)
	}

	lookupLabelsSeconds.Observe(time.Since(start).Seconds())

	return labelList, nil
}

// LookupIDs returns a IDs corresponding to the specified labels.Label lists
// It also return the metric TTLs
// The result list will be the same length as input lists and using the same order.
func (c *CassandraIndex) LookupIDs(labelsList []labels.Labels) ([]types.MetricID, []int64, error) {
	return c.lookupIDs(labelsList, time.Now())
}

func (c *CassandraIndex) lookupIDs(labelsList []labels.Labels, now time.Time) ([]types.MetricID, []int64, error) { // nolint: gocognit
	start := time.Now()

	defer func() {
		LookupIDRequestSeconds.Observe(time.Since(start).Seconds())
	}()

	LookupIDs.Add(float64(len(labelsList)))

	idsData := make([]idData, len(labelsList))
	founds := make([]bool, len(labelsList))
	foundCount := 0
	ttls := make([]int64, len(labelsList))
	labelsKeys := make([]uint64, len(labelsList))

	for i, labels := range labelsList {
		if len(labels) == 0 {
			return nil, nil, errors.New("empty labels set")
		}

		ttls[i] = timeToLiveFromLabels(&labelsList[i])
		if ttls[i] == 0 {
			ttls[i] = int64(c.options.DefaultTimeToLive.Seconds())
		}
	}

	c.lookupIDMutex.Lock()
	for i, labels := range labelsList {
		labelsKeys[i] = labels.Hash()
		idsData[i], founds[i] = c.getIDData(labelsKeys[i], labels)

		if founds[i] {
			foundCount++
		}
	}
	c.lookupIDMutex.Unlock()

	LookupIDMisses.Add(float64(len(labelsList) - foundCount))

	if c.options.IncludeID {
		if err := c.lookupIDsFromLabels(labelsList, idsData, founds); err != nil {
			return nil, nil, fmt.Errorf("lookup with %s failed: %v", idLabelName, err)
		}
	}

	var (
		requests []createMetricRequest
		err      error
	)

	duplicatedMetric := make(map[string][]int)

	for i, labels := range labelsList {
		if founds[i] {
			continue
		}

		sortedLabels := sortLabels(labels)
		sortedLabelsString := sortedLabels.String()

		duplicatedMetric[sortedLabelsString] = append(duplicatedMetric[sortedLabelsString], i)

		if len(duplicatedMetric[sortedLabelsString]) == 1 {
			requests, err = c.searchMetric(requests, sortedLabelsString, sortedLabels, &idsData[i], &founds[i], ttls[i], now)
			if err != nil {
				return nil, nil, fmt.Errorf("searching metric failed: %v", err)
			}
		} else {
			iFirst := duplicatedMetric[sortedLabelsString][0]
			if founds[iFirst] {
				idsData[i] = idsData[iFirst]
				founds[i] = true
			}
		}
	}

	if len(requests) > 0 {
		c.newMetricLock.Lock()

		newIDs, err := c.createMetrics(requests)

		c.newMetricLock.Unlock()

		if err != nil {
			return nil, nil, err
		}

		for j, id := range newIDs {
			req := requests[j]

			for _, i := range duplicatedMetric[req.sortedLabelsString] {
				wantedEntryExpiration := now.Add(time.Duration(ttls[i]) * time.Second)
				cassandraExpiration := wantedEntryExpiration.Add(cassandraTTLUpdateDelay)
				cassandraExpiration = cassandraExpiration.Add(time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second)
				idsData[i].id = id
				idsData[i].cassandraEntryExpiration = cassandraExpiration
				founds[i] = true
			}
		}
	}

	c.lookupIDMutex.Lock()
	defer c.lookupIDMutex.Unlock()

	ids := make([]types.MetricID, len(labelsList))

	for i, idData := range idsData {
		if !founds[i] {
			return nil, nil, errors.New("unexpected error in lookup ID: the metric is not found even after creation")
		}

		ids[i] = idData.id

		wantedEntryExpiration := now.Add(time.Duration(ttls[i]) * time.Second)
		cassandraExpiration := wantedEntryExpiration.Add(cassandraTTLUpdateDelay)
		cassandraExpiration = cassandraExpiration.Add(time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second)
		needTTLUpdate := idData.cassandraEntryExpiration.Before(wantedEntryExpiration)

		if needTTLUpdate {
			if err := c.refreshExpiration(idData.id, idData.cassandraEntryExpiration, cassandraExpiration); err != nil {
				return nil, nil, err
			}

			idData.cassandraEntryExpiration = cassandraExpiration
		}

		idData.cacheExpirationTime = now.Add(cacheExpirationDelay)
		idData.unsortedLabels = labelsList[i]
		c.setIDData(labelsKeys[i], idData)
	}

	return ids, ttls, nil
}

// search metric ID by labels in Cassadran. Update and return requests to create metric if not found.
func (c *CassandraIndex) searchMetric(requests []createMetricRequest, sortedLabelsString string, sortedLabels labels.Labels, idData *idData, found *bool, ttl int64, now time.Time) ([]createMetricRequest, error) {
	if id, err := c.store.SelectLabels2ID(sortedLabelsString); err == nil {
		idData.id = id
		idData.cassandraEntryExpiration, err = c.store.SelectID2LabelsExpiration(idData.id)

		if err != nil && err != gocql.ErrNotFound {
			return nil, err
		}

		if err != gocql.ErrNotFound {
			*found = true
		}
	} else if err != gocql.ErrNotFound {
		return nil, err
	}

	if !*found {
		LookupIDNew.Inc()

		wantedEntryExpiration := now.Add(time.Duration(ttl) * time.Second)
		cassandraExpiration := wantedEntryExpiration.Add(cassandraTTLUpdateDelay)
		cassandraExpiration = cassandraExpiration.Add(time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second)

		requests = append(requests, createMetricRequest{
			sortedLabelsString:  sortedLabelsString,
			sortedLabels:        sortedLabels,
			cassandraExpiration: cassandraExpiration,
		})
	}

	return requests, nil
}

func (c *CassandraIndex) refreshExpiration(id types.MetricID, oldExpiration time.Time, newExpiration time.Time) error {
	LookupIDRefresh.Inc()

	err := c.store.UpdateID2LabelsExpiration(id, newExpiration)
	if err != nil {
		return err
	}

	// Move the metrics ID for the new expiration list, but do it
	// in a background task to send multiple update at the same time.
	previousDay := oldExpiration.Truncate(24 * time.Hour)
	newDay := newExpiration.Truncate(24 * time.Hour)

	if previousDay.Equal(newDay) {
		return nil
	}

	req := c.expirationUpdateRequests[previousDay]

	req.RemoveIDs = append(req.RemoveIDs, uint64(id))
	c.expirationUpdateRequests[previousDay] = req

	req = c.expirationUpdateRequests[newDay]

	req.AddIDs = append(req.AddIDs, uint64(id))
	c.expirationUpdateRequests[newDay] = req

	return nil
}

// lookupIDsFromLabels will idData for metrics which as the idLabelName label.
func (c *CassandraIndex) lookupIDsFromLabels(labelsList []labels.Labels, idsData []idData, founds []bool) error {
	for i, labels := range labelsList {
		if founds[i] {
			continue
		}

		idStr := labels.Get(idLabelName)
		if idStr != "" {
			id, err := strconv.ParseInt(idStr, 10, 0)
			if err != nil {
				return err
			}

			founds[i] = true
			idsData[i].id = types.MetricID(id)
			idsData[i].cassandraEntryExpiration, err = c.store.SelectID2LabelsExpiration(idsData[i].id)

			if err == gocql.ErrNotFound {
				return fmt.Errorf("label %s (value is %s) is provided but the metric does not exists", idLabelName, idStr)
			}

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Search a free ID using dichotomy.
func freeFreeID(bitmap *roaring.Bitmap) uint64 {
	card := bitmap.Count()
	if card == 0 {
		return 1
	}

	max := bitmap.Max()

	if max == card {
		return max + 1
	}

	if bitmap.Contains(0) && max+1 == card {
		return max + 1
	}

	lowIdx := uint64(1)
	highIdx := max

	for highIdx-lowIdx > 32768 {
		pivot := lowIdx + (highIdx-lowIdx)/2

		countIfFull := pivot - lowIdx
		if bitmap.CountRange(lowIdx, pivot) >= countIfFull {
			lowIdx = pivot + 1
		} else {
			highIdx = pivot
		}
	}

	freemap := roaring.NewBTreeBitmap()
	freemap = freemap.Flip(lowIdx, highIdx+1)
	freemap = freemap.Xor(bitmap)

	results := freemap.SliceRange(lowIdx, highIdx+1)
	if len(results) == 0 {
		return 0
	}

	return results[0]
}

type createMetricRequest struct {
	sortedLabelsString  string
	sortedLabels        labels.Labels
	cassandraExpiration time.Time
	newID               uint64
}

// createMetrics creates a new metric IDs associated with provided request
// Some care should be taken to avoid assigned the same ID from two SquirrelDB instance, so:
//
// * A lock is taken (lock is stored in Cassandra). On single SquirrelDB this lock could be a normal in-memory lock (but currently its a Cassandra lock)
// * To avoid race-condition, redo a check that metrics is not yet registered now that lock is acquired
// * Read the all-metric postings. From there we find a free ID
// * Update Cassandra tables to store this new metrics. The insertion is done in the following order:
//   * First an entry is added to the expiration table. This ensure that in case of crash in this process, the ID will eventually be freed.
//   * Then it update:
//     * the all-metric postings. This effectively reseve the ID.
//     * the id2labels tables (it give informations needed to cleanup other postings)
//   * the we update postings for each label pairs
//   * finally insert in labels2id, which is done as last because it's this table that determine that a metrics didn't exists
// * Release the lock
//
// If the above process crash and partially write some value, it still in a good state because:
// * For the insertion, it's the last entry (in labels2id) that matter. The the creation will be retried when next point is received
// * For reading, even if the metric ID may match search (as soon as it's in some posting, it may happen), since no data points could be wrote
//   and empty result are stipped, they won't be in results
// * For writing using __metric_id__ labels, it may indeed success if the partial write reacher id2labels... BUT to have the metric ID, client must
//   first do a succesfull read to get the ID. So this shouldn't happen.
//
// The expiration tables is used to known which metrics are likely to expire on a give date. They are grouped by day (that is, the tables contains on
// row per day, each row being the day and the list of metric IDs that may expire on this day).
// A background process will process each past day from this tables and for each metrics:
// * Check if the metrics is actually expired. It may not be the case, if the metrics continued to get points. It does this check using
//   a field of the table id2labels which is refreshed.
// * If expired, delete entry for this metric from the index (the opposite of creation)
// * Of not expired, add the metric IDs to the new expiration day in the table.
// * Once finished, delete the processed day.
func (c *CassandraIndex) createMetrics(requests []createMetricRequest) ([]types.MetricID, error) { // nolint: gocognit
	start := time.Now()
	results := make([]types.MetricID, len(requests))
	expirationUpdateRequests := make(map[time.Time]expirationUpdateRequest)
	postingUpdates := make([]postingUpdateRequest, 0)
	labelToPostingUpdates := make(map[string]map[string]int)

	defer func() {
		CreateMetricSeconds.Observe(time.Since(start).Seconds())
	}()

	allPosting, err := c.postings(allPostingLabelName, allPostingLabelValue)
	if err != nil {
		return nil, err
	}

	for i, req := range requests {
		if req.newID == 0 {
			// Be sure no-one registered the metric before we took the lock.
			if id, err := c.store.SelectLabels2ID(req.sortedLabelsString); err == nil {
				requests[i].newID = uint64(id)

				LookupIDConcurrentNew.Inc()
			} else if err != gocql.ErrNotFound {
				return nil, err
			}

			if requests[i].newID == 0 {
				requests[i].newID = freeFreeID(allPosting)
			}
		}

		if requests[i].newID == 0 {
			return nil, errors.New("too many metrics registered, unable to find a free ID")
		}

		_, err = allPosting.Add(requests[i].newID)
		if err != nil {
			return nil, err
		}

		results[i] = types.MetricID(requests[i].newID)

		day := req.cassandraExpiration.Truncate(24 * time.Hour)
		expReq := expirationUpdateRequests[day]

		expReq.AddIDs = append(expReq.AddIDs, requests[i].newID)

		expirationUpdateRequests[day] = expReq

		for _, label := range req.sortedLabels {
			m, ok := labelToPostingUpdates[label.Name]
			if !ok {
				m = make(map[string]int)
				labelToPostingUpdates[label.Name] = m
			}

			idx, ok := m[label.Value]
			if !ok {
				idx = len(postingUpdates)
				postingUpdates = append(postingUpdates, postingUpdateRequest{
					Label: label,
				})
				m[label.Value] = idx
			}

			postingUpdates[idx].AddIDs = append(postingUpdates[idx].AddIDs, requests[i].newID)
		}
	}

	for day, req := range expirationUpdateRequests {
		req.Day = day

		err = c.expirationUpdate(req)
		if err != nil {
			return nil, err
		}
	}

	var buffer bytes.Buffer

	_, err = allPosting.WriteTo(&buffer)

	if err != nil {
		return nil, err
	}

	err = c.store.InsertPostings(allPostingLabelName, allPostingLabelValue, buffer.Bytes())
	if err != nil {
		return nil, err
	}

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range requests {
			req := req
			task := func() error {
				return c.store.InsertID2Labels(types.MetricID(req.newID), req.sortedLabels, req.cassandraExpiration)
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range postingUpdates {
			req := req
			task := func() error {
				return c.postingUpdate(req)
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range requests {
			req := req
			task := func() error {
				return c.store.InsertLabels2ID(req.sortedLabelsString, types.MetricID(req.newID))
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// Search returns a list of IDs corresponding to the specified MetricLabelMatcher list
//
// It implement a revered index (as used in full-text search). The idea is that word
// are a couple LabelName=LabelValue. As in a revered index, it use this "word" to
// query a posting table which return the list of document ID (metric ID here) that
// has this "word".
//
// In normal full-text search, the document(s) with the most match will be return, here
// it return the "document" (metric ID) that exactly match all matchers
//
// Finally since Matcher could be other thing than LabelName=LabelValue (like not equal or using regular expression)
// there is a fist pass that convert them to something that works with the revered index: it query all values for the
// given label name then for each value, it the value match the filter convert to an simple equal matcher. Then this
// simple equal matcher could be used with the posting of the reversed index (e.g. name!="cpu" will be converted in
// something like name="memory" || name="disk" || name="...")
//
// There is still two additional special case: when label should be defined (regardless of the value, e.g. name!="") or
// when the label should NOT be defined (e.g. name="").
// In those case, it use the ability of our posting table to query for metric ID that has a LabelName regardless of the values.
// * For label must be defined, it increament the number of Matcher satified if the metric has the label. In the principe it's the
//   same as if it expanded it to all possible values (e.g. with name!="" it avoid expanding to name="memory" || name="disk" and directly
//   ask for name=*)
// * For label must NOT be defined, it query for all metric IDs that has this label, then increament the number of Matcher satified if
//   currently found metrics are not in the list of metrics having this label.
//   Note: this means that it must already have found some metrics (and that this filter is applied at the end) but PromQL forbid to only
//   have label-not-defined matcher, so some other matcher must exists.
func (c *CassandraIndex) Search(matchers []*labels.Matcher) ([]types.MetricID, error) {
	start := time.Now()

	defer func() {
		searchMetricsSeconds.Observe(time.Since(start).Seconds())
	}()

	if len(matchers) == 0 {
		return nil, nil
	}

	var (
		ids   []types.MetricID
		found bool
	)

	if c.options.IncludeID {
		var idStr string
		idStr, found = getMatchersValue(matchers, idLabelName)

		if found {
			id, err := strconv.ParseInt(idStr, 10, 0)

			if err != nil {
				return nil, nil
			}

			ids = append(ids, types.MetricID(id))
		}
	}

	if !found {
		var err error
		ids, err = c.postingsForMatchers(matchers)

		if err != nil {
			return nil, err
		}
	}

	searchMetricsTotal.Add(float64(len(ids)))

	return ids, nil
}

// Deletes all expired cache entries.
func (c *CassandraIndex) expire(now time.Time) {
	c.lookupIDMutex.Lock()
	c.searchMutex.Lock()
	defer c.lookupIDMutex.Unlock()
	defer c.searchMutex.Unlock()

	for key, idsData := range c.labelsToID {
		for _, idData := range idsData {
			if idData.cacheExpirationTime.Before(now) {
				// This may delete too many entry, but:
				// 1) normally only 1 entry match the hash
				// 2) it's a cache, we don't loss data
				delete(c.labelsToID, key)
				break
			}
		}
	}

	for id, labelsData := range c.idsToLabels {
		if labelsData.expirationTime.Before(now) {
			delete(c.idsToLabels, id)
		}
	}
}

func (c *CassandraIndex) applyExpirationUpdateRequests() {
	c.lookupIDMutex.Lock()

	start := time.Now()

	defer func() {
		expirationMoveSeconds.Observe(time.Since(start).Seconds())
	}()

	expireUpdates := make([]expirationUpdateRequest, 0, len(c.expirationUpdateRequests))

	for day, v := range c.expirationUpdateRequests {
		v.Day = day
		expireUpdates = append(expireUpdates, v)
	}

	c.expirationUpdateRequests = make(map[time.Time]expirationUpdateRequest)

	c.lookupIDMutex.Unlock()

	c.newMetricLock.Lock()

	err := c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range expireUpdates {
			req := req
			task := func() error {
				return c.expirationUpdate(req)
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	c.newMetricLock.Unlock()

	if err != nil {
		logger.Printf("Warning: update of expiration date failed: %v", err)

		c.lookupIDMutex.Lock()

		for _, v := range expireUpdates {
			v2 := c.expirationUpdateRequests[v.Day]
			v2.AddIDs = append(v2.AddIDs, v.AddIDs...)
			v2.RemoveIDs = append(v2.RemoveIDs, v.RemoveIDs...)
			c.expirationUpdateRequests[v.Day] = v2
		}

		c.lookupIDMutex.Unlock()
	}
}

// InternalForceExpirationTimestamp will force the state for the most recently processed day of metrics expiration
// This should only be used in test & benchmark.
func (c *CassandraIndex) InternalForceExpirationTimestamp(value time.Time) error {
	lock := c.options.LockFactory.CreateLock(expireMetricLockName, metricExpiratorLockTimeToLive)
	if acquired := lock.TryLock(); !acquired {
		return errors.New("lock held, please retry")
	}

	defer lock.Unlock()

	return c.options.States.Write(expireMetricStateName, value.Format(time.RFC3339))
}

// cassandraExpire remove all entry in Cassandra that have expired.
func (c *CassandraIndex) cassandraExpire(now time.Time) {
	lock := c.options.LockFactory.CreateLock(expireMetricLockName, metricExpiratorLockTimeToLive)
	if acquired := lock.TryLock(); !acquired {
		return
	}
	defer lock.Unlock()

	start := time.Now()

	defer func() {
		expireTotalSeconds.Observe(time.Since(start).Seconds())
	}()

	var lastProcessedDay time.Time

	{
		var fromTimeStr string
		_, err := c.options.States.Read(expireMetricStateName, &fromTimeStr)

		if err != nil {
			logger.Printf("Waring: unable to get last processed day for metrics expiration: %v", err)
			return
		}

		if fromTimeStr != "" {
			lastProcessedDay, _ = time.Parse(time.RFC3339, fromTimeStr)
		}
	}

	maxTime := now.Truncate(24 * time.Hour).Add(-24 * time.Hour)

	if lastProcessedDay.IsZero() {
		lastProcessedDay = maxTime

		err := c.options.States.Write(expireMetricStateName, lastProcessedDay.Format(time.RFC3339))
		if err != nil {
			logger.Printf("Waring: unable to set last processed day for metrics expiration: %v", err)
			return
		}
	}

	candidateDay := lastProcessedDay.Add(24 * time.Hour)

	if candidateDay.After(maxTime) {
		return
	}

	// We don't need the newMetricLockName lock here, because newly created metrics
	// won't be added in candidateDay (which is in the past).
	bitmap, err := c.cassandraGetExpirationList(candidateDay)
	if err != nil {
		logger.Printf("Waring: unable to get list of metrics to check for expiration: %v", err)
		return
	}

	debug.Print(debug.Level1, logger, "processing expiration for day %v", candidateDay)

	results := make([]uint64, expireBatchSize)

	var buffer bytes.Buffer

	for {
		results = results[0:expireBatchSize]
		iter := bitmap.Iterator()

		n := 0
		for v, eof := iter.Next(); !eof && n < expireBatchSize; v, eof = iter.Next() {
			results[n] = v
			n++
		}

		results = results[:n]

		if len(results) == 0 {
			break
		}

		if err := c.cassandraCheckExpire(results, now); err != nil {
			logger.Printf("Waring: unable to perform expiration check of metrics: %v", err)
			return
		}

		_, err = bitmap.Remove(results...)
		if err != nil {
			logger.Printf("Waring: unable to update list of metrics to check for expiration: %v", err)
			return
		}

		if !bitmap.Any() {
			break
		}

		buffer.Reset()

		_, err = bitmap.WriteTo(&buffer)
		if err != nil {
			logger.Printf("Waring: unable to update list of metrics to check for expiration: %v", err)
			return
		}

		err = c.store.InsertExpiration(candidateDay, buffer.Bytes())
		if err != nil {
			logger.Printf("Waring: unable to update list of metrics to check for expiration: %v", err)
			return
		}
	}

	err = c.store.DeleteExpiration(candidateDay)
	if err != nil && err != gocql.ErrNotFound {
		logger.Printf("Waring: unable to remove processed list of metrics to check for expiration: %v", err)
		return
	}

	err = c.options.States.Write(expireMetricStateName, candidateDay.Format(time.RFC3339))
	if err != nil {
		logger.Printf("Waring: unable to set last processed day for metrics expiration: %v", err)
		return
	}
}

// cassandraCheckExpire actually check for metric expired or not, and perform changes.
// It assume that Cassandra lock expireMetricLockName is taken
//
// This method perform changes at the end, because changes will require the Cassandra lock newMetricLockName,
// and we want to hold this lock only a very short time.
//
// This purge will also remove entry from the in-memory cache.
func (c *CassandraIndex) cassandraCheckExpire(ids []uint64, now time.Time) error {
	var (
		expireUpdates []expirationUpdateRequest
	)

	dayToExpireUpdates := make(map[time.Time]int)
	bulkDelete := newBulkDeleter(c)

	for _, intID := range ids {
		id := types.MetricID(intID)

		expire, err := c.store.SelectID2LabelsExpiration(id)
		if err == gocql.ErrNotFound {
			// This shouldn't happen. It means that metric were partially created.
			// Cleanup this metric from all posting if ever it's present in this list.
			expireGhostMetric.Inc()

			bulkDelete.PrepareDelete(id, nil, false)

			continue
		} else if err != nil {
			return err
		}

		if expire.After(now) {
			expireDay := expire.Truncate(24 * time.Hour)

			idx, ok := dayToExpireUpdates[expireDay]
			if !ok {
				idx = len(expireUpdates)
				expireUpdates = append(expireUpdates, expirationUpdateRequest{
					Day: expireDay,
				})
				dayToExpireUpdates[expireDay] = idx
			}

			expireUpdates[idx].AddIDs = append(expireUpdates[idx].AddIDs, intID)

			continue
		}

		sortedLabels, err := c.lookupLabels(id, false, now)
		if err != nil {
			return err
		}

		bulkDelete.PrepareDelete(id, sortedLabels, false)
	}

	c.newMetricLock.Lock()
	defer c.newMetricLock.Unlock()

	start := time.Now()

	err := bulkDelete.Delete()
	if err != nil {
		return err
	}

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range expireUpdates {
			req := req
			task := func() error {
				return c.expirationUpdate(req)
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	expireLockSeconds.Observe(time.Since(start).Seconds())

	expireMetricDelete.Add(float64(len(bulkDelete.deleteIDs)))
	expireMetric.Add(float64(len(ids)))

	return nil
}

// Run tasks concurrently with at most concurrency task in parralle.
// The queryGenerator must stop sending to the channel as soon as ctx is terminated.
func (c *CassandraIndex) concurrentTasks(queryGenerator func(ctx context.Context, c chan<- func() error) error) error {
	group, ctx := errgroup.WithContext(context.Background())
	work := make(chan func() error)

	group.Go(func() error {
		defer close(work)
		return queryGenerator(ctx, work)
	})

	for n := 0; n < concurrentInsert; n++ {
		group.Go(func() error {
			for task := range work {
				err := task()

				if err != nil {
					return err
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			return nil
		})
	}

	return group.Wait()
}

type postingUpdateRequest struct {
	Label     labels.Label
	AddIDs    []uint64
	RemoveIDs []uint64
}

func (c *CassandraIndex) postingUpdate(job postingUpdateRequest) error {
	bitmap, err := c.postings(job.Label.Name, job.Label.Value)
	if err != nil {
		return err
	}

	_, err = bitmap.Add(job.AddIDs...)
	if err != nil {
		return err
	}

	_, err = bitmap.Remove(job.RemoveIDs...)
	if err != nil {
		return err
	}

	if !bitmap.Any() {
		return c.store.DeletePostings(job.Label.Name, job.Label.Value)
	}

	var buffer bytes.Buffer

	_, err = bitmap.WriteTo(&buffer)
	if err != nil {
		return err
	}

	err = c.store.InsertPostings(job.Label.Name, job.Label.Value, buffer.Bytes())

	return err
}

type expirationUpdateRequest struct {
	Day       time.Time
	AddIDs    []uint64
	RemoveIDs []uint64
}

func (c *CassandraIndex) expirationUpdate(job expirationUpdateRequest) error {
	bitmapExpiration, err := c.cassandraGetExpirationList(job.Day)

	if err != nil {
		return err
	}

	_, err = bitmapExpiration.Add(job.AddIDs...)
	if err != nil {
		return err
	}

	_, err = bitmapExpiration.Remove(job.RemoveIDs...)
	if err != nil {
		return err
	}

	if !bitmapExpiration.Any() {
		return c.store.DeleteExpiration(job.Day)
	}

	var buffer bytes.Buffer

	_, err = bitmapExpiration.WriteTo(&buffer)

	if err != nil {
		return err
	}

	return c.store.InsertExpiration(job.Day, buffer.Bytes())
}

func matchValues(matcher *labels.Matcher, re *regexp.Regexp, value string) bool {
	var match bool

	if re != nil {
		match = re.MatchString(value)
	} else {
		match = (value == matcher.Value)
	}

	if matcher.Type == labels.MatchNotEqual || matcher.Type == labels.MatchNotRegexp {
		return !match
	}

	return match
}

func inverseMatcher(m *labels.Matcher) *labels.Matcher {
	mInv := labels.Matcher{
		Name:  m.Name,
		Value: m.Value,
	}

	switch m.Type {
	case labels.MatchEqual:
		mInv.Type = labels.MatchNotEqual
	case labels.MatchNotEqual:
		mInv.Type = labels.MatchEqual
	case labels.MatchRegexp:
		mInv.Type = labels.MatchNotRegexp
	case labels.MatchNotRegexp:
		mInv.Type = labels.MatchRegexp
	}

	return &mInv
}

// postingsForMatchers return metric IDs matching given matcher.
// The logic is taken from Prometheus PostingsForMatchers (in querier.go).
func (c *CassandraIndex) postingsForMatchers(matchers []*labels.Matcher) (ids []types.MetricID, err error) { //nolint: gocognit
	re := make([]*regexp.Regexp, len(matchers))
	labelMustBeSet := make(map[string]bool, len(matchers))

	for i, m := range matchers {
		if m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp {
			re[i], err = regexp.Compile("^(?:" + m.Value + ")$")

			if err != nil {
				return nil, err
			}
		}

		if !matchValues(m, re[i], "") {
			labelMustBeSet[m.Name] = true
		}
	}

	var its, notIts []*roaring.Bitmap

	for i, m := range matchers {
		if labelMustBeSet[m.Name] {
			matchesEmpty := matchValues(m, re[i], "")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp

			// nolint: gocritic
			if isNot && matchesEmpty { // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse := inverseMatcher(m)
				it, err := c.postingsForMatcher(inverse, re[i])

				if err != nil {
					return nil, err
				}

				notIts = append(notIts, it)
			} else if isNot && !matchesEmpty { // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse := inverseMatcher(m)

				it, err := c.inversePostingsForMatcher(inverse, re[i])
				if err != nil {
					return nil, err
				}
				its = append(its, it)
			} else { // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it, err := c.postingsForMatcher(m, re[i])

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
			it, err := c.inversePostingsForMatcher(m, re[i])

			if err != nil {
				return nil, err
			}

			notIts = append(notIts, it)
		}
	}

	// If there's nothing to subtract from, add in everything and remove the notIts later.
	if len(its) == 0 && len(notIts) != 0 {
		allPostings, err := c.postings(allPostingLabelName, allPostingLabelValue)

		if err != nil {
			return nil, err
		}

		its = append(its, allPostings)
	}

	if len(its) == 0 {
		return nil, nil
	}

	it := its[0]

	for _, other := range its[1:] {
		it = it.Intersect(other)
	}

	it = substractResult(it, notIts...)

	return bitsetToIDs(it), nil
}

// postingsForMatcher return id that match one matcher.
// This method will not return postings for missing labels.
func (c *CassandraIndex) postingsForMatcher(m *labels.Matcher, re *regexp.Regexp) (*roaring.Bitmap, error) {
	if m.Type == labels.MatchEqual {
		return c.postings(m.Name, m.Value)
	}

	values, err := c.labelValues(m.Name)

	if err != nil {
		return nil, err
	}

	var res []string

	for _, val := range values {
		if matchValues(m, re, val) {
			res = append(res, val)
		}
	}

	workSets := make([]*roaring.Bitmap, len(res))
	for i, v := range res {
		workSets[i], err = c.postings(m.Name, v)

		if err != nil {
			return nil, err
		}
	}

	if len(workSets) == 0 {
		return roaring.NewBTreeBitmap(), nil
	}

	it := workSets[0]

	for _, other := range workSets[1:] {
		it.UnionInPlace(other)
	}

	return it, nil
}

func (c *CassandraIndex) inversePostingsForMatcher(m *labels.Matcher, re *regexp.Regexp) (*roaring.Bitmap, error) {
	values, err := c.labelValues(m.Name)

	if err != nil {
		return nil, err
	}

	var res []string

	for _, val := range values {
		if !matchValues(m, re, val) {
			res = append(res, val)
		}
	}

	workSets := make([]*roaring.Bitmap, len(res))

	for i, v := range res {
		workSets[i], err = c.postings(m.Name, v)

		if err != nil {
			return nil, err
		}
	}

	if len(workSets) == 0 {
		return roaring.NewBTreeBitmap(), nil
	}

	it := workSets[0]

	for _, other := range workSets[1:] {
		it.UnionInPlace(other)
	}

	return it, nil
}

// substractResult remove from main all ID found in on lists.
func substractResult(main *roaring.Bitmap, lists ...*roaring.Bitmap) *roaring.Bitmap {
	if len(lists) == 0 {
		return main
	}

	l := lists[0]

	for _, other := range lists[1:] {
		l.UnionInPlace(other)
	}

	return main.Difference(l)
}

func (c *CassandraIndex) cassandraGetExpirationList(day time.Time) (*roaring.Bitmap, error) {
	tmp := roaring.NewBTreeBitmap()

	buffer, err := c.store.SelectExpiration(day)
	if err == gocql.ErrNotFound {
		return tmp, nil
	} else if err != nil {
		return nil, err
	}

	err = tmp.UnmarshalBinary(buffer)

	return tmp, err
}

// popLabelsValue get and delete value via its name from a labels.Label list.
func popLabelsValue(labels *labels.Labels, key string) (string, bool) {
	for i, label := range *labels {
		if label.Name == key {
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			return label.Value, true
		}
	}

	return "", false
}

// getMatchersValue gets value via its name from a labels.Matcher list.
func getMatchersValue(matchers []*labels.Matcher, name string) (string, bool) {
	for _, matcher := range matchers {
		if matcher.Name == name {
			return matcher.Value, true
		}
	}

	return "", false
}

// sortLabels returns the labels.Label list sorted by name.
func sortLabels(labelList labels.Labels) labels.Labels {
	sortedLabels := labelList.Copy()
	sort.Sort(sortedLabels)

	return sortedLabels
}

// Returns and delete time to live from a labels.Label list.
func timeToLiveFromLabels(labels *labels.Labels) int64 {
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

func bitsetToIDs(it *roaring.Bitmap) []types.MetricID {
	resultInts := it.Slice()
	results := make([]types.MetricID, len(resultInts))

	for i, v := range resultInts {
		results[i] = types.MetricID(v)
	}

	return results
}

// HasNext() must always be called once before each Next() (and next called once).
type bytesIter interface {
	HasNext() bool
	Next() []byte
	Err() error
}

type cassandraStore struct {
	session    *gocql.Session
	schemaLock sync.Locker
}

type cassandraByteIter struct {
	Iter   *gocql.Iter
	buffer []byte
	err    error
}

func (i *cassandraByteIter) HasNext() bool {
	if i.Iter.Scan(&i.buffer) {
		return true
	}

	i.err = i.Iter.Close()

	return false
}

func (i cassandraByteIter) Next() []byte {
	return i.buffer
}

func (i cassandraByteIter) Err() error {
	return i.err
}

// createTables create all Cassandra tables.
func (s cassandraStore) Init() error {
	s.schemaLock.Lock()
	defer s.schemaLock.Unlock()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS index_labels2id (
			labels text,
			id bigint,
			PRIMARY KEY (labels)
		)`,
		`CREATE TABLE IF NOT EXISTS index_postings (
			name text,
			value text,
			bitset blob,
			PRIMARY KEY (name, value)
		)`,
		`CREATE TABLE IF NOT EXISTS index_id2labels (
			id bigint,
			labels frozen<list<tuple<text, text>>>,
			expiration_date timestamp,
			PRIMARY KEY (id)
		)`,
		`CREATE TABLE IF NOT EXISTS index_expiration (
			day timestamp,
			bitset blob,
			PRIMARY KEY (day)
		)`,
	}

	for _, query := range queries {
		if err := s.session.Query(query).Consistency(gocql.All).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (s cassandraStore) InsertPostings(name string, value string, bitset []byte) error {
	return s.session.Query(
		"INSERT INTO index_postings (name, value, bitset) VALUES (?, ?, ?)",
		name, value, bitset,
	).Exec()
}

func (s cassandraStore) InsertID2Labels(id types.MetricID, sortedLabels labels.Labels, expiration time.Time) error {
	return s.session.Query(
		"INSERT INTO index_id2labels (id, labels, expiration_date) VALUES (?, ?, ?)",
		id, sortedLabels, expiration,
	).Exec()
}
func (s cassandraStore) InsertLabels2ID(sortedLabelsString string, id types.MetricID) error {
	return s.session.Query(
		"INSERT INTO index_labels2id (labels, id) VALUES (?, ?)",
		sortedLabelsString, id,
	).Exec()
}

func (s cassandraStore) DeleteLabels2ID(sortedLabelsString string) error {
	return s.session.Query(
		"DELETE FROM index_labels2id WHERE labels = ?",
		sortedLabelsString,
	).Exec()
}

func (s cassandraStore) DeleteID2Labels(id types.MetricID) error {
	return s.session.Query(
		"DELETE FROM index_id2labels WHERE id = ?",
		id,
	).Exec()
}

func (s cassandraStore) DeleteExpiration(day time.Time) error {
	return s.session.Query(
		"DELETE FROM index_expiration WHERE day = ?",
		day,
	).Exec()
}

func (s cassandraStore) DeletePostings(name string, value string) error {
	return s.session.Query(
		"DELETE FROM index_postings WHERE name = ? AND value = ?",
		name, value,
	).Exec()
}

func (s cassandraStore) InsertExpiration(day time.Time, bitset []byte) error {
	return s.session.Query(
		"INSERT INTO index_expiration (day, bitset) VALUES (?, ?)",
		day, bitset,
	).Exec()
}

func (s cassandraStore) SelectLabels2ID(sortedLabelsString string) (types.MetricID, error) {
	query := s.session.Query(
		"SELECT id FROM index_labels2id WHERE labels = ?",
		sortedLabelsString,
	)

	var cqlID int64

	err := query.Scan(&cqlID)

	return types.MetricID(cqlID), err
}

func (s cassandraStore) SelectID2Labels(id types.MetricID) (labelList labels.Labels, err error) {
	query := s.session.Query(
		"SELECT labels FROM index_id2labels WHERE id = ?",
		int64(id),
	)

	err = query.Scan(&labelList)

	return
}

func (s cassandraStore) SelectID2LabelsExpiration(id types.MetricID) (time.Time, error) {
	query := s.session.Query(
		"SELECT expiration_date FROM index_id2labels WHERE id = ?",
		int64(id),
	)

	var expiration time.Time

	err := query.Scan(&expiration)

	return expiration, err
}

func (s cassandraStore) SelectExpiration(day time.Time) ([]byte, error) {
	query := s.session.Query(
		"SELECT bitset FROM index_expiration WHERE day = ?",
		day,
	)

	var buffer []byte
	err := query.Scan(&buffer)

	return buffer, err
}

func (s cassandraStore) UpdateID2LabelsExpiration(id types.MetricID, expiration time.Time) error {
	query := s.session.Query(
		"UPDATE index_id2labels SET expiration_date = ? WHERE id = ?",
		expiration,
		int64(id),
	)

	return query.Exec()
}

func (s cassandraStore) SelectPostingByName(name string) bytesIter {
	iter := s.session.Query(
		"SELECT bitset FROM index_postings WHERE name = ?",
		name,
	).Iter()

	return &cassandraByteIter{
		Iter: iter,
	}
}

func (s cassandraStore) SelectPostingByNameValue(name string, value string) (buffer []byte, err error) {
	query := s.session.Query(
		"SELECT bitset FROM index_postings WHERE name = ? AND value = ?",
		name, value,
	)

	err = query.Scan(&buffer)

	return
}

func (s cassandraStore) SelectValueForName(name string) ([]string, error) {
	iter := s.session.Query(
		"SELECT value FROM index_postings WHERE name = ?",
		name,
	).Iter()

	var (
		values []string
		value  string
	)

	for iter.Scan(&value) {
		values = append(values, value)
	}

	err := iter.Close()

	return values, err
}
