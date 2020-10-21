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
	idInShard                map[int32]*roaring.Bitmap
	idInShardLastAccess      map[int32]time.Time

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
	SelectLabelsList2ID(sortedLabelsListString []string) (map[string]types.MetricID, error)
	SelectIDS2Labels(id []types.MetricID) (map[types.MetricID]labels.Labels, error)
	SelectExpiration(day time.Time) ([]byte, error)
	SelectIDS2LabelsExpiration(id []types.MetricID) (map[types.MetricID]time.Time, error)
	SelectPostingByName(shard int32, name string) bytesIter
	SelectPostingByNameValue(shard int32, name string, value string) ([]byte, error)
	SelectValueForName(shard int32, name string) ([]string, [][]byte, error)
	InsertPostings(shard int32, name string, value string, bitset []byte) error
	InsertID2Labels(id types.MetricID, sortedLabels labels.Labels, expiration time.Time) error
	InsertLabels2ID(sortedLabelsString string, id types.MetricID) error
	InsertExpiration(day time.Time, bitset []byte) error
	UpdateID2LabelsExpiration(id types.MetricID, expiration time.Time) error
	DeleteLabels2ID(sortedLabelsString string) error
	DeleteID2Labels(id types.MetricID) error
	DeleteExpiration(day time.Time) error
	DeletePostings(shard int32, name string, value string) error
}

const (
	globalAllPostingLabel = "__global__all|metrics__" // we use the "|" since it's invalid for prometheus label name
	allPostingLabel       = "__all|metrics__"
	maybePostingLabel     = "__maybe|metrics__" // ID are added in two-phase in postings. This one is updated first. See updatePostings
	existingShardsLabel   = "__shard|exists__"  // We store existings shards in postings
	postingShardSize      = 7 * 24 * time.Hour
	// Index is shard by time for postings. The shard number (an int32) is the
	// rounded to postingShardSize number of hours since epoc (1970).
	// The globalShardNumber value is an impossible value for normal shard,
	// because postingShardSize is a multiple of 2 hours making odd shard number
	// impossible.
	globalShardNumber = -1
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
		idInShard:                make(map[int32]*roaring.Bitmap),
		idInShardLastAccess:      make(map[int32]time.Time),
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
			for ctx.Err() == nil && c.RunOnce(ctx, time.Now()) {
				time.Sleep(10 * time.Second)
			}
		case <-ctx.Done():
			debug.Print(2, logger, "Cassandra index service stopped")
			return
		}
	}
}

// RunOnce run the tasks scheduled by Run, return true if more work is pending.
// Prefer using Run() than calling RunOnce multiple time. RunOnce is mostly here
// for squirreldb-cassandra-index-bench program.
func (c *CassandraIndex) RunOnce(ctx context.Context, now time.Time) bool {
	c.expire(now)
	c.applyExpirationUpdateRequests()
	moreWork := c.cassandraExpire(now)
	c.periodicRefreshIDInShard(now)

	return moreWork
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

		for shard, bitmap := range c.idInShard {
			_, _ = bitmap.RemoveN(deleteIDs...)
			c.idInShard[shard] = bitmap
		}
		c.lookupIDMutex.Unlock()
	}
}

// Verify perform some verification of the indexes health.
func (c *CassandraIndex) Verify(ctx context.Context, w io.Writer, doFix bool, acquireLock bool) (hadIssue bool, err error) {
	bulkDeleter := newBulkDeleter(c)

	if doFix && !acquireLock {
		return hadIssue, errors.New("doFix require acquire lock")
	}

	if acquireLock {
		c.newMetricLock.Lock()
		defer c.newMetricLock.Unlock()
	}

	allGoodIds := roaring.NewBTreeBitmap()

	allPosting, err := c.postings([]int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel)
	if err != nil {
		return hadIssue, err
	}

	count := 0
	countOk := 0
	it := allPosting.Iterator()

	labelNames := make(map[string]interface{})
	pendingIds := make([]types.MetricID, 0, 10000)

	for ctx.Err() == nil {
		pendingIds = pendingIds[:0]

		for ctx.Err() == nil {
			id, eof := it.Next()
			if eof {
				break
			}

			metricID := types.MetricID(id)

			count++

			_, _ = allGoodIds.AddN(id)

			pendingIds = append(pendingIds, metricID)

			if len(pendingIds) > 1000 {
				break
			}
		}

		if len(pendingIds) == 0 {
			break
		}

		if len(pendingIds) > 0 {
			newOk, err := c.verifyBulk(ctx, w, doFix, pendingIds, bulkDeleter, labelNames, allPosting)
			if err != nil {
				return hadIssue, err
			}

			countOk += newOk
		}
	}

	fmt.Fprintf(w, "Index contains %d metrics and %d ok. There is %d label names\n", count, countOk, len(labelNames))

	if doFix {
		fmt.Fprintf(w, "Applying fix...")

		if err := bulkDeleter.Delete(); err != nil {
			return hadIssue, err
		}
	}
	/*
		for name := range labelNames {
			if ctx.Err() != nil {
				break
			}

			iter := c.store.SelectPostingByName(name)
			for iter.HasNext() {
				tmp := roaring.NewBTreeBitmap()

				err := tmp.UnmarshalBinary(iter.Next())
				if err != nil {
					return hadIssue, err
				}

				tmp = tmp.Difference(allGoodIds)
				it := tmp.Iterator()

				for ctx.Err() == nil {
					id, eof := it.Next()
					if eof {
						break
					}

					hadIssue = true

					fmt.Fprintf(
						w,
						"Posting for name %s has ID %d which is not in all posting!\n",
						name,
						id,
					)
				}
			}
		}
	*/
	return hadIssue, ctx.Err()
}

func (c *CassandraIndex) verifyBulk(ctx context.Context, w io.Writer, doFix bool, ids []types.MetricID, bulkDeleter *deleter, labelNames map[string]interface{}, allPosting *roaring.Bitmap) (newOk int, err error) { // nolint: gocognit
	id2Labels, err := c.store.SelectIDS2Labels(ids)
	if err != nil {
		return 0, err
	}

	id2expiration, err := c.store.SelectIDS2LabelsExpiration(ids)
	if err != nil {
		return 0, err
	}

	allLabelsString := make([]string, 0, len(ids))

	for _, id := range ids {
		lbls, ok := id2Labels[id]
		if !ok {
			fmt.Fprintf(w, "ID %10d does not exists in ID2Labels, partial write ?\n", id)

			if doFix {
				bulkDeleter.PrepareDelete(id, nil, false)
			}

			continue
		}

		for _, l := range lbls {
			labelNames[l.Name] = nil
		}

		allLabelsString = append(allLabelsString, lbls.String())

		_, ok = id2expiration[id]
		if !ok {
			return 0, fmt.Errorf("ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify", id, lbls.String())
		}
	}

	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	labels2ID, err := c.store.SelectLabelsList2ID(allLabelsString)
	if err != nil {
		return 0, err
	}

	countOk := 0

	for _, id := range ids {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		lbls, ok := id2Labels[id]
		if !ok {
			continue
		}

		expiration, ok := id2expiration[id]
		if !ok {
			continue
		}

		id2, ok := labels2ID[lbls.String()]
		if !ok {
			fmt.Fprintf(w, "ID %10d (%v) not found in Labels2ID, partial write ?\n", id, lbls.String())

			if doFix {
				bulkDeleter.PrepareDelete(id, lbls, false)
			}

			continue
		}

		if id != id2 {
			tmp, err := c.store.SelectIDS2Labels([]types.MetricID{id2})
			if err != nil {
				return 0, err
			}

			lbls2 := tmp[id2]

			tmp2, err := c.store.SelectIDS2LabelsExpiration([]types.MetricID{id2})
			if err != nil {
				return 0, err
			}

			expiration2, ok := tmp2[id2]
			if !ok && lbls2 != nil {
				return 0, fmt.Errorf("ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify", id2, lbls2.String())
			}

			switch {
			case lbls2 == nil:
				fmt.Fprintf(
					w,
					"ID %10d (%v) conflict with ID %d (which is a partial write! THIS SHOULD NOT HAPPEN.)\n",
					id,
					lbls.String(),
					id2,
				)

				if doFix {
					// well, the only solution is to delete *both* ID.
					bulkDeleter.PrepareDelete(id2, lbls, false)
					bulkDeleter.PrepareDelete(id, lbls, false)
				}
			case !allPosting.Contains(uint64(id2)):
				fmt.Fprintf(
					w,
					"ID %10d (%v) conflict with ID %d (which isn't listed in all posting! THIS SHOULD NOT HAPPEN.)\n",
					id,
					lbls.String(),
					id2,
				)

				if doFix {
					// well, the only solution is to delete *both* ID.
					bulkDeleter.PrepareDelete(id2, lbls2, false)
					bulkDeleter.PrepareDelete(id, lbls, false)
				}
			default:
				fmt.Fprintf(
					w,
					"ID %10d (%v) conflict with ID %d (%v). first expire at %v, second at %v\n",
					id,
					lbls.String(),
					id2,
					lbls2.String(),
					expiration,
					expiration2,
				)
				// Assume that metric2 is better. It has id2labels, labels2id and in all postings
				if doFix {
					bulkDeleter.PrepareDelete(id, lbls, true)
				}
			}

			continue
		}

		if time.Now().Add(24 * time.Hour).After(expiration) {
			fmt.Fprintf(w, "ID %10d (%v) should have expired on %v\n", id, lbls.String(), expiration)

			if doFix {
				bulkDeleter.PrepareDelete(id, lbls, false)
			}

			continue
		}

		countOk++
	}

	return countOk, nil
}

// AllIDs returns all ids stored in the index.
func (c *CassandraIndex) AllIDs(start time.Time, end time.Time) ([]types.MetricID, error) {
	shards := getTimeShards(start, end)
	bitmap, err := c.postings(shards, allPostingLabel, allPostingLabel)

	if err != nil {
		return nil, err
	}

	return bitsetToIDs(bitmap), nil
}

// postings return ids matching give Label name & value
// If value is the empty string, it match any values (but the label must be set).
func (c *CassandraIndex) postings(shards []int32, name string, value string) (*roaring.Bitmap, error) {
	if name == allPostingLabel {
		value = allPostingLabel
	}

	if name == globalAllPostingLabel {
		value = globalAllPostingLabel
	}

	var result *roaring.Bitmap

	for _, shard := range shards {
		tmp := roaring.NewBTreeBitmap()

		buffer, err := c.store.SelectPostingByNameValue(shard, name, value)

		if err == gocql.ErrNotFound {
			err = nil
		} else if err == nil {
			err = tmp.UnmarshalBinary(buffer)
		}

		if err != nil {
			return nil, err
		}

		if result == nil {
			result = tmp
		} else {
			result.UnionInPlace(tmp)
		}
	}

	if result == nil {
		result = roaring.NewBTreeBitmap()
	}

	return result, nil
}

// LookupLabels returns a Label list for each specified ID.
func (c *CassandraIndex) LookupLabels(ids []types.MetricID) ([]labels.Labels, error) {
	return c.lookupLabels(ids, c.options.IncludeID, time.Now())
}

func (c *CassandraIndex) lookupLabels(ids []types.MetricID, addID bool, now time.Time) ([]labels.Labels, error) {
	start := time.Now()

	founds := make([]bool, len(ids))
	labelsDataList := make([]labelsData, len(ids))
	idToQuery := make([]types.MetricID, 0)

	c.searchMutex.Lock()

	for i, id := range ids {
		labelsDataList[i], founds[i] = c.idsToLabels[id]
		if !founds[i] {
			idToQuery = append(idToQuery, id)
		}
	}

	c.searchMutex.Unlock()

	if len(idToQuery) > 0 {
		idToLabels, err := c.store.SelectIDS2Labels(idToQuery)
		if err != nil {
			lookupLabelsSeconds.Observe(time.Since(start).Seconds())

			return nil, err
		}

		for i, id := range ids {
			if !founds[i] {
				var ok bool

				labelsDataList[i].labels, ok = idToLabels[id]
				if !ok {
					return nil, fmt.Errorf("labels for metric ID %d not found", id)
				}
			}

			labelsDataList[i].expirationTime = now.Add(cacheExpirationDelay)
		}
	}

	c.searchMutex.Lock()

	for i, id := range ids {
		c.idsToLabels[id] = labelsDataList[i]
	}

	c.searchMutex.Unlock()

	labelList := make([]labels.Labels, len(ids))
	for i, id := range ids {
		labelList[i] = labelsDataList[i].labels
		if addID {
			labelList[i] = labelsDataList[i].labels.Copy()
			labelList[i] = append(labelList[i], labels.Label{
				Name:  idLabelName,
				Value: strconv.FormatInt(int64(id), 10),
			})
		}
	}

	lookupLabelsSeconds.Observe(time.Since(start).Seconds())

	return labelList, nil
}

// LookupIDs returns a IDs corresponding to the specified labels.Label lists
// It also return the metric TTLs
// The result list will be the same length as input lists and using the same order.
// The input may be mutated (the labels list), so reusing it would be avoided.
func (c *CassandraIndex) LookupIDs(ctx context.Context, requests []types.LookupRequest) ([]types.MetricID, []int64, error) {
	return c.lookupIDs(ctx, requests, time.Now())
}

func (c *CassandraIndex) lookupIDs(ctx context.Context, requests []types.LookupRequest, now time.Time) ([]types.MetricID, []int64, error) { // nolint: gocyclo,gocognit
	start := time.Now()

	defer func() {
		LookupIDRequestSeconds.Observe(time.Since(start).Seconds())
	}()

	LookupIDs.Add(float64(len(requests)))

	for _, req := range requests {
		if len(req.Labels) == 0 {
			return nil, nil, errors.New("empty labels set")
		}
	}

	entries, labelsToIndices, err := c.lookupIDsFromCache(now, requests)
	if err != nil {
		return nil, nil, err
	}

	if c.options.IncludeID {
		/*if err := c.lookupIDsFromLabels(requests, cassandraRequests); err != nil {
		return nil, nil, fmt.Errorf("lookup with %s failed: %v", idLabelName, err)
		}*/
		panic("not yet reimplemented")
	}

	labelsToQuery := make([]string, 0, len(requests))

	miss := 0

	for lbls, indicies := range labelsToIndices {
		miss += len(indicies)

		labelsToQuery = append(labelsToQuery, lbls)
	}

	LookupIDMisses.Add(float64(miss))

	if len(labelsToQuery) > 0 {
		labels2ID, err := c.store.SelectLabelsList2ID(labelsToQuery)
		if err != nil {
			return nil, nil, fmt.Errorf("searching metric failed: %v", err)
		}

		idsToQuery := make([]types.MetricID, 0, len(labelsToQuery))

		for _, id := range labels2ID {
			idsToQuery = append(idsToQuery, id)
		}

		ids2Expiration, err := c.store.SelectIDS2LabelsExpiration(idsToQuery)
		if err != nil {
			return nil, nil, fmt.Errorf("searching metric failed: %v", err)
		}

		for lbls, id := range labels2ID {
			if expiration, ok := ids2Expiration[id]; ok {
				for _, idx := range labelsToIndices[lbls] {
					entries[idx].id = id
					entries[idx].cassandraEntryExpiration = expiration
				}

				delete(labelsToIndices, lbls)
			}
		}
	}

	notFoundCount := 0
	founds := make([]lookupEntry, 0, len(entries))

	for _, entry := range entries {
		if entry.id == 0 {
			notFoundCount++
		} else {
			founds = append(founds, entry)
		}
	}

	LookupIDNew.Add(float64(notFoundCount))

	if notFoundCount > 0 {
		indicies := make([]int, 0, len(labelsToIndices))
		pending := make([]lookupEntry, len(labelsToIndices))

		for _, tmp := range labelsToIndices {
			indicies = append(indicies, tmp[0])
		}

		sort.Ints(indicies)

		for i, idx := range indicies {
			pending[i] = entries[idx]
		}

		if ok := c.newMetricLock.TryLock(ctx, 15*time.Second); !ok {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}

			return nil, nil, errors.New("newMetricLock is not acquired")
		}

		done, err := c.createMetrics(now, pending, false)

		c.newMetricLock.Unlock()

		if err != nil {
			return nil, nil, err
		}

		err = c.updatePostingShards(ctx, done, true)
		if err != nil {
			return nil, nil, err
		}

		for _, entry := range done {
			for _, idx := range labelsToIndices[entry.sortedLabelsString] {
				entries[idx].id = entry.id
				entries[idx].cassandraEntryExpiration = entry.cassandraEntryExpiration
			}
		}
	}

	err = c.updatePostingShards(ctx, founds, false)
	if err != nil {
		return nil, nil, err
	}

	c.lookupIDMutex.Lock()
	defer c.lookupIDMutex.Unlock()

	ids := make([]types.MetricID, len(entries))
	ttls := make([]int64, len(entries))

	for i, entry := range entries {
		if entry.id == 0 {
			return nil, nil, errors.New("unexpected error in lookup ID: metric with ID = 0 was assigned")
		}

		if entry.idData.cassandraEntryExpiration.IsZero() {
			return nil, nil, errors.New("unexpected error in lookup ID: metric with expiration = 0")
		}

		ids[i] = entry.id
		ttls[i] = entry.ttl

		wantedEntryExpiration := now.Add(time.Duration(entry.ttl) * time.Second)
		cassandraExpiration := wantedEntryExpiration.Add(cassandraTTLUpdateDelay)
		cassandraExpiration = cassandraExpiration.Add(time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second)
		// The 3*backgroundCheckInterval is to be slightly larger than 2*backgroundCheckInterval used in lookupIDsFromCache.
		// It ensure that live metrics stay in cache.
		needTTLUpdate := entry.idData.cassandraEntryExpiration.Before(wantedEntryExpiration) || entry.idData.cassandraEntryExpiration.Before(now.Add(3*backgroundCheckInterval))

		if needTTLUpdate {
			if err := c.refreshExpiration(entry.id, entry.cassandraEntryExpiration, cassandraExpiration); err != nil {
				return nil, nil, err
			}

			entry.cassandraEntryExpiration = cassandraExpiration
		}

		entry.cacheExpirationTime = now.Add(cacheExpirationDelay)
		c.setIDData(entry.labelsKey, entry.idData)
	}

	return ids, ttls, nil
}

func (c *CassandraIndex) lookupIDsFromCache(now time.Time, requests []types.LookupRequest) (entries []lookupEntry, labelsToIndices map[string][]int, err error) {
	entries = make([]lookupEntry, len(requests))
	labelsToIndices = make(map[string][]int)
	possibleInvalidIDs := make([]uint64, 0)

	c.lookupIDMutex.Lock()

	for i, req := range requests {
		ttl := timeToLiveFromLabels(&req.Labels)
		if ttl == 0 {
			ttl = int64(c.options.DefaultTimeToLive.Seconds())
		}

		labelsKey := req.Labels.Hash()
		data, found := c.getIDData(labelsKey, req.Labels)

		if found && data.cassandraEntryExpiration.Before(now.Add(2*backgroundCheckInterval)) {
			// This entry will expire soon. To reduce risk of using invalid cache (due to race
			// condition with another SquirrelDB delete metrics), first refresh the expiration,
			// and then refresh entry from Cassandra (ignore the cache).
			wantedEntryExpiration := now.Add(time.Duration(ttl) * time.Second)
			cassandraExpiration := wantedEntryExpiration.Add(cassandraTTLUpdateDelay)
			cassandraExpiration = cassandraExpiration.Add(time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second)

			if err := c.refreshExpiration(data.id, data.cassandraEntryExpiration, cassandraExpiration); err != nil {
				c.lookupIDMutex.Unlock()
				return nil, nil, err
			}

			possibleInvalidIDs = append(possibleInvalidIDs, uint64(data.id))
			found = false
		}

		if !found {
			data = idData{
				unsortedLabels: req.Labels,
			}
		}

		entries[i] = lookupEntry{
			idData:       data,
			labelsKey:    labelsKey,
			ttl:          ttl,
			wantedShards: getTimeShards(req.Start, req.End),
		}

		if !found {
			sortedLabels := sortLabels(req.Labels)
			sortedLabelsString := sortedLabels.String()

			entries[i].sortedLabels = sortedLabels
			entries[i].sortedLabelsString = sortedLabelsString

			labelsToIndices[sortedLabelsString] = append(labelsToIndices[sortedLabelsString], i)
		}
	}

	c.lookupIDMutex.Unlock()

	if len(possibleInvalidIDs) > 0 {
		c.deleteIDsFromCache(possibleInvalidIDs)
	}

	return entries, labelsToIndices, nil
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
func (c *CassandraIndex) lookupIDsFromLabels(requests []types.LookupRequest, cassandraRequests []lookupEntry) error { // nolint: unused
	for i, req := range requests {
		if cassandraRequests[i].id != 0 {
			continue
		}

		idStr := req.Labels.Get(idLabelName)
		if idStr != "" {
			id, err := strconv.ParseUint(idStr, 10, 0)
			if err != nil {
				return err
			}

			cassandraRequests[i].id = types.MetricID(id)

			tmp, err := c.store.SelectIDS2LabelsExpiration([]types.MetricID{types.MetricID(id)})
			if err != nil {
				return err
			}

			expiration, ok := tmp[types.MetricID(id)]
			if !ok {
				return fmt.Errorf("label %s (value is %s) is provided but the metric does not exists", idLabelName, idStr)
			}

			cassandraRequests[i].cassandraEntryExpiration = expiration
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

type lookupEntry struct {
	idData
	labelsKey          uint64
	ttl                int64
	sortedLabelsString string
	sortedLabels       labels.Labels
	wantedShards       []int32
}

// createMetrics creates a new metric IDs associated with provided request
// The lock is assumed to be held.
// Some care should be taken to avoid assigned the same ID from two SquirrelDB instance, so:
//
// * To avoid race-condition, redo a check that metrics is not yet registered now that lock is acquired
// * Read the all-metric postings. From there we find a free ID
// * Update Cassandra tables to store this new metrics. The insertion is done in the following order:
//   * First an entry is added to the expiration table. This ensure that in case of crash in this process, the ID will eventually be freed.
//   * Then it update:
//     * the all-metric postings. This effectively reseve the ID.
//     * the id2labels tables (it give informations needed to cleanup other postings)
//   * finally insert in labels2id, which is done as last because it's this table that determine that a metrics didn't exists
//
// If the above process crash and partially write some value, it still in a good state because:
// * For the insertion, it's the last entry (in labels2id) that matter. The the creation will be retried when point is retried
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
func (c *CassandraIndex) createMetrics(now time.Time, pending []lookupEntry, allowForcingIDAndExpiration bool) ([]lookupEntry, error) { // nolint: gocognit
	start := time.Now()
	expirationUpdateRequests := make(map[time.Time]expirationUpdateRequest)

	defer func() {
		CreateMetricSeconds.Observe(time.Since(start).Seconds())
	}()

	allPosting, err := c.postings([]int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel)
	if err != nil {
		return nil, err
	}

	labelsToQuery := make([]string, len(pending))

	for i, entry := range pending {
		labelsToQuery[i] = entry.sortedLabelsString
	}

	// Be sure no-one registered the metric before we took the lock.
	labels2ID, err := c.store.SelectLabelsList2ID(labelsToQuery)
	if err != nil {
		return nil, err
	}

	for i, entry := range pending {
		var newID types.MetricID

		id, ok := labels2ID[entry.sortedLabelsString]

		switch {
		case ok:
			newID = id

			LookupIDConcurrentNew.Inc()
		case entry.id != 0 && allowForcingIDAndExpiration:
			// This case is only used during test, where we want to force ID value
			newID = entry.id
		default:
			newID = types.MetricID(freeFreeID(allPosting))
		}

		if newID == 0 {
			return nil, errors.New("too many metrics registered, unable to find a free ID")
		}

		_, err = allPosting.Add(uint64(newID))
		if err != nil {
			return nil, err
		}

		wantedEntryExpiration := now.Add(time.Duration(entry.ttl) * time.Second)
		cassandraExpiration := wantedEntryExpiration.Add(cassandraTTLUpdateDelay)
		cassandraExpiration = cassandraExpiration.Add(time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second)

		if !pending[i].cassandraEntryExpiration.IsZero() && allowForcingIDAndExpiration {
			cassandraExpiration = pending[i].cassandraEntryExpiration
		}

		pending[i].id = newID
		pending[i].cassandraEntryExpiration = cassandraExpiration

		day := cassandraExpiration.Truncate(24 * time.Hour)
		expReq := expirationUpdateRequests[day]

		expReq.AddIDs = append(expReq.AddIDs, uint64(newID))

		expirationUpdateRequests[day] = expReq
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

	err = c.store.InsertPostings(globalShardNumber, globalAllPostingLabel, globalAllPostingLabel, buffer.Bytes())
	if err != nil {
		return nil, err
	}

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, entry := range pending {
			entry := entry
			task := func() error {
				return c.store.InsertID2Labels(entry.id, entry.sortedLabels, entry.cassandraEntryExpiration)
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
		for _, entry := range pending {
			entry := entry
			task := func() error {
				return c.store.InsertLabels2ID(entry.sortedLabelsString, entry.id)
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

	return pending, nil
}

// updatePostingShards update sharded-posting.
//
// It works with the following way:
// * if updateCache is true, gather all shard impacted and refresh the idInShard cache (avoid re-use issue after metric creation)
// * For each update in a shard, if the ID is already present, skip it
// * Then update list of existings shards
// * Add ID to maybe-present posting list (this is used in cleanup)
// * Add ID to postings
// * Add ID to presence list per shard
//
// This insertion order all to recover some failure:
// * No write will happen because ID is inserted to the presence list. Meaning
//   reads won't get inconsistent result.
//   It also means that retry of write will fix the issue
// * The insert in maybe-present allow cleanup to known if an ID (may) need cleanup in the shard.
func (c *CassandraIndex) updatePostingShards(ctx context.Context, pending []lookupEntry, updateCache bool) error { // nolint: gocognit
	start := time.Now()

	defer func() {
		updatePostingSeconds.Observe(time.Since(start).Seconds())
	}()

	if updateCache {
		shardAffected := make(map[int32]bool)

		for _, entry := range pending {
			for _, shard := range entry.wantedShards {
				shardAffected[shard] = true
			}
		}

		err := c.refreshPostingIDInShard(shardAffected)
		if err != nil {
			return err
		}
	}

	maybePresent := make(map[int32]postingUpdateRequest)
	precense := make(map[int32]postingUpdateRequest)
	updates := make([]postingUpdateRequest, 0)
	shardToLabelToIndex := make(map[int32]map[labels.Label]int)
	now := time.Now()

	c.lookupIDMutex.Lock()
	for _, entry := range pending {
		for _, shard := range entry.wantedShards {
			c.idInShardLastAccess[shard] = now

			it := c.idInShard[shard]
			if it != nil && it.Contains(uint64(entry.id)) {
				continue
			}

			req, ok := maybePresent[shard]
			if !ok {
				req.Shard = shard
				req.Label = labels.Label{
					Name:  maybePostingLabel,
					Value: maybePostingLabel,
				}
			}

			req.AddIDs = append(req.AddIDs, uint64(entry.id))
			maybePresent[shard] = req

			for _, lbl := range entry.sortedLabels {
				m, ok := shardToLabelToIndex[shard]
				if !ok {
					m = make(map[labels.Label]int)
					shardToLabelToIndex[shard] = m
				}

				idx, ok := m[lbl]
				if !ok {
					idx = len(updates)
					updates = append(updates, postingUpdateRequest{
						Label: lbl,
						Shard: shard,
					})
					m[lbl] = idx
				}

				updates[idx].AddIDs = append(updates[idx].AddIDs, uint64(entry.id))
			}

			req, ok = precense[shard]
			if !ok {
				req.Shard = shard
				req.Label = labels.Label{
					Name:  allPostingLabel,
					Value: allPostingLabel,
				}
			}

			req.AddIDs = append(req.AddIDs, uint64(entry.id))
			precense[shard] = req
		}
	}
	c.lookupIDMutex.Unlock()

	if len(maybePresent) > 0 {
		if ok := c.newMetricLock.TryLock(ctx, 15*time.Second); !ok {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return errors.New("newMetricLock is not acquired")
		}

		err := c.applyUpdatePostingShards(maybePresent, updates, precense)

		c.newMetricLock.Unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CassandraIndex) refreshPostingIDInShard(shards map[int32]bool) error {
	return c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for shard := range shards {
			shard := shard
			task := func() error {
				tmp, err := c.postings([]int32{shard}, allPostingLabel, allPostingLabel)
				if err != nil {
					return err
				}

				c.lookupIDMutex.Lock()
				c.idInShard[shard] = tmp
				c.lookupIDMutex.Unlock()

				return nil
			}

			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
}

func (c *CassandraIndex) applyUpdatePostingShards(maybePresent map[int32]postingUpdateRequest, updates []postingUpdateRequest, precense map[int32]postingUpdateRequest) error {
	newShard := make([]uint64, 0, len(maybePresent))

	for shard := range maybePresent {
		newShard = append(newShard, uint64(shard))
	}

	_, err := c.postingUpdate(postingUpdateRequest{
		Shard: globalShardNumber,
		Label: labels.Label{
			Name:  existingShardsLabel,
			Value: existingShardsLabel,
		},
		AddIDs: newShard,
	})
	if err != nil {
		return err
	}

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range maybePresent {
			req := req
			task := func() error {
				_, err := c.postingUpdate(req)
				return err
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

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range updates {
			req := req
			task := func() error {
				_, err := c.postingUpdate(req)
				return err
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

	err = c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range precense {
			req := req
			task := func() error {
				bitmap, err := c.postingUpdate(req)
				if err == nil {
					c.lookupIDMutex.Lock()
					c.idInShard[req.Shard] = bitmap
					c.lookupIDMutex.Unlock()
				}
				return err
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

	return nil
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
func (c *CassandraIndex) Search(queryStart time.Time, queryEnd time.Time, matchers []*labels.Matcher) ([]types.MetricLabel, error) {
	start := time.Now()
	shards := getTimeShards(queryStart, queryEnd)

	defer func() {
		searchMetricsSeconds.Observe(time.Since(start).Seconds())
	}()

	if len(matchers) == 0 {
		return nil, nil
	}

	var (
		ids        []types.MetricID
		labelsList []labels.Labels
		found      bool
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
		ids, labelsList, err = c.postingsForMatchers(shards, matchers)

		if err != nil {
			return nil, err
		}
	}

	searchMetricsTotal.Add(float64(len(ids)))

	results := make([]types.MetricLabel, len(ids))

	for i, id := range ids {
		results[i] = types.MetricLabel{
			ID:     id,
			Labels: labelsList[i],
		}
	}

	return results, nil
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

// InternalMaxTTLUpdateDelay return the highest delay between TTL update.
// This should only be used in test & benchmark.
func InternalMaxTTLUpdateDelay() time.Duration {
	return cassandraTTLUpdateDelay + cassandraTTLUpdateJitter
}

// InternalCreateMetric create metrics in the index with ID value forced.
// This should only be used in test & benchmark.
// The following condition on input must be meet:
// * no duplicated metrics
// * no conflict in IDs
// * labels must be sorted
// * len(metrics) == len(ids) == len(expirations).
//
// The metrics will be added in all shards between start & end.
func (c *CassandraIndex) InternalCreateMetric(start time.Time, end time.Time, metrics []labels.Labels, ids []types.MetricID, expirations []time.Time) error {
	requests := make([]lookupEntry, len(metrics))
	shards := getTimeShards(start, end)

	for i, labels := range metrics {
		sortedLabelsString := labels.String()
		requests[i] = lookupEntry{
			idData: idData{
				id:                       ids[i],
				unsortedLabels:           labels,
				cassandraEntryExpiration: expirations[i],
			},
			labelsKey:          labels.Hash(),
			wantedShards:       shards,
			sortedLabelsString: sortedLabelsString,
			sortedLabels:       labels,
		}
	}

	if ok := c.newMetricLock.TryLock(context.Background(), 15*time.Second); !ok {
		return errors.New("newMetricLock is not acquired")
	}

	done, err := c.createMetrics(time.Now(), requests, true)

	c.newMetricLock.Unlock()

	if err != nil {
		return err
	}

	for i, id := range ids {
		if done[i].id != id {
			return fmt.Errorf("savedIDs=%v didn't match requested id=%v", done[0].id, id)
		}
	}

	err = c.updatePostingShards(context.Background(), done, true)

	return err
}

// InternalForceExpirationTimestamp will force the state for the most recently processed day of metrics expiration
// This should only be used in test & benchmark.
func (c *CassandraIndex) InternalForceExpirationTimestamp(value time.Time) error {
	lock := c.options.LockFactory.CreateLock(expireMetricLockName, metricExpiratorLockTimeToLive)
	if acquired := lock.TryLock(context.Background(), 0); !acquired {
		return errors.New("lock held, please retry")
	}

	defer lock.Unlock()

	return c.options.States.Write(expireMetricStateName, value.Format(time.RFC3339))
}

func (c *CassandraIndex) periodicRefreshIDInShard(now time.Time) {
	c.lookupIDMutex.Lock()

	allShard := make(map[int32]bool, len(c.idInShardLastAccess))

	for shard, atime := range c.idInShardLastAccess {
		if now.Sub(atime) > cacheExpirationDelay {
			delete(c.idInShard, shard)
			delete(c.idInShardLastAccess, shard)
		} else {
			allShard[shard] = true
		}
	}

	c.lookupIDMutex.Unlock()

	err := c.refreshPostingIDInShard(allShard)
	if err != nil {
		debug.Print(debug.Level1, logger, "refresh PostingIDInShard failed: %v", err)
	}
}

// cassandraExpire remove all entry in Cassandra that have expired.
func (c *CassandraIndex) cassandraExpire(now time.Time) bool {
	lock := c.options.LockFactory.CreateLock(expireMetricLockName, metricExpiratorLockTimeToLive)
	if acquired := lock.TryLock(context.Background(), 0); !acquired {
		return false
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
			return false
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
			return false
		}
	}

	candidateDay := lastProcessedDay.Add(24 * time.Hour)

	if candidateDay.After(maxTime) {
		return false
	}

	// We don't need the newMetricLockName lock here, because newly created metrics
	// won't be added in candidateDay (which is in the past).
	bitmap, err := c.cassandraGetExpirationList(candidateDay)
	if err != nil {
		logger.Printf("Waring: unable to get list of metrics to check for expiration: %v", err)
		return false
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
			return false
		}

		_, err = bitmap.Remove(results...)
		if err != nil {
			logger.Printf("Waring: unable to update list of metrics to check for expiration: %v", err)
			return false
		}

		if !bitmap.Any() {
			break
		}

		buffer.Reset()

		_, err = bitmap.WriteTo(&buffer)
		if err != nil {
			logger.Printf("Waring: unable to update list of metrics to check for expiration: %v", err)
			return false
		}

		err = c.store.InsertExpiration(candidateDay, buffer.Bytes())
		if err != nil {
			logger.Printf("Waring: unable to update list of metrics to check for expiration: %v", err)
			return false
		}
	}

	err = c.store.DeleteExpiration(candidateDay)
	if err != nil && err != gocql.ErrNotFound {
		logger.Printf("Waring: unable to remove processed list of metrics to check for expiration: %v", err)
		return false
	}

	err = c.options.States.Write(expireMetricStateName, candidateDay.Format(time.RFC3339))
	if err != nil {
		logger.Printf("Waring: unable to set last processed day for metrics expiration: %v", err)
		return false
	}

	return true
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
	idToDeleteWithLabels := make([]types.MetricID, 0)

	metricIDs := make([]types.MetricID, len(ids))
	for i, intID := range ids {
		metricIDs[i] = types.MetricID(intID)
	}

	expires, err := c.store.SelectIDS2LabelsExpiration(metricIDs)
	if err != nil {
		return err
	}

	for _, id := range metricIDs {
		expire, ok := expires[id]
		if !ok {
			// This shouldn't happen. It means that metric were partially created.
			// Cleanup this metric from all posting if ever it's present in this list.
			expireGhostMetric.Inc()

			bulkDelete.PrepareDelete(id, nil, false)

			continue
		} else if expire.After(now) {
			expireDay := expire.Truncate(24 * time.Hour)

			idx, ok := dayToExpireUpdates[expireDay]
			if !ok {
				idx = len(expireUpdates)
				expireUpdates = append(expireUpdates, expirationUpdateRequest{
					Day: expireDay,
				})
				dayToExpireUpdates[expireDay] = idx
			}

			expireUpdates[idx].AddIDs = append(expireUpdates[idx].AddIDs, uint64(id))

			continue
		}

		idToDeleteWithLabels = append(idToDeleteWithLabels, id)
	}

	if len(idToDeleteWithLabels) > 0 {
		labelLists, err := c.lookupLabels(idToDeleteWithLabels, false, now)
		if err != nil {
			return err
		}

		for i, id := range idToDeleteWithLabels {
			bulkDelete.PrepareDelete(id, labelLists[i], false)
		}
	}

	c.newMetricLock.Lock()
	defer c.newMetricLock.Unlock()

	start := time.Now()

	err = bulkDelete.Delete()
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
	Shard     int32
}

func (c *CassandraIndex) postingUpdate(job postingUpdateRequest) (*roaring.Bitmap, error) {
	bitmap, err := c.postings([]int32{job.Shard}, job.Label.Name, job.Label.Value)
	if err != nil {
		return nil, err
	}

	_, err = bitmap.Add(job.AddIDs...)
	if err != nil {
		return nil, err
	}

	_, err = bitmap.Remove(job.RemoveIDs...)
	if err != nil {
		return nil, err
	}

	if !bitmap.Any() {
		return nil, c.store.DeletePostings(job.Shard, job.Label.Name, job.Label.Value)
	}

	var buffer bytes.Buffer

	_, err = bitmap.WriteTo(&buffer)
	if err != nil {
		return nil, err
	}

	err = c.store.InsertPostings(job.Shard, job.Label.Name, job.Label.Value, buffer.Bytes())

	return bitmap, err
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

	idsToRemove := roaring.NewBitmap(job.RemoveIDs...)
	missingIDs := idsToRemove.Difference(bitmapExpiration)

	if missingIDs.Any() {
		slice := missingIDs.Slice()
		expireConflictMetric.Add(float64(len(slice)))

		c.deleteIDsFromCache(slice)
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

// postingsForMatchers return metric IDs matching given matcher.
// The logic is taken from Prometheus PostingsForMatchers (in querier.go).
func (c *CassandraIndex) postingsForMatchers(shards []int32, matchers []*labels.Matcher) (ids []types.MetricID, labelsList []labels.Labels, err error) { //nolint: gocognit,gocyclo
	labelMustBeSet := make(map[string]bool, len(matchers))

	for _, m := range matchers {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

	var results *roaring.Bitmap

	checkMatches := false

	// Unlike Prometheus querier.go, we merge/update directly into results (instead of
	// adding into its and notIts then building results).
	// We do this in two loops, one which fill its (the one which could add IDs - the "its" of Prometheus)
	// then one which remove ids (the "notIts" of Prometheus).
	for _, m := range matchers {
		// If there is only few results, prefer doing an explicit matching on labels
		// for each IDs left. This may spare few Cassandra query.
		// With postings filtering, we do one Cassandra query per matchers.
		// With explicit matching on labels, we do one Cassandra query per IDs BUT this query will be done anyway if the
		// series would be kept.
		if results != nil && results.Count() <= 3 {
			checkMatches = true
			break
		}

		if labelMustBeSet[m.Name] {
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp

			if isNot && !matchesEmpty { // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, nil, err
				}

				it, err := c.inversePostingsForMatcher(shards, inverse)
				if err != nil {
					return nil, nil, err
				}

				if results == nil {
					results = it
				} else {
					results = results.Intersect(it)
				}
			} else if !isNot { // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it, err := c.postingsForMatcher(shards, m)

				if err != nil {
					return nil, nil, err
				}

				if results == nil {
					results = it
				} else {
					results = results.Intersect(it)
				}
			}
		}
	}

	for _, m := range matchers {
		if results != nil && results.Count() <= 3 {
			checkMatches = true
			break
		}

		if labelMustBeSet[m.Name] {
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp

			if isNot && matchesEmpty { // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, nil, err
				}

				it, err := c.postingsForMatcher(shards, inverse)

				if err != nil {
					return nil, nil, err
				}

				if results == nil {
					// If there's nothing to subtract from, add in everything and remove the notIts later.
					results, err = c.postings(shards, allPostingLabel, allPostingLabel)
					if err != nil {
						return nil, nil, err
					}
				}

				results = results.Difference(it)
			}
		} else { // l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			it, err := c.inversePostingsForMatcher(shards, m)

			if err != nil {
				return nil, nil, err
			}

			if results == nil {
				// If there's nothing to subtract from, add in everything and remove the notIts later.
				results, err = c.postings(shards, allPostingLabel, allPostingLabel)
				if err != nil {
					return nil, nil, err
				}
			}

			results = results.Difference(it)
		}
	}

	if results == nil {
		return nil, nil, nil
	}

	ids = bitsetToIDs(results)

	labelsList, err = c.lookupLabels(ids, false, time.Now())
	if err != nil {
		return nil, nil, err
	}

	if checkMatches {
		newIds := make([]types.MetricID, 0, len(ids))
		newLabels := make([]labels.Labels, 0, len(ids))

		for i, id := range ids {
			lbls := labelsList[i]

			if matcherMatches(matchers, lbls) {
				newIds = append(newIds, id)
				newLabels = append(newLabels, lbls)
			}
		}

		ids = newIds
		labelsList = newLabels
	}

	return ids, labelsList, nil
}

// postingsForMatcher return id that match one matcher.
// This method will not return postings for missing labels.
func (c *CassandraIndex) postingsForMatcher(shards []int32, m *labels.Matcher) (*roaring.Bitmap, error) {
	if m.Type == labels.MatchEqual {
		return c.postings(shards, m.Name, m.Value)
	}

	var it *roaring.Bitmap

	for _, baseTime := range shards {
		values, allBuffers, err := c.store.SelectValueForName(baseTime, m.Name)
		if err == gocql.ErrNotFound {
			continue
		}

		if err != nil {
			return nil, err
		}

		for i, val := range values {
			if m.Matches(val) {
				bitset := roaring.NewBTreeBitmap()

				err = bitset.UnmarshalBinary(allBuffers[i])
				if err != nil {
					return nil, err
				}

				if it == nil {
					it = bitset
				} else {
					it.UnionInPlace(bitset)
				}
			}
		}
	}

	if it == nil {
		it = roaring.NewBTreeBitmap()
	}

	return it, nil
}

func (c *CassandraIndex) inversePostingsForMatcher(shards []int32, m *labels.Matcher) (*roaring.Bitmap, error) {
	if m.Type == labels.MatchNotEqual && m.Value != "" {
		inverse, err := m.Inverse()
		if err != nil {
			return nil, err
		}

		return c.postingsForMatcher(shards, inverse)
	}

	var it *roaring.Bitmap

	for _, baseTime := range shards {
		values, allBuffers, err := c.store.SelectValueForName(baseTime, m.Name)
		if err == gocql.ErrNotFound {
			continue
		}

		if err != nil {
			return nil, err
		}

		for i, val := range values {
			if !m.Matches(val) {
				bitset := roaring.NewBTreeBitmap()

				err = bitset.UnmarshalBinary(allBuffers[i])
				if err != nil {
					return nil, err
				}

				if it == nil {
					it = bitset
				} else {
					it.UnionInPlace(bitset)
				}
			}
		}
	}

	if it == nil {
		it = roaring.NewBTreeBitmap()
	}

	return it, nil
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

func getTimeShards(start time.Time, end time.Time) []int32 {
	shardSize := int32(postingShardSize.Hours())
	startTS := int32(start.Unix() / 3600)
	startTS = (startTS / shardSize) * shardSize

	endTS := int32(end.Unix() / 3600)
	if end.Unix()%3600 != 0 {
		endTS++
	}

	results := make([]int32, 0, (endTS-startTS)/shardSize)
	current := startTS

	for current <= endTS {
		results = append(results, current)
		current += shardSize
	}

	return results
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

	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS index_labels2id (
			labels text,
			id bigint,
			PRIMARY KEY (labels)
		)`,
		`CREATE TABLE IF NOT EXISTS index_postings (
			shard int,
			name text,
			value text,
			bitset blob,
			PRIMARY KEY ((shard, name), value)
		)`,
		`CREATE TABLE IF NOT EXISTS index_postings_shard (
			arbitrary_constant int,
			shard int,
			PRIMARY KEY (arbitrary_constant, shard)
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

// InternalDropTables drop tables used by the index.
// This should only be used in test & benchmark.
func InternalDropTables(session *gocql.Session) error {
	queries := []string{
		"DROP TABLE IF EXISTS index_labels2id",
		"DROP TABLE IF EXISTS index_postings",
		"DROP TABLE IF EXISTS index_id2labels",
		"DROP TABLE IF EXISTS index_expiration",
	}
	for _, query := range queries {
		if err := session.Query(query).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (s cassandraStore) InsertPostings(shard int32, name string, value string, bitset []byte) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_postings (shard, name, value, bitset) VALUES (?, ?, ?, ?)",
		shard, name, value, bitset,
	).Exec()
}

func (s cassandraStore) InsertID2Labels(id types.MetricID, sortedLabels labels.Labels, expiration time.Time) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_id2labels (id, labels, expiration_date) VALUES (?, ?, ?)",
		id, sortedLabels, expiration,
	).Exec()
}
func (s cassandraStore) InsertLabels2ID(sortedLabelsString string, id types.MetricID) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_labels2id (labels, id) VALUES (?, ?)",
		sortedLabelsString, id,
	).Exec()
}

func (s cassandraStore) DeleteLabels2ID(sortedLabelsString string) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_labels2id WHERE labels = ?",
		sortedLabelsString,
	).Exec()
}

func (s cassandraStore) DeleteID2Labels(id types.MetricID) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_id2labels WHERE id = ?",
		id,
	).Exec()
}

func (s cassandraStore) DeleteExpiration(day time.Time) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_expiration WHERE day = ?",
		day,
	).Exec()
}

func (s cassandraStore) DeletePostings(shard int32, name string, value string) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_postings WHERE shard = ? AND name = ? AND value = ?",
		shard, name, value,
	).Exec()
}

func (s cassandraStore) InsertExpiration(day time.Time, bitset []byte) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_expiration (day, bitset) VALUES (?, ?)",
		day, bitset,
	).Exec()
}

func (s cassandraStore) SelectLabelsList2ID(sortedLabelsListString []string) (map[string]types.MetricID, error) {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsRead.Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT labels, id FROM index_labels2id WHERE labels in ?",
		sortedLabelsListString,
	).Iter()

	var (
		cqlID  int64
		labels string
	)

	result := make(map[string]types.MetricID)

	for iter.Scan(&labels, &cqlID) {
		result[labels] = types.MetricID(cqlID)
	}

	err := iter.Close()

	return result, err
}

func (s cassandraStore) SelectIDS2Labels(ids []types.MetricID) (map[types.MetricID]labels.Labels, error) {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsRead.Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT id, labels FROM index_id2labels WHERE id IN ?",
		ids,
	).Iter()

	var (
		lbls labels.Labels
		id   int64
	)

	results := make(map[types.MetricID]labels.Labels, len(ids))

	for iter.Scan(&id, &lbls) {
		results[types.MetricID(id)] = lbls
	}

	err := iter.Close()

	return results, err
}

func (s cassandraStore) SelectIDS2LabelsExpiration(ids []types.MetricID) (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsRead.Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT id, expiration_date FROM index_id2labels WHERE id in ?",
		ids,
	).Iter()

	var (
		expiration time.Time
		id         int64
	)

	results := make(map[types.MetricID]time.Time, len(ids))

	for iter.Scan(&id, &expiration) {
		results[types.MetricID(id)] = expiration
	}

	err := iter.Close()

	return results, err
}

func (s cassandraStore) SelectExpiration(day time.Time) ([]byte, error) {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsRead.Observe(time.Since(start).Seconds())
	}()

	query := s.session.Query(
		"SELECT bitset FROM index_expiration WHERE day = ?",
		day,
	)

	var buffer []byte
	err := query.Scan(&buffer)

	return buffer, err
}

func (s cassandraStore) UpdateID2LabelsExpiration(id types.MetricID, expiration time.Time) error {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	query := s.session.Query(
		"UPDATE index_id2labels SET expiration_date = ? WHERE id = ?",
		expiration,
		int64(id),
	)

	return query.Exec()
}

func (s cassandraStore) SelectPostingByName(shard int32, name string) bytesIter {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsRead.Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT bitset FROM index_postings WHERE shard = ? AND name = ?",
		shard, name,
	).Iter()

	return &cassandraByteIter{
		Iter: iter,
	}
}

func (s cassandraStore) SelectPostingByNameValue(shard int32, name string, value string) (buffer []byte, err error) {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsRead.Observe(time.Since(start).Seconds())
	}()

	query := s.session.Query(
		"SELECT bitset FROM index_postings WHERE shard = ? AND name = ? AND value = ?",
		shard, name, value,
	)

	err = query.Scan(&buffer)

	return
}

func (s cassandraStore) SelectValueForName(shard int32, name string) ([]string, [][]byte, error) {
	start := time.Now()

	defer func() {
		cassandraQueriesSecondsRead.Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT value, bitset FROM index_postings WHERE shard = ? AND name = ?",
		shard, name,
	).Iter()

	var (
		values  []string
		buffers [][]byte
		value   string
		buffer  []byte
	)

	for iter.Scan(&value, &buffer) {
		values = append(values, value)
		buffers = append(buffers, buffer)
		// This is required or gocql will reuse (overwrite) the buffer
		buffer = nil
	}

	err := iter.Close()

	return values, buffers, err
}

func matcherMatches(matchers []*labels.Matcher, lbls labels.Labels) bool {
	for _, m := range matchers {
		value := lbls.Get(m.Name)
		if !m.Matches(value) {
			return false
		}
	}

	return true
}
