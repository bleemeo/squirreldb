package index

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"squirreldb/logger"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const backgroundCheckInterval = time.Minute

const cacheExpirationDelay = 300 * time.Second

const (
	concurrentInsert      = 20
	concurrentDelete      = 20
	concurrentPostingRead = 20
	concurrentRead        = 4
	maxCQLInValue         = 100 // limit of the number of value for the "IN" clause of a CQL query
	verifyBulkSize        = 1000
)

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
	clusterChannelPostingInvalidate = "index-invalidate-postings"
	newMetricLockName               = "index-new-metric"
	expireMetricLockName            = "index-ttl-metric"
	expireMetricStateName           = "index-expired-until"
	expireBatchSize                 = 1000
)

const (
	timeToLiveLabelName = "__ttl__"
)

const (
	// The expiration of entries in Cassandra starts everyday at 00:00 UTC + expirationStartOffset.
	expirationStartOffset = 6 * time.Hour
	// The expiration is checked every expirationCheckInterval.
	expirationCheckInterval = 15 * time.Minute
)

//nolint:gochecknoglobals
var (
	minTime = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTime = time.Date(100000, 1, 1, 0, 0, 0, 0, time.UTC)
)

var (
	errTimeOutOfRange = errors.New("time out of range")
	errBitmapEmpty    = errors.New("the result bitmap is empty")
)

type idData struct {
	cassandraEntryExpiration time.Time
	cacheExpirationTime      time.Time
	unsortedLabels           labels.Labels
	id                       types.MetricID
}

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type Options struct {
	LockFactory       lockFactory
	States            types.State
	SchemaLock        sync.Locker
	Cluster           types.Cluster
	DefaultTimeToLive time.Duration
}

type CassandraIndex struct {
	store   storeImpl
	options Options

	wg     sync.WaitGroup
	cancel context.CancelFunc

	lookupIDMutex            sync.Mutex
	newMetricLock            types.TryLocker
	expirationUpdateRequests map[time.Time]expirationUpdateRequest
	expirationBackoff        backoff.BackOff
	nextExpirationAt         time.Time
	labelsToID               map[uint64][]idData
	idInShard                map[int32]*roaring.Bitmap
	idInShardLastAccess      map[int32]time.Time
	existingShards           *roaring.Bitmap

	idsToLabels   *labelsLookupCache
	postingsCache *postingsCache
	metrics       *metrics
	logger        zerolog.Logger
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

	c.metrics.CacheSize.WithLabelValues("lookup-id").Inc()
}

type storeImpl interface {
	Init(ctx context.Context) error
	SelectLabelsList2ID(ctx context.Context, sortedLabelsListString []string) (map[string]types.MetricID, error)
	SelectIDS2LabelsAndExpiration(
		ctx context.Context,
		id []types.MetricID,
	) (map[types.MetricID]labels.Labels, map[types.MetricID]time.Time, error)
	SelectExpiration(ctx context.Context, day time.Time) ([]byte, error)
	// SelectPostingByName return  label value with the associated postings in sorted order
	SelectPostingByName(ctx context.Context, shard int32, name string) postingIter
	SelectPostingByNameValue(ctx context.Context, shard int32, name string, value string) ([]byte, error)
	SelectValueForName(ctx context.Context, shard int32, name string) ([]string, [][]byte, error)
	InsertPostings(ctx context.Context, shard int32, name string, value string, bitset []byte) error
	InsertID2Labels(ctx context.Context, id types.MetricID, sortedLabels labels.Labels, expiration time.Time) error
	InsertLabels2ID(ctx context.Context, sortedLabelsString string, id types.MetricID) error
	InsertExpiration(ctx context.Context, day time.Time, bitset []byte) error
	UpdateID2LabelsExpiration(ctx context.Context, id types.MetricID, expiration time.Time) error
	DeleteLabels2ID(ctx context.Context, sortedLabelsString string) error
	DeleteID2Labels(ctx context.Context, id types.MetricID) error
	DeleteExpiration(ctx context.Context, day time.Time) error
	DeletePostings(ctx context.Context, shard int32, name string, value string) error
}

const (
	globalAllPostingLabel = "__global__all|metrics__" // we use the "|" since it's invalid for prometheus label name
	allPostingLabel       = "__all|metrics__"
	postinglabelName      = "__label|names__" // kept known labels name as label value
	// ID are added in two-phase in postings. This one is updated first. See updatePostings.
	maybePostingLabel   = "__maybe|metrics__"
	existingShardsLabel = "__shard|exists__" // We store existings shards in postings
	postingShardSize    = 7 * 24 * time.Hour
	// Index is shard by time for postings. The shard number (an int32) is the
	// rounded to postingShardSize number of hours since epoc (1970).
	// The globalShardNumber value is an impossible value for normal shard,
	// because postingShardSize is a multiple of 2 hours making odd shard number
	// impossible.
	globalShardNumber = -1
)

// New creates a new CassandraIndex object.
func New(
	ctx context.Context,
	reg prometheus.Registerer,
	session *gocql.Session,
	options Options,
	logger zerolog.Logger,
) (*CassandraIndex, error) {
	metrics := newMetrics(reg)

	return initialize(
		ctx,
		cassandraStore{
			session:    session,
			schemaLock: options.SchemaLock,
			metrics:    metrics,
		},
		options,
		metrics,
		logger,
	)
}

func initialize(
	ctx context.Context,
	store storeImpl,
	options Options,
	metrics *metrics,
	logger zerolog.Logger,
) (*CassandraIndex, error) {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = time.Minute
	expBackoff.MaxInterval = 15 * time.Minute
	expBackoff.MaxElapsedTime = 0
	expBackoff.Reset()

	index := &CassandraIndex{
		store:               store,
		options:             options,
		idInShard:           make(map[int32]*roaring.Bitmap),
		idInShardLastAccess: make(map[int32]time.Time),
		labelsToID:          make(map[uint64][]idData),
		idsToLabels:         &labelsLookupCache{cache: make(map[types.MetricID]labelsEntry)},
		postingsCache: &postingsCache{
			cache: make(map[postingsCacheKey]postingEntry),
		},
		expirationUpdateRequests: make(map[time.Time]expirationUpdateRequest),
		expirationBackoff:        expBackoff,
		newMetricLock:            options.LockFactory.CreateLock(newMetricLockName, metricCreationLockTimeToLive),
		metrics:                  metrics,
		logger:                   logger,
	}

	if err := index.store.Init(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize store: %w", err)
	}

	options.Cluster.Subscribe(clusterChannelPostingInvalidate, index.invalidatePostingsListenner)

	return index, nil
}

// Start starts all Cassandra Index services.
func (c *CassandraIndex) Start(_ context.Context) error {
	if c.cancel != nil {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)

	go func() {
		defer logger.ProcessPanic()

		c.run(ctx)
	}()

	return nil
}

// Stop stop and wait all Cassandra Index services.
func (c *CassandraIndex) Stop() error {
	if c.cancel == nil {
		return errors.New("not started")
	}

	c.cancel()
	c.cancel = nil
	c.wg.Wait()

	return nil
}

func (c *CassandraIndex) run(ctx context.Context) {
	ticker := time.NewTicker(backgroundCheckInterval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			c.RunOnce(ctx, time.Now())
		case <-ctx.Done():
			c.logger.Trace().Msg("Cassandra index service stopped")

			return
		}
	}
}

// RunOnce run the tasks scheduled by Run, return true if more work is pending.
// Prefer using Start() than calling RunOnce multiple time. RunOnce is mostly here
// for squirreldb-cassandra-index-bench program.
func (c *CassandraIndex) RunOnce(ctx context.Context, now time.Time) bool {
	c.expire(now)
	c.applyExpirationUpdateRequests(ctx)
	c.periodicRefreshIDInShard(ctx, now)

	// Expire entries in cassandra every minute when there is more work, every 15mn when all work is done,
	// or with an exponential backoff between 1mn and 15mn when errors occurs.
	if now.Before(c.nextExpirationAt) {
		return false
	}

	moreWork, err := c.cassandraExpire(ctx, now)
	if err != nil {
		nextBackoff := c.expirationBackoff.NextBackOff()
		c.nextExpirationAt = now.Add(nextBackoff)

		c.logger.Printf("Warning: %v. Retrying in %v", err, nextBackoff.Round(time.Second))
	} else {
		c.expirationBackoff.Reset()

		if !moreWork {
			c.nextExpirationAt = now.Add(expirationCheckInterval)
		}
	}

	return moreWork
}

func (c *CassandraIndex) deleteIDsFromCache(deleteIDs []uint64) {
	if len(deleteIDs) > 0 {
		c.lookupIDMutex.Lock()
		size := c.idsToLabels.Drop(deleteIDs)
		c.metrics.CacheSize.WithLabelValues("lookup-labels").Set(float64(size))

		// Since we don't force sorting labels on input, we don't known the key used
		// for c.labelsToID (it's likely to be keyFromLabels(sortedLabels))
		deleteIDsMap := make(map[types.MetricID]bool, len(deleteIDs))

		for _, id := range deleteIDs {
			deleteIDsMap[types.MetricID(id)] = true
		}

		for key, idsData := range c.labelsToID {
			for _, v := range idsData {
				if deleteIDsMap[v.id] {
					// This may delete too many entry, but:
					// 1) normally only 1 entry match the hash
					// 2) it's a cache, we don't loss data
					delete(c.labelsToID, key)

					c.metrics.CacheSize.WithLabelValues("lookup-id").Sub(float64(len(idsData)))

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

func (c *CassandraIndex) getMaybePresent(ctx context.Context, shards []uint64) (map[int32]*roaring.Bitmap, error) {
	results := make(map[int32]*roaring.Bitmap, len(shards))
	l := &sync.Mutex{}

	return results, c.concurrentTasks(
		ctx,
		concurrentPostingRead,
		func(ctx context.Context, work chan<- func() error) error {
			for _, shard := range shards {
				shard := int32(shard)
				task := func() error {
					tmp, err := c.postings(ctx, []int32{shard}, allPostingLabel, allPostingLabel, false)
					if err != nil {
						return err
					}

					l.Lock()
					results[shard] = tmp
					l.Unlock()

					return nil
				}

				select {
				case work <- task:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)
}

// Dump writes a CSV with all metrics known by this index.
// The format should not be considered stable and should only be used for debugging.
func (c *CassandraIndex) Dump(ctx context.Context, w io.Writer, withExpiration bool) error {
	allPosting, err := c.postings(ctx, []int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel, false)
	if err != nil {
		return err
	}

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	it := allPosting.Iterator()
	pendingIds := make([]types.MetricID, 0, 10000)

	for ctx.Err() == nil {
		pendingIds = pendingIds[:0]

		for ctx.Err() == nil {
			id, eof := it.Next()
			if eof {
				break
			}

			metricID := types.MetricID(id)

			pendingIds = append(pendingIds, metricID)

			if len(pendingIds) > 1000 {
				break
			}
		}

		if len(pendingIds) == 0 {
			break
		}

		if len(pendingIds) > 0 {
			err := c.dumpBulk(ctx, csvWriter, pendingIds, withExpiration)
			if err != nil {
				return err
			}
		}
	}

	return ctx.Err()
}

func (c *CassandraIndex) DumpByExpirationDate(ctx context.Context, w io.Writer, expirationDate time.Time) error {
	expirationBitmap, err := c.cassandraGetExpirationList(ctx, expirationDate.Truncate(24*time.Hour))
	if err != nil {
		return fmt.Errorf("unable to get metric expiration list: %w", err)
	}

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	iter := expirationBitmap.Iterator()
	pendingIDs := make([]types.MetricID, expireBatchSize)

	for ctx.Err() == nil {
		pendingIDs = pendingIDs[0:expireBatchSize]

		n := 0
		for id, eof := iter.Next(); !eof && n < expireBatchSize; id, eof = iter.Next() {
			pendingIDs[n] = types.MetricID(id)
			n++
		}

		pendingIDs = pendingIDs[:n]

		if len(pendingIDs) == 0 {
			break
		}

		err := c.dumpBulk(ctx, csvWriter, pendingIDs, true)
		if err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (c *CassandraIndex) dumpBulk(ctx context.Context, w *csv.Writer, ids []types.MetricID, withExpiration bool) error {
	id2Labels, id2Expiration, err := c.selectIDS2LabelsAndExpiration(ctx, ids)
	if err != nil {
		return fmt.Errorf("get labels: %w", err)
	}

	for _, id := range ids {
		expiration, expirationOk := id2Expiration[id]
		lbls, labelsOk := id2Labels[id]

		var csvLine []string

		switch {
		case !labelsOk:
			csvLine = []string{
				strconv.FormatInt(int64(id), 10),
				"Missing labels! Partial write ?",
			}
		case withExpiration && !expirationOk:
			csvLine = []string{
				strconv.FormatInt(int64(id), 10),
				lbls.String(),
				"Missing expiration which shouldn't be possible !",
			}
		case withExpiration:
			csvLine = []string{
				strconv.FormatInt(int64(id), 10),
				lbls.String(),
				expiration.String(),
			}
		default:
			csvLine = []string{
				strconv.FormatInt(int64(id), 10),
				lbls.String(),
			}
		}

		err := w.Write(csvLine)
		if err != nil {
			return err
		}
	}

	return nil
}

// Verify perform some verification of the indexes health.
func (c *CassandraIndex) Verify(
	ctx context.Context,
	w io.Writer,
	doFix bool,
	acquireLock bool,
) (hadIssue bool, err error) {
	return c.verify(ctx, time.Now(), w, doFix, acquireLock)
}

func (c *CassandraIndex) verify(
	ctx context.Context,
	now time.Time,
	w io.Writer,
	doFix bool,
	acquireLock bool,
) (hadIssue bool, err error) {
	bulkDeleter := newBulkDeleter(c)

	if doFix && !acquireLock {
		return hadIssue, errors.New("doFix require acquire lock")
	}

	if acquireLock {
		c.newMetricLock.Lock()
		defer c.newMetricLock.Unlock()
	}

	issueCount, shards, err := c.verifyMissingShard(ctx, w, doFix)
	if err != nil {
		return hadIssue, err
	}

	hadIssue = hadIssue || issueCount > 0

	allGoodIds := roaring.NewBTreeBitmap()

	allPosting, err := c.postings(ctx, []int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel, false)
	if err != nil {
		return hadIssue, err
	}

	fmt.Fprintf(w, "Index had %d shards and should have %d metrics\n", shards.Count(), allPosting.Count())

	if respf, ok := w.(http.Flusher); ok {
		respf.Flush()
	}

	count := 0
	it := allPosting.Iterator()

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

			pendingIds = append(pendingIds, metricID)

			if len(pendingIds) > verifyBulkSize {
				break
			}
		}

		if len(pendingIds) == 0 {
			break
		}

		if len(pendingIds) > 0 {
			err := c.verifyBulk(ctx, now, w, doFix, pendingIds, bulkDeleter, allPosting, allGoodIds)
			if err != nil {
				return hadIssue, err
			}
		}
	}

	fmt.Fprintf(w, "Index contains %d metrics and %d ok\n", count, allGoodIds.Count())

	if respf, ok := w.(http.Flusher); ok {
		respf.Flush()
	}

	if doFix {
		fmt.Fprintf(w, "Applying fix...")

		if err := bulkDeleter.Delete(ctx); err != nil {
			return hadIssue, err
		}
	}

	for _, shard := range shards.Slice() {
		shard := int32(shard)
		fmt.Fprintf(w, "Checking shard %d\n", shard)

		tmp, err := c.verifyShard(ctx, w, doFix, shard, allGoodIds)
		if err != nil {
			return hadIssue, err
		}

		hadIssue = hadIssue || tmp

		if respf, ok := w.(http.Flusher); ok {
			respf.Flush()
		}
	}

	return hadIssue, ctx.Err()
}

// verifyMissingShard search last 100 shards from now+3 shards-size for shard not present in existingShards.
// It also verify that all shards in existingShards actually exists.
func (c *CassandraIndex) verifyMissingShard(
	ctx context.Context,
	w io.Writer,
	doFix bool,
) (errorCount int, shards *roaring.Bitmap, err error) {
	shards, err = c.postings(ctx, []int32{globalShardNumber}, existingShardsLabel, existingShardsLabel, false)
	if err != nil {
		return 0, shards, fmt.Errorf("get postings for existing shards: %w", err)
	}

	current := time.Now().Add(3 * postingShardSize)

	for n := 0; n < 100; n++ {
		if ctx.Err() != nil {
			return 0, shards, ctx.Err()
		}

		queryShard := []int32{ShardForTime(current.Unix())}

		it, err := c.postings(ctx, queryShard, maybePostingLabel, maybePostingLabel, false)
		if err != nil {
			return 0, shards, err
		}

		if it != nil && it.Any() && !shards.Contains(uint64(queryShard[0])) {
			errorCount++

			fmt.Fprintf(w, "Shard %d for time %v isn't in all shards", queryShard[0], current.String())

			if doFix {
				_, err = shards.AddN(uint64(queryShard[0]))
				if err != nil {
					return 0, shards, fmt.Errorf("update bitmap: %w", err)
				}
			}
		}
	}

	slice := shards.Slice()
	for _, shard := range slice {
		if ctx.Err() != nil {
			return 0, shards, ctx.Err()
		}

		shard := int32(shard)

		it, err := c.postings(ctx, []int32{shard}, maybePostingLabel, maybePostingLabel, false)
		if err != nil {
			return 0, shards, fmt.Errorf("get postings for maybe metric IDs: %w", err)
		}

		if it == nil || !it.Any() {
			errorCount++

			fmt.Fprintf(w, "Shard %d is listed in all shards but don't exists\n", shard)

			if doFix {
				_, err = shards.RemoveN(uint64(shard))
				if err != nil {
					return 0, shards, fmt.Errorf("update bitmap: %w", err)
				}
			}
		}
	}

	if errorCount > 0 && doFix {
		var buffer bytes.Buffer

		_, err = shards.WriteTo(&buffer)

		if err != nil {
			return errorCount, shards, fmt.Errorf("serialize bitmap: %w", err)
		}

		err = c.store.InsertPostings(ctx, globalShardNumber, existingShardsLabel, existingShardsLabel, buffer.Bytes())
		if err != nil {
			return errorCount, shards, fmt.Errorf("update existing shards: %w", err)
		}
	}

	return errorCount, shards, nil
}

// check that given metric IDs existing in labels2id and id2labels.
func (c *CassandraIndex) verifyBulk(
	ctx context.Context,
	now time.Time,
	w io.Writer,
	doFix bool,
	ids []types.MetricID,
	bulkDeleter *deleter,
	allPosting *roaring.Bitmap,
	allGoodIds *roaring.Bitmap,
) (err error) {
	id2Labels, id2expiration, err := c.selectIDS2LabelsAndExpiration(ctx, ids)
	if err != nil {
		return fmt.Errorf("get labels: %w", err)
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

		allLabelsString = append(allLabelsString, lbls.String())

		_, ok = id2expiration[id]
		if !ok {
			msg := "ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify"

			return fmt.Errorf(msg, id, lbls.String())
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	labels2ID, err := c.selectLabelsList2ID(ctx, allLabelsString)
	if err != nil {
		return fmt.Errorf("get labels2ID: %w", err)
	}

	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
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

		if id != id2 { //nolint:nestif
			tmp, tmp2, err := c.selectIDS2LabelsAndExpiration(ctx, []types.MetricID{id2})
			if err != nil {
				return fmt.Errorf("get labels from store: %w", err)
			}

			lbls2 := tmp[id2]
			expiration2, ok := tmp2[id2]

			if !ok && lbls2 != nil {
				msg := "ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify"

				return fmt.Errorf(msg, id2, lbls2.String())
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

		if now.After(expiration.Add(24 * time.Hour)) {
			fmt.Fprintf(w, "ID %10d (%v) should have expired on %v\n", id, lbls.String(), expiration)

			if doFix {
				bulkDeleter.PrepareDelete(id, lbls, false)
			}

			continue
		}

		_, err = allGoodIds.AddN(uint64(id))
		if err != nil {
			return fmt.Errorf("update bitmap: %w", err)
		}
	}

	return nil
}

// check that postings for given shard is consistent.
func (c *CassandraIndex) verifyShard( //nolint:maintidx
	ctx context.Context,
	w io.Writer,
	doFix bool,
	shard int32,
	allGoodIds *roaring.Bitmap,
) (hadIssue bool, err error) {
	updates := make([]postingUpdateRequest, 0)
	labelToIndex := make(map[labels.Label]int)

	localAll, err := c.postings(ctx, []int32{shard}, allPostingLabel, allPostingLabel, false)
	if err != nil {
		return false, err
	}

	localMaybe, err := c.postings(ctx, []int32{shard}, allPostingLabel, allPostingLabel, false)
	if err != nil {
		return false, err
	}

	if !localAll.Any() {
		hadIssue = true

		fmt.Fprintf(
			w,
			"shard %d is empty (automatic cleanup may fix this)!\n",
			shard,
		)
	}

	if !localMaybe.Any() {
		hadIssue = true

		fmt.Fprintf(
			w,
			"shard %d is empty!\n",
			shard,
		)
	}

	tmp := localAll.Difference(localMaybe)
	it := tmp.Iterator()

	for {
		id, eof := it.Next()
		if eof {
			break
		}

		hadIssue = true

		fmt.Fprintf(
			w,
			"shard %d: ID %d is present in localAll but not in localMaybe!\n",
			shard,
			id,
		)

		if doFix {
			lbl := labels.Label{
				Name:  maybePostingLabel,
				Value: maybePostingLabel,
			}

			idx, ok := labelToIndex[lbl]
			if !ok {
				idx = len(updates)
				updates = append(updates, postingUpdateRequest{
					Label: lbl,
					Shard: shard,
				})
				labelToIndex[lbl] = idx
			}

			updates[idx].AddIDs = append(updates[idx].AddIDs, id)
		}
	}

	tmp = localMaybe.Difference(localAll)
	it = tmp.Iterator()

	for {
		id, eof := it.Next()
		if eof {
			break
		}

		hadIssue = true

		fmt.Fprintf(
			w,
			"shard %d: ID %d is present in localMaybe but not in localAll (automatic cleanup may fix this)!\n",
			shard,
			id,
		)

		if doFix {
			lbl := labels.Label{
				Name:  maybePostingLabel,
				Value: maybePostingLabel,
			}

			idx, ok := labelToIndex[lbl]
			if !ok {
				idx = len(updates)
				updates = append(updates, postingUpdateRequest{
					Label: lbl,
					Shard: shard,
				})
				labelToIndex[lbl] = idx
			}

			updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, id)
		}
	}

	wantedPostings := make(map[labels.Label]*roaring.Bitmap)
	labelNames := make(map[string]interface{})
	it = localAll.Iterator()

	pendingIds := make([]types.MetricID, 0, 10000)

	for ctx.Err() == nil {
		pendingIds = pendingIds[:0]

		for ctx.Err() == nil {
			id, eof := it.Next()
			if eof {
				break
			}

			pendingIds = append(pendingIds, types.MetricID(id))
			if len(pendingIds) > 1000 {
				break
			}
		}

		if len(pendingIds) == 0 {
			break
		}

		tmp, _, err := c.selectIDS2LabelsAndExpiration(ctx, pendingIds)
		if err != nil {
			return true, fmt.Errorf("get labels: %w", err)
		}

		for id, lbls := range tmp {
			for _, lbl := range lbls {
				labelNames[lbl.Name] = nil

				bitset := wantedPostings[lbl]
				if bitset == nil {
					bitset = roaring.NewBTreeBitmap()
				}

				_, err = bitset.AddN(uint64(id))
				if err != nil {
					return true, fmt.Errorf("update bitmap: %w", err)
				}

				wantedPostings[lbl] = bitset

				lbl2 := labels.Label{
					Name:  postinglabelName,
					Value: lbl.Name,
				}

				bitset = wantedPostings[lbl2]
				if bitset == nil {
					bitset = roaring.NewBTreeBitmap()
				}

				_, err = bitset.AddN(uint64(id))
				if err != nil {
					return true, fmt.Errorf("update bitmap: %w", err)
				}

				wantedPostings[lbl2] = bitset
			}
		}
	}

	references := []struct {
		it   *roaring.Bitmap
		name string
	}{
		{name: "global all IDs", it: allGoodIds},
		{name: "shard all IDs", it: localAll},
		{name: "shard maybe present IDs", it: localMaybe},
	}

	labelNames[postinglabelName] = true

	for name := range labelNames {
		if ctx.Err() != nil {
			break
		}

		iter := c.store.SelectPostingByName(ctx, shard, name)
		for iter.HasNext() {
			tmp := roaring.NewBTreeBitmap()
			labelValue, buffer := iter.Next()

			err := tmp.UnmarshalBinary(buffer)
			if err != nil {
				return hadIssue, fmt.Errorf("unmarshal fail: %w", err)
			}

			lbl := labels.Label{
				Name:  name,
				Value: labelValue,
			}

			wanted := wantedPostings[lbl]
			if wanted == nil { //nolint:nestif
				hadIssue = true

				fmt.Fprintf(
					w,
					"shard %d: extra posting for %s=%s exists (with %d IDs)\n",
					shard,
					name,
					labelValue,
					tmp.Count(),
				)

				if doFix {
					idx, ok := labelToIndex[lbl]
					if !ok {
						idx = len(updates)
						updates = append(updates, postingUpdateRequest{
							Label: lbl,
							Shard: shard,
						})
						labelToIndex[lbl] = idx
					}

					updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, tmp.Slice()...)
				}
			} else {
				delete(wantedPostings, lbl)

				tmp2 := wanted.Difference(tmp)
				it := tmp2.Iterator()
				for {
					id, eof := it.Next()
					if eof {
						break
					}

					hadIssue = true

					fmt.Fprintf(
						w,
						"shard %d: missing ID %d in posting for %s=%s\n",
						shard,
						id,
						name,
						labelValue,
					)

					if doFix {
						idx, ok := labelToIndex[lbl]
						if !ok {
							idx = len(updates)
							updates = append(updates, postingUpdateRequest{
								Label: lbl,
								Shard: shard,
							})
							labelToIndex[lbl] = idx
						}

						updates[idx].AddIDs = append(updates[idx].AddIDs, id)
					}
				}

				tmp2 = tmp.Difference(wanted)
				it = tmp2.Iterator()
				for {
					id, eof := it.Next()
					if eof {
						break
					}

					hadIssue = true

					fmt.Fprintf(
						w,
						"shard %d: extra ID %d in posting for %s=%s (present in maybe=%v allId=%v globalAll=%v)\n",
						shard,
						id,
						name,
						labelValue,
						localMaybe.Contains(id),
						localAll.Contains(id),
						allGoodIds.Contains(id),
					)

					if doFix {
						idx, ok := labelToIndex[lbl]
						if !ok {
							idx = len(updates)
							updates = append(updates, postingUpdateRequest{
								Label: lbl,
								Shard: shard,
							})
							labelToIndex[lbl] = idx
						}

						updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, id)
					}
				}
			}

			for _, reference := range references {
				tmp = tmp.Difference(reference.it)
				it := tmp.Iterator()

				for ctx.Err() == nil {
					id, eof := it.Next()
					if eof {
						break
					}

					hadIssue = true

					fmt.Fprintf(
						w,
						"shard %d: posting for %s=%s has ID %d which is not in %s!\n",
						shard,
						name,
						labelValue,
						id,
						reference.name,
					)
				}
			}
		}
	}

	for lbl, wantValue := range wantedPostings {
		hadIssue = true

		fmt.Fprintf(
			w,
			"shard %d: posting %s=%s was expected to exists\n",
			shard,
			lbl.Name,
			lbl.Value,
		)

		if doFix {
			idx, ok := labelToIndex[lbl]
			if !ok {
				idx = len(updates)
				updates = append(updates, postingUpdateRequest{
					Label: lbl,
					Shard: shard,
				})
				labelToIndex[lbl] = idx
			}

			updates[idx].AddIDs = append(updates[idx].AddIDs, wantValue.Slice()...)
		}
	}

	if doFix && len(updates) > 0 {
		err = c.concurrentTasks(
			ctx,
			concurrentInsert,
			func(ctx context.Context, work chan<- func() error) error {
				for _, req := range updates {
					req := req
					task := func() error {
						_, err := c.postingUpdate(ctx, req)

						if errors.Is(err, errBitmapEmpty) {
							err = nil
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
			},
		)
		if err != nil {
			return hadIssue, err
		}
	}

	return hadIssue, nil
}

// AllIDs returns all ids stored in the index.
func (c *CassandraIndex) AllIDs(ctx context.Context, start time.Time, end time.Time) ([]types.MetricID, error) {
	shards, err := c.getTimeShards(ctx, start, end, false)
	if err != nil {
		return nil, err
	}

	bitmap, err := c.postings(ctx, shards, allPostingLabel, allPostingLabel, false)
	if err != nil {
		return nil, err
	}

	return bitsetToIDs(bitmap), nil
}

// postings return ids matching give Label name & value
// If value is the empty string, it match any values (but the label must be set).
func (c *CassandraIndex) postings(
	ctx context.Context,
	shards []int32,
	name string,
	value string,
	useCache bool,
) (*roaring.Bitmap, error) {
	if name == allPostingLabel {
		value = allPostingLabel
	}

	if name == globalAllPostingLabel {
		value = globalAllPostingLabel
	}

	var result *roaring.Bitmap

	cloneDone := false

	for _, shard := range shards {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var tmp *roaring.Bitmap

		if useCache {
			tmp = c.postingsCache.Get(shard, name, value)

			status := "miss"
			if tmp != nil {
				status = "hit"
			}

			c.metrics.CacheAccess.WithLabelValues("postings", status).Inc()
		}

		if tmp == nil {
			tmp = roaring.NewBTreeBitmap()

			buffer, err := c.store.SelectPostingByNameValue(ctx, shard, name, value)

			if errors.Is(err, gocql.ErrNotFound) {
				err = nil
			} else if err == nil {
				err = tmp.UnmarshalBinary(buffer)
			}

			if err != nil {
				return nil, fmt.Errorf("unmarshal fail: %w", err)
			}

			if useCache {
				size := c.postingsCache.Set(shard, name, value, tmp)

				c.metrics.CacheSize.WithLabelValues("postings").Set(float64(size))
			}
		}

		if result == nil {
			result = tmp
		} else {
			if useCache && !cloneDone {
				cloneDone = true
				result = result.Union(tmp)
			} else {
				result.UnionInPlace(tmp)
			}
		}
	}

	if result == nil {
		result = roaring.NewBTreeBitmap()
	}

	return result, nil
}

func (c *CassandraIndex) lookupLabels(
	ctx context.Context,
	ids []types.MetricID,
	now time.Time,
) ([]labels.Labels, error) {
	start := time.Now()

	miss := make([]bool, len(ids))
	idToQuery := make([]types.MetricID, 0)

	labelList := c.idsToLabels.MGet(now, ids)
	for i, lbls := range labelList {
		if lbls == nil {
			idToQuery = append(idToQuery, ids[i])
			miss[i] = true
		}
	}

	c.metrics.CacheAccess.WithLabelValues("lookup-labels", "miss").Add(float64(len(idToQuery)))
	c.metrics.CacheAccess.WithLabelValues("lookup-labels", "hit").Add(float64(len(ids) - len(idToQuery)))

	if len(idToQuery) > 0 {
		idToLabels, idToExpiration, err := c.selectIDS2LabelsAndExpiration(ctx, idToQuery)
		if err != nil {
			c.metrics.LookupLabelsSeconds.Observe(time.Since(start).Seconds())

			return nil, fmt.Errorf("get labels from store: %w", err)
		}

		for i, id := range ids {
			if miss[i] {
				var ok bool

				labelList[i], ok = idToLabels[id]
				if !ok {
					return nil, fmt.Errorf("labels for metric ID %d not found", id)
				}

				size := c.idsToLabels.Set(now, id, labelList[i], idToExpiration[id])

				c.metrics.CacheSize.WithLabelValues("lookup-labels").Set(float64(size))
			}
		}
	}

	c.metrics.LookupLabelsSeconds.Observe(time.Since(start).Seconds())

	return labelList, nil
}

// LabelValues returns potential values for a label name. Values will have at least
// one metrics matching matchers.
// Typical matchers will filter by tenant ID, to get all values for one tenant.
// It is not safe to use the strings beyond the lifefime of the querier.
func (c *CassandraIndex) LabelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	if name == "" || strings.Contains(name, "|") {
		return nil, fmt.Errorf("invalid label name \"%s\"", name)
	}

	return c.labelValues(ctx, start, end, name, matchers)
}

// LabelNames returns the unique label names for metrics matching matchers in sorted order.
// Typical matchers will filter by tenant ID, to get all values for one tenant.
func (c *CassandraIndex) LabelNames(
	ctx context.Context,
	start, end time.Time,
	matchers []*labels.Matcher,
) ([]string, error) {
	return c.labelValues(ctx, start, end, postinglabelName, matchers)
}

func (c *CassandraIndex) labelValues(
	ctx context.Context,
	start, end time.Time,
	name string,
	matchers []*labels.Matcher,
) ([]string, error) {
	shards, err := c.getTimeShards(ctx, start, end, false)
	if err != nil {
		return nil, err
	}

	bitmap, _, err := c.postingsForMatchers(ctx, shards, matchers, 0)
	if err != nil {
		return nil, err
	}

	var (
		results []string
		work    []string
	)

	for _, shard := range shards {
		if work != nil {
			work = work[:0]
		}

		iter := c.store.SelectPostingByName(ctx, shard, name)

		for iter.HasNext() {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			tmp := roaring.NewBTreeBitmap()
			value, buffer := iter.Next()

			err := tmp.UnmarshalBinary(buffer)
			if err != nil {
				return nil, fmt.Errorf("unmarshal fail: %w", err)
			}

			if len(matchers) == 0 || tmp.IntersectionCount(bitmap) > 0 {
				work = append(work, value)
			}
		}

		if results == nil {
			results = work
			work = nil
		} else if len(work) > 0 {
			results = mergeSorted(results, work)
		}

		if iter.Err() != nil {
			return nil, iter.Err()
		}
	}

	return results, nil
}

func mergeSorted(left, right []string) (result []string) {
	result = make([]string, len(left)+len(right))

	i := 0

	for len(left) > 0 && len(right) > 0 {
		switch {
		case left[0] < right[0]:
			result[i] = left[0]
			left = left[1:]
		case left[0] != right[0]:
			result[i] = right[0]
			right = right[1:]
		default:
			right = right[1:]

			continue
		}
		i++
	}

	for j := 0; j < len(left); j++ {
		result[i] = left[j]
		i++
	}

	for j := 0; j < len(right); j++ {
		result[i] = right[j]
		i++
	}

	return result[:i]
}

// LookupIDs returns a IDs corresponding to the specified labels.Label lists
// It also return the metric TTLs
// The result list will be the same length as input lists and using the same order.
// The input may be mutated (the labels list), so reusing it would be avoided.
func (c *CassandraIndex) LookupIDs(
	ctx context.Context,
	requests []types.LookupRequest,
) ([]types.MetricID, []int64, error) {
	return c.lookupIDs(ctx, requests, time.Now())
}

func (c *CassandraIndex) lookupIDs(
	ctx context.Context,
	requests []types.LookupRequest,
	now time.Time,
) ([]types.MetricID, []int64, error) {
	start := time.Now()

	defer func() {
		c.metrics.LookupIDRequestSeconds.Observe(time.Since(start).Seconds())
	}()

	for _, req := range requests {
		if len(req.Labels) == 0 {
			return nil, nil, errors.New("empty labels set")
		}
	}

	entries, labelsToIndices, err := c.lookupIDsFromCache(ctx, now, requests)
	if err != nil {
		return nil, nil, err
	}

	labelsToQuery := make([]string, 0, len(requests))

	miss := 0

	for lbls, indicies := range labelsToIndices {
		miss += len(indicies)

		labelsToQuery = append(labelsToQuery, lbls)
	}

	c.metrics.CacheAccess.WithLabelValues("lookup-id", "hit").Add(float64(len(requests) - miss))
	c.metrics.CacheAccess.WithLabelValues("lookup-id", "miss").Add(float64(miss))

	if len(labelsToQuery) > 0 {
		labels2ID, err := c.selectLabelsList2ID(ctx, labelsToQuery)
		if err != nil {
			return nil, nil, fmt.Errorf("searching metric failed: %w", err)
		}

		idsToQuery := make([]types.MetricID, 0, len(labelsToQuery))

		for _, id := range labels2ID {
			idsToQuery = append(idsToQuery, id)
		}

		_, ids2Expiration, err := c.selectIDS2LabelsAndExpiration(ctx, idsToQuery)
		if err != nil {
			return nil, nil, fmt.Errorf("searching metric failed: %w", err)
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

	c.metrics.LookupIDNew.Add(float64(notFoundCount))

	if notFoundCount > 0 { //nolint:nestif
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

		done, err := c.createMetrics(ctx, now, pending, false)

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
		jitterDur := time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second //nolint:gosec
		cassandraExpiration = cassandraExpiration.Add(jitterDur)
		// The 3*backgroundCheckInterval is to be slightly larger than 2*backgroundCheckInterval used in lookupIDsFromCache.
		// It ensure that live metrics stay in cache.
		needTTLUpdate := entry.idData.cassandraEntryExpiration.Before(wantedEntryExpiration) ||
			entry.idData.cassandraEntryExpiration.Before(now.Add(3*backgroundCheckInterval))

		if needTTLUpdate {
			if err := c.refreshExpiration(ctx, entry.id, entry.cassandraEntryExpiration, cassandraExpiration); err != nil {
				return nil, nil, err
			}

			entry.cassandraEntryExpiration = cassandraExpiration
		}

		entry.cacheExpirationTime = now.Add(cacheExpirationDelay)
		c.setIDData(entry.labelsKey, entry.idData)
	}

	return ids, ttls, nil
}

func (c *CassandraIndex) lookupIDsFromCache(
	ctx context.Context,
	now time.Time,
	requests []types.LookupRequest,
) (entries []lookupEntry, labelsToIndices map[string][]int, err error) {
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
			cassandraExpiration = cassandraExpiration.Add(
				time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second, //nolint:gosec
			)

			if err := c.refreshExpiration(ctx, data.id, data.cassandraEntryExpiration, cassandraExpiration); err != nil {
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

		shards, err := c.getTimeShards(ctx, req.Start, req.End, true)
		if err != nil {
			return nil, nil, err
		}

		entries[i] = lookupEntry{
			idData:       data,
			labelsKey:    labelsKey,
			ttl:          ttl,
			wantedShards: shards,
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

func (c *CassandraIndex) refreshExpiration(
	ctx context.Context,
	id types.MetricID,
	oldExpiration, newExpiration time.Time,
) error {
	c.metrics.LookupIDRefresh.Inc()

	err := c.store.UpdateID2LabelsExpiration(ctx, id, newExpiration)
	if err != nil {
		return fmt.Errorf("update expiration: %w", err)
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

// Search a free ID using dichotomy.
func findFreeID(bitmap *roaring.Bitmap) uint64 {
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
	highIdx := max + 1

	for highIdx-lowIdx > 32768 {
		pivot := lowIdx + (highIdx-lowIdx)/2

		countIfFull := pivot - lowIdx
		if bitmap.CountRange(lowIdx, pivot) >= countIfFull {
			lowIdx = pivot
		} else {
			highIdx = pivot
		}
	}

	freemap := roaring.NewBTreeBitmap()
	freemap = freemap.Flip(lowIdx, highIdx)
	freemap = freemap.Xor(bitmap)

	results := freemap.SliceRange(lowIdx, highIdx)

	// becaus Flip produce a broken NewBitmap which is known to have bugs (on update only it seems)
	// So we will double-check that the result is indeed unused to catch possible bugs
	if len(results) == 0 || bitmap.Contains(results[0]) {
		if max < math.MaxUint64 {
			return max + 1
		}

		return 0
	}

	return results[0]
}

type lookupEntry struct {
	sortedLabelsString string
	sortedLabels       labels.Labels
	wantedShards       []int32
	idData
	labelsKey uint64
	ttl       int64
}

// createMetrics creates a new metric IDs associated with provided request
// The lock is assumed to be held.
// Some care should be taken to avoid assigned the same ID from two SquirrelDB instance, so:
//
// * To avoid race-condition, redo a check that metrics is not yet registered now that lock is acquired
// * Read the all-metric postings. From there we find a free ID
// * Update Cassandra tables to store this new metrics. The insertion is done in the following order:
//   * First an entry is added to the expiration table. This ensure that in case of crash in this process,
//     the ID will eventually be freed.
//   * Then it update:
//     * the all-metric postings. This effectively reseve the ID.
//     * the id2labels tables (it give informations needed to cleanup other postings)
//   * finally insert in labels2id, which is done as last because it's this table that determine that
//     a metrics didn't exists
//
// If the above process crash and partially write some value, it still in a good state because:
// * For the insertion, it's the last entry (in labels2id) that matter.
//   The creation will be retried when point is retried
// * For reading, even if the metric ID may match search (as soon as it's in some posting, it may happen),
//   since no data points could be wrote and empty result are stipped, they won't be in results
// * For writing using __metric_id__ labels, it may indeed success if the partial write reacher id2labels...
//   BUT to have the metric ID, client must first do a succesfull read to get the ID. So this shouldn't happen.
//
// The expiration tables is used to known which metrics are likely to expire on a give date.
// They are grouped by day (that is, the tables contains on row per day, each row being the day
// and the list of metric IDs that may expire on this day).
// A background process will process each past day from this tables and for each metrics:
// * Check if the metrics is actually expired. It may not be the case, if the metrics continued to get points.
//   It does this check using a field of the table id2labels which is refreshed.
// * If expired, delete entry for this metric from the index (the opposite of creation)
// * Of not expired, add the metric IDs to the new expiration day in the table.
// * Once finished, delete the processed day.
func (c *CassandraIndex) createMetrics(
	ctx context.Context,
	now time.Time,
	pending []lookupEntry,
	allowForcingIDAndExpiration bool,
) ([]lookupEntry, error) {
	start := time.Now()
	expirationUpdateRequests := make(map[time.Time]expirationUpdateRequest)

	defer func() {
		c.metrics.CreateMetricSeconds.Observe(time.Since(start).Seconds())
	}()

	allPosting, err := c.postings(ctx, []int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel, false)
	if err != nil {
		return nil, err
	}

	labelsToQuery := make([]string, len(pending))

	for i, entry := range pending {
		labelsToQuery[i] = entry.sortedLabelsString
	}

	// Be sure no-one registered the metric before we took the lock.
	labels2ID, err := c.selectLabelsList2ID(ctx, labelsToQuery)
	if err != nil {
		return nil, fmt.Errorf("check IDs from store: %w", err)
	}

	for i, entry := range pending {
		var newID types.MetricID

		id, ok := labels2ID[entry.sortedLabelsString]

		switch {
		case ok:
			newID = id

			c.metrics.LookupIDConcurrentNew.Inc()
		case entry.id != 0 && allowForcingIDAndExpiration:
			// This case is only used during test, where we want to force ID value
			newID = entry.id
		default:
			newID = types.MetricID(findFreeID(allPosting))
		}

		if newID == 0 {
			return nil, errors.New("too many metrics registered, unable to find a free ID")
		}

		_, err = allPosting.Add(uint64(newID))
		if err != nil {
			return nil, fmt.Errorf("update bitmap: %w", err)
		}

		wantedEntryExpiration := now.Add(time.Duration(entry.ttl) * time.Second)
		cassandraExpiration := wantedEntryExpiration.Add(cassandraTTLUpdateDelay)
		cassandraExpiration = cassandraExpiration.Add(
			time.Duration(rand.Float64()*cassandraTTLUpdateJitter.Seconds()) * time.Second, //nolint:gosec
		)

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

		err = c.expirationUpdate(ctx, req)
		if err != nil {
			return nil, err
		}
	}

	var buffer bytes.Buffer

	_, err = allPosting.WriteTo(&buffer)

	if err != nil {
		return nil, fmt.Errorf("serialize bitmap: %w", err)
	}

	err = c.store.InsertPostings(ctx, globalShardNumber, globalAllPostingLabel, globalAllPostingLabel, buffer.Bytes())
	if err != nil {
		return nil, fmt.Errorf("update used metric IDs: %w", err)
	}

	err = c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, entry := range pending {
				entry := entry
				task := func() error {
					return c.store.InsertID2Labels(ctx, entry.id, entry.sortedLabels, entry.cassandraEntryExpiration)
				}
				select {
				case work <- task:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	err = c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, entry := range pending {
				entry := entry
				task := func() error {
					return c.store.InsertLabels2ID(ctx, entry.sortedLabelsString, entry.id)
				}
				select {
				case work <- task:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return pending, nil
}

// updatePostingShards update sharded-posting.
//
// It works with the following way:
// * if updateCache is true, gather all shard impacted and refresh the idInShard cache
//   (avoid re-use issue after metric creation)
// * For each update in a shard, if the ID is already present, skip it
// * Then update list of existings shards
// * Add ID to maybe-present posting list (this is used in cleanup)
// * Add ID to postings
// * Add ID to presence list per shard
//
// This insertion order all to recover some failure:
// * No write will happen because ID is inserted to the presence list. Meaning
//   reads won't get inconsistent result.
//   It also means that retry of write will fix the issue,
// * The insert in maybe-present allow cleanup to known if an ID (may) need cleanup in the shard.
func (c *CassandraIndex) updatePostingShards(
	ctx context.Context,
	pending []lookupEntry,
	updateCache bool,
) error {
	start := time.Now()

	defer func() {
		c.metrics.UpdatePostingSeconds.Observe(time.Since(start).Seconds())
	}()

	if updateCache {
		shardAffected := make(map[int32]bool)

		for _, entry := range pending {
			for _, shard := range entry.wantedShards {
				shardAffected[shard] = true
			}
		}

		err := c.refreshPostingIDInShard(ctx, shardAffected)
		if err != nil {
			return err
		}
	}

	maybePresent := make(map[int32]postingUpdateRequest)
	precense := make(map[int32]postingUpdateRequest)
	updates := make([]postingUpdateRequest, 0)
	shardToLabelToIndex := make(map[int32]map[labels.Label]int)
	now := time.Now()
	keysToInvalidate := make([]postingsCacheKey, 0)

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

			labelsList := make(labels.Labels, 0, len(entry.unsortedLabels)*2)
			labelsList = append(labelsList, entry.unsortedLabels...)

			for _, lbl := range entry.unsortedLabels {
				labelsList = append(labelsList, labels.Label{
					Name:  postinglabelName,
					Value: lbl.Name,
				})

				keysToInvalidate = append(keysToInvalidate, postingsCacheKey{
					Shard: shard,
					Name:  lbl.Name,
					Value: lbl.Value,
				})
			}

			for _, lbl := range labelsList {
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

	c.invalidatePostings(ctx, keysToInvalidate)

	if len(maybePresent) > 0 {
		if ok := c.newMetricLock.TryLock(ctx, 15*time.Second); !ok {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return errors.New("newMetricLock is not acquired")
		}

		err := c.applyUpdatePostingShards(ctx, maybePresent, updates, precense)

		c.newMetricLock.Unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CassandraIndex) invalidatePostingsListenner(message []byte) {
	var keys []postingsCacheKey

	dec := gob.NewDecoder(bytes.NewReader(message))

	if err := dec.Decode(&keys); err != nil {
		c.logger.Err(err).Msg("Unable to deserialize new metrics message. Search cache may be wrong.")
	} else {
		size := c.postingsCache.Invalidate(keys)
		c.metrics.CacheSize.WithLabelValues("postings").Set(float64(size))
	}
}

func (c *CassandraIndex) invalidatePostings(ctx context.Context, entries []postingsCacheKey) {
	buffer := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buffer)

	if err := enc.Encode(entries); err != nil {
		c.logger.Err(err).Msg("Unable to serialize new metrics. Cache invalidation on other name may fail.")
	} else {
		err := c.options.Cluster.Publish(ctx, clusterChannelPostingInvalidate, buffer.Bytes())
		if err != nil {
			c.logger.Err(err).Msg("Unable to send message for new metrics to other node. Their cache won't be invalidated.")
		}
	}
}

func (c *CassandraIndex) refreshPostingIDInShard(ctx context.Context, shards map[int32]bool) error {
	return c.concurrentTasks(
		ctx,
		concurrentPostingRead,
		func(ctx context.Context, work chan<- func() error) error {
			for shard := range shards {
				shard := shard
				task := func() error {
					tmp, err := c.postings(ctx, []int32{shard}, allPostingLabel, allPostingLabel, false)
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

func (c *CassandraIndex) applyUpdatePostingShards(
	ctx context.Context,
	maybePresent map[int32]postingUpdateRequest,
	updates []postingUpdateRequest,
	precense map[int32]postingUpdateRequest,
) error {
	newShard := make([]uint64, 0, len(maybePresent))

	for shard := range maybePresent {
		newShard = append(newShard, uint64(shard))
	}

	_, err := c.postingUpdate(ctx, postingUpdateRequest{
		Shard: globalShardNumber,
		Label: labels.Label{
			Name:  existingShardsLabel,
			Value: existingShardsLabel,
		},
		AddIDs: newShard,
	})
	if err != nil && !errors.Is(err, errBitmapEmpty) {
		return err
	}

	c.lookupIDMutex.Lock()
	if c.existingShards == nil {
		c.existingShards = roaring.NewBTreeBitmap()
	}

	_, err = c.existingShards.AddN(newShard...)
	c.lookupIDMutex.Unlock()

	if err != nil {
		return fmt.Errorf("update bitmap: %w", err)
	}

	err = c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, req := range maybePresent {
				req := req
				task := func() error {
					_, err := c.postingUpdate(ctx, req)

					if errors.Is(err, errBitmapEmpty) {
						err = nil
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
		},
	)
	if err != nil {
		return err
	}

	err = c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, req := range updates {
				req := req
				task := func() error {
					_, err := c.postingUpdate(ctx, req)

					if errors.Is(err, errBitmapEmpty) {
						err = nil
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
		},
	)
	if err != nil {
		return err
	}

	err = c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, req := range precense {
				req := req
				task := func() error {
					bitmap, err := c.postingUpdate(ctx, req)
					if err == nil {
						c.lookupIDMutex.Lock()
						c.idInShard[req.Shard] = bitmap
						c.lookupIDMutex.Unlock()
					}

					if errors.Is(err, errBitmapEmpty) {
						c.lookupIDMutex.Lock()
						delete(c.idInShard, req.Shard)
						delete(c.idInShardLastAccess, req.Shard)
						c.lookupIDMutex.Unlock()

						err = nil
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
		},
	)
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
// In those case, it use the ability of our posting table to query for metric ID that has a LabelName regardless of
// the values.
// * For label must be defined, it increament the number of Matcher satified if the metric has the label.
//   In principle it's the same as if it expanded it to all possible values (e.g. with name!="" it avoids expanding
//   to name="memory" || name="disk" and directly ask for name=*)
// * For label must NOT be defined, it query for all metric IDs that has this label, then increments the number of
//   Matcher satified if currently found metrics are not in the list of metrics having this label.
//   Note: this means that it must already have found some metrics (and that this filter is applied at the end)
//   but PromQL forbid to only have label-not-defined matcher, so some other matcher must exists.
func (c *CassandraIndex) Search(
	ctx context.Context,
	queryStart, queryEnd time.Time,
	matchers []*labels.Matcher,
) (types.MetricsSet, error) {
	start := time.Now()

	shards, err := c.getTimeShards(ctx, queryStart, queryEnd, false)
	if err != nil {
		return nil, err
	}

	defer func() {
		c.metrics.SearchMetricsSeconds.Observe(time.Since(start).Seconds())
	}()

	if len(matchers) == 0 {
		return nil, nil
	}

	ids, labelsList, err := c.idsForMatchers(ctx, shards, matchers, 3)
	if err != nil {
		return nil, err
	}

	c.metrics.SearchMetrics.Add(float64(len(ids)))

	return &metricsLabels{c: c, ctx: ctx, ids: ids, labelsList: labelsList}, nil
}

type metricsLabels struct {
	ctx        context.Context //nolint:containedctx
	err        error
	c          *CassandraIndex
	ids        []types.MetricID
	labelsList []labels.Labels
	next       int
}

func (l *metricsLabels) Next() bool {
	if l.next >= len(l.ids) {
		return false
	}

	if l.labelsList == nil {
		l.labelsList, l.err = l.c.lookupLabels(l.ctx, l.ids, time.Now())
		if l.err != nil {
			return false
		}
	}

	l.next++

	return true
}

func (l *metricsLabels) At() types.MetricLabel {
	return types.MetricLabel{
		ID:     l.ids[l.next-1],
		Labels: l.labelsList[l.next-1],
	}
}

func (l *metricsLabels) Err() error {
	return l.err
}

func (l *metricsLabels) Count() int {
	return len(l.ids)
}

// Deletes all expired cache entries.
func (c *CassandraIndex) expire(now time.Time) {
	c.lookupIDMutex.Lock()
	defer c.lookupIDMutex.Unlock()

	size := 0

	for key, idsData := range c.labelsToID {
		size += len(idsData)

		for _, idData := range idsData {
			if idData.cacheExpirationTime.Before(now) {
				// This may delete too many entry, but:
				// 1) normally only 1 entry match the hash
				// 2) it's a cache, we don't loss data
				delete(c.labelsToID, key)

				size -= len(idsData)

				break
			}
		}
	}

	c.metrics.CacheSize.WithLabelValues("lookup-id").Set(float64(size))
}

func (c *CassandraIndex) applyExpirationUpdateRequests(ctx context.Context) {
	c.lookupIDMutex.Lock()

	start := time.Now()

	defer func() {
		c.metrics.ExpirationMoveSeconds.Observe(time.Since(start).Seconds())
	}()

	expireUpdates := make([]expirationUpdateRequest, 0, len(c.expirationUpdateRequests))

	for day, v := range c.expirationUpdateRequests {
		v.Day = day
		expireUpdates = append(expireUpdates, v)
	}

	c.expirationUpdateRequests = make(map[time.Time]expirationUpdateRequest)

	c.lookupIDMutex.Unlock()

	if len(expireUpdates) == 0 {
		return
	}

	for _, req := range expireUpdates {
		c.logger.Debug().Msgf("Updating expiration day %v, add %v, remove %v", req.Day, req.AddIDs, req.RemoveIDs)
	}

	c.newMetricLock.Lock()

	err := c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, req := range expireUpdates {
				req := req
				task := func() error {
					return c.expirationUpdate(ctx, req)
				}
				select {
				case work <- task:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)

	c.newMetricLock.Unlock()

	if err != nil {
		c.logger.Warn().Err(err).Msg("Update of expiration date failed")

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

// InternalUpdatePostingShards update the time-sharded postings for given metrics.
// This is useful during test/benchmark when one want to fill index quickly.
// This assume len(ids) == len(labels) == len(shards).
// Each element in the list match:
// * labels[0] is the labels for ids[0].
// * shards[0] is the list of shards ids[0] is present.
func (c *CassandraIndex) InternalUpdatePostingShards(
	ctx context.Context,
	ids []types.MetricID,
	labelsList []labels.Labels,
	shards [][]int32,
) error {
	reqs := make([]lookupEntry, len(ids))

	for i, id := range ids {
		reqs[i] = lookupEntry{
			idData:       idData{id: id, unsortedLabels: labelsList[i]},
			wantedShards: shards[i],
		}
	}

	return c.updatePostingShards(ctx, reqs, true)
}

// InternalCreateMetric create metrics in the index with ID value forced.
// This should only be used in test & benchmark.
// The following condition on input must be meet:
// * no duplicated metrics
// * no conflict in IDs
// * labels must be sorted
// * len(metrics) == len(ids) == len(expirations).
//
// Note: you can also provide "0" for the IDs and have the index allocate an ID for you.
// But you should only use 0 in such case, because your non-zero ID shouldn't conflict with
// ID assigned by the index.
//
// Finally you can skip posting updates, but in this case you must write at least one
// points for each shards. This case is mostly useful to pre-create metrics in bulk.
//
// The metrics will be added in all shards between start & end.
func (c *CassandraIndex) InternalCreateMetric(
	ctx context.Context,
	start, end time.Time,
	metrics []labels.Labels,
	ids []types.MetricID,
	expirations []time.Time,
	skipPostings bool,
) ([]types.MetricID, error) {
	requests := make([]lookupEntry, len(metrics))

	shards, err := c.getTimeShards(ctx, start, end, true)
	if err != nil {
		return nil, err
	}

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

	if ok := c.newMetricLock.TryLock(ctx, 15*time.Second); !ok {
		return nil, errors.New("newMetricLock is not acquired")
	}

	done, err := c.createMetrics(ctx, time.Now(), requests, true)

	c.newMetricLock.Unlock()

	if err != nil {
		return nil, err
	}

	for i, id := range ids {
		if done[i].id != id && id != 0 {
			return ids, fmt.Errorf("savedIDs=%v didn't match requested id=%v", done[0].id, id)
		}

		ids[i] = done[i].id
	}

	if !skipPostings {
		err = c.updatePostingShards(ctx, done, true)
	}

	return ids, err
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

func (c *CassandraIndex) periodicRefreshIDInShard(ctx context.Context, now time.Time) {
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

	_, err := c.getExistingShards(ctx, true)
	if err != nil {
		c.logger.Debug().Err(err).Msg("Refresh existingsShards failed")
	}

	err = c.refreshPostingIDInShard(ctx, allShard)
	if err != nil {
		c.logger.Debug().Err(err).Msg("Refresh PostingIDInShard failed")
	}
}

func (c *CassandraIndex) getExistingShards(ctx context.Context, forceUpdate bool) (*roaring.Bitmap, error) {
	c.lookupIDMutex.Lock()
	defer c.lookupIDMutex.Unlock()

	if c.existingShards == nil || forceUpdate {
		tmp, err := c.postings(ctx, []int32{globalShardNumber}, existingShardsLabel, existingShardsLabel, false)
		if err != nil {
			return nil, err
		}

		c.existingShards = tmp
	}

	return c.existingShards, nil
}

// cassandraExpire remove all entry in Cassandra that have expired.
func (c *CassandraIndex) cassandraExpire(ctx context.Context, now time.Time) (bool, error) {
	lock := c.options.LockFactory.CreateLock(expireMetricLockName, metricExpiratorLockTimeToLive)
	if acquired := lock.TryLock(ctx, 0); !acquired {
		return false, nil
	}
	defer lock.Unlock()

	start := time.Now()

	defer func() {
		c.metrics.ExpireTotalSeconds.Observe(time.Since(start).Seconds())
	}()

	var lastProcessedDay time.Time

	{
		var fromTimeStr string

		_, err := c.options.States.Read(expireMetricStateName, &fromTimeStr)
		if err != nil {
			return false, fmt.Errorf("unable to get last processed day for metrics expiration: %w", err)
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
			return false, fmt.Errorf("unable to set last processed day for metrics expiration: %w", err)
		}
	}

	candidateDay := lastProcessedDay.Add(24 * time.Hour)

	// Process entries due to expire before yesterday and entries due to expire yesterday with an offset.
	//
	// On the processing of the 29 April 2022:
	// - the most recent day we could process is 28 April 2022, the day must be completely terminated
	// - before the processing, the lastProcessedDay is 27 April 2022
	// - so candidateDay is 28 April 2022
	// - therefore, now.Sub(candidateDay)-24*time.Hour is the time since the 29 April 2022 at 00:00
	// So on this day, with an expirationStartOffset of 6h, the expiration is skipped before 6AM.
	skipOffset := now.Sub(candidateDay)-24*time.Hour < expirationStartOffset
	if skipOffset {
		return false, nil
	}

	// We don't need the newMetricLockName lock here, because newly created metrics
	// won't be added in candidateDay (which is in the past).
	bitmap, err := c.cassandraGetExpirationList(ctx, candidateDay)
	if err != nil {
		return false, fmt.Errorf("unable to get list of metrics to check for expiration: %w", err)
	}

	c.logger.Debug().Msgf("Processing expiration for day %v", candidateDay)

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

		if err := c.cassandraCheckExpire(ctx, results, now); err != nil {
			return false, fmt.Errorf("unable to perform expiration check of metrics: %w", err)
		}

		_, err = bitmap.Remove(results...)
		if err != nil {
			return false, fmt.Errorf("unable to update list of metrics to check for expiration: %w", err)
		}

		if !bitmap.Any() {
			break
		}

		buffer.Reset()

		_, err = bitmap.WriteTo(&buffer)
		if err != nil {
			return false, fmt.Errorf("unable to update list of metrics to check for expiration: %w", err)
		}

		err = c.store.InsertExpiration(ctx, candidateDay, buffer.Bytes())
		if err != nil {
			return false, fmt.Errorf("unable to update list of metrics to check for expiration: %w", err)
		}
	}

	err = c.store.DeleteExpiration(ctx, candidateDay)
	if err != nil && !errors.Is(err, gocql.ErrNotFound) {
		return false, fmt.Errorf("unable to remove processed list of metrics to check for expiration: %w", err)
	}

	err = c.options.States.Write(expireMetricStateName, candidateDay.Format(time.RFC3339))
	if err != nil {
		return false, fmt.Errorf("unable to set last processed day for metrics expiration: %w", err)
	}

	return true, nil
}

// cassandraCheckExpire actually check for metric expired or not, and perform changes.
// It assume that Cassandra lock expireMetricLockName is taken
//
// This method perform changes at the end, because changes will require the Cassandra lock newMetricLockName,
// and we want to hold this lock only a very short time.
//
// This purge will also remove entry from the in-memory cache.
func (c *CassandraIndex) cassandraCheckExpire(ctx context.Context, ids []uint64, now time.Time) error {
	var expireUpdates []expirationUpdateRequest

	dayToExpireUpdates := make(map[time.Time]int)
	bulkDelete := newBulkDeleter(c)

	metricIDs := make([]types.MetricID, len(ids))
	for i, intID := range ids {
		metricIDs[i] = types.MetricID(intID)
	}

	idToLabels, expires, err := c.selectIDS2LabelsAndExpiration(ctx, metricIDs)
	if err != nil {
		return fmt.Errorf("get expiration from store : %w", err)
	}

	for _, id := range metricIDs {
		expire, ok := expires[id]
		if !ok {
			// This shouldn't happen. It means that metric were partially created.
			// Cleanup this metric from all posting if ever it's present in this list.
			c.metrics.ExpireGhostMetric.Inc()

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

		bulkDelete.PrepareDelete(id, idToLabels[id], false)
	}

	c.newMetricLock.Lock()
	defer c.newMetricLock.Unlock()

	start := time.Now()

	err = bulkDelete.Delete(ctx)
	if err != nil {
		return err
	}

	err = c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, req := range expireUpdates {
				req := req
				task := func() error {
					return c.expirationUpdate(ctx, req)
				}
				select {
				case work <- task:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)
	if err != nil {
		return err
	}

	c.metrics.ExpireLockSeconds.Observe(time.Since(start).Seconds())

	c.metrics.ExpireMetricDelete.Add(float64(len(bulkDelete.deleteIDs)))
	c.metrics.ExpireMetric.Add(float64(len(ids)))

	return nil
}

// Run tasks concurrently with at most concurrency task in parallel.
// The queryGenerator must stop sending to the channel as soon as ctx is terminated.
func (c *CassandraIndex) concurrentTasks(
	ctx context.Context,
	concurrency int,
	queryGenerator func(ctx context.Context, c chan<- func() error) error,
) error {
	group, ctx := errgroup.WithContext(ctx)
	work := make(chan func() error)

	group.Go(func() error {
		defer close(work)

		return queryGenerator(ctx, work)
	})

	startCount := 0

	for task := range work {
		task := task

		group.Go(func() error {
			if err := task(); err != nil {
				return err
			}

			for task := range work {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				if err := task(); err != nil {
					return err
				}
			}

			return nil
		})

		startCount++
		if startCount >= concurrency {
			break
		}
	}

	return group.Wait()
}

type postingUpdateRequest struct {
	Label     labels.Label
	AddIDs    []uint64
	RemoveIDs []uint64
	Shard     int32
}

func (c *CassandraIndex) postingUpdate(ctx context.Context, job postingUpdateRequest) (*roaring.Bitmap, error) {
	bitmap, err := c.postings(ctx, []int32{job.Shard}, job.Label.Name, job.Label.Value, false)
	if err != nil {
		return nil, err
	}

	_, err = bitmap.Add(job.AddIDs...)
	if err != nil {
		return nil, fmt.Errorf("update bitmap: %w", err)
	}

	_, err = bitmap.Remove(job.RemoveIDs...)
	if err != nil {
		return nil, fmt.Errorf("update bitmap: %w", err)
	}

	if !bitmap.Any() {
		if err := c.store.DeletePostings(ctx, job.Shard, job.Label.Name, job.Label.Value); err != nil {
			return nil, fmt.Errorf("drop posting: %w", err)
		}

		return nil, errBitmapEmpty
	}

	var buffer bytes.Buffer

	_, err = bitmap.WriteTo(&buffer)
	if err != nil {
		return nil, fmt.Errorf("serialize bitmap: %w", err)
	}

	err = c.store.InsertPostings(ctx, job.Shard, job.Label.Name, job.Label.Value, buffer.Bytes())
	if err != nil {
		return bitmap, fmt.Errorf("update postings: %w", err)
	}

	return bitmap, nil
}

type expirationUpdateRequest struct {
	Day       time.Time
	AddIDs    []uint64
	RemoveIDs []uint64
}

func (c *CassandraIndex) expirationUpdate(ctx context.Context, job expirationUpdateRequest) error {
	bitmapExpiration, err := c.cassandraGetExpirationList(ctx, job.Day)
	if err != nil {
		return err
	}

	_, err = bitmapExpiration.Add(job.AddIDs...)
	if err != nil {
		return fmt.Errorf("update bitmap: %w", err)
	}

	idsToRemove := roaring.NewBitmap(job.RemoveIDs...)
	missingIDs := idsToRemove.Difference(bitmapExpiration)

	if missingIDs.Any() {
		slice := missingIDs.Slice()
		c.metrics.ExpireConflictMetric.Add(float64(len(slice)))

		c.deleteIDsFromCache(slice)
	}

	_, err = bitmapExpiration.Remove(job.RemoveIDs...)
	if err != nil {
		return fmt.Errorf("update bitmap: %w", err)
	}

	if !bitmapExpiration.Any() {
		return c.store.DeleteExpiration(ctx, job.Day)
	}

	var buffer bytes.Buffer

	_, err = bitmapExpiration.WriteTo(&buffer)
	if err != nil {
		return fmt.Errorf("derialize bitmap: %w", err)
	}

	return c.store.InsertExpiration(ctx, job.Day, buffer.Bytes())
}

// idsForMatcher return metric IDs matching given matchers.
// It's a wrapper around postingsForMatchers.
func (c *CassandraIndex) idsForMatchers(
	ctx context.Context,
	shards []int32,
	matchers []*labels.Matcher,
	directCheckThreshold int,
) (ids []types.MetricID, labelsList []labels.Labels, err error) {
	results, checkMatches, err := c.postingsForMatchers(ctx, shards, matchers, directCheckThreshold)
	if err != nil {
		return nil, nil, err
	}

	if results == nil {
		return nil, nil, nil
	}

	ids = bitsetToIDs(results)

	if checkMatches {
		labelsList, err = c.lookupLabels(ctx, ids, time.Now())
		if err != nil {
			return nil, nil, err
		}

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

// postingsForMatchers return metric IDs matching given matcher.
// The logic is inspired from Prometheus PostingsForMatchers (in querier.go).
func (c *CassandraIndex) postingsForMatchers( //nolint:gocognit
	ctx context.Context,
	shards []int32,
	matchers []*labels.Matcher,
	directCheckThreshold int,
) (bitmap *roaring.Bitmap, needCheckMatches bool, err error) {
	labelMustBeSet := make(map[string]bool, len(matchers))

	for _, m := range matchers {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

	var results *roaring.Bitmap

	// Unlike Prometheus querier.go, we merge/update directly into results (instead of
	// adding into its and notIts then building results).
	// We do this in two loops, one which fill its (the one which could add IDs - the "its" of Prometheus)
	// then one which remove ids (the "notIts" of Prometheus).
	for _, m := range matchers {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}

		// If there is only few results, prefer doing an explicit matching on labels
		// for each IDs left. This may spare few Cassandra query.
		// With postings filtering, we do one Cassandra query per matchers.
		// With explicit matching on labels, we do one Cassandra query per IDs BUT this query will be done anyway if the
		// series would be kept.
		if results != nil && results.Count() <= uint64(directCheckThreshold) {
			needCheckMatches = true

			break
		}

		if labelMustBeSet[m.Name] { //nolint:nestif
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp

			if isNot && !matchesEmpty { // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, false, fmt.Errorf("inverse matcher: %w", err)
				}

				it, err := c.inversePostingsForMatcher(ctx, shards, inverse)
				if err != nil {
					return nil, false, err
				}

				if results == nil {
					results = it
				} else {
					results = results.Intersect(it)
				}
			} else if !isNot { // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it, err := c.postingsForMatcher(ctx, shards, m)
				if err != nil {
					return nil, false, err
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
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}

		if results != nil && results.Count() <= uint64(directCheckThreshold) {
			needCheckMatches = true

			break
		}

		if labelMustBeSet[m.Name] { //nolint:nestif
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp

			if isNot && matchesEmpty { // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse, err := m.Inverse()
				if err != nil {
					return nil, false, fmt.Errorf("inverse matcher: %w", err)
				}

				it, err := c.postingsForMatcher(ctx, shards, inverse)
				if err != nil {
					return nil, false, err
				}

				if results == nil {
					// If there's nothing to subtract from, add in everything and remove the notIts later.
					results, err = c.postings(ctx, shards, allPostingLabel, allPostingLabel, true)
					if err != nil {
						return nil, false, err
					}
				}

				results = results.Difference(it)
			}
		} else { // l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			it, err := c.inversePostingsForMatcher(ctx, shards, m)
			if err != nil {
				return nil, false, err
			}

			if results == nil {
				// If there's nothing to subtract from, add in everything and remove the notIts later.
				results, err = c.postings(ctx, shards, allPostingLabel, allPostingLabel, true)
				if err != nil {
					return nil, false, err
				}
			}

			results = results.Difference(it)
		}
	}

	return results, needCheckMatches, nil
}

// postingsForMatcher return id that match one matcher.
// This method will not return postings for missing labels.
func (c *CassandraIndex) postingsForMatcher(
	ctx context.Context,
	shards []int32,
	m *labels.Matcher,
) (*roaring.Bitmap, error) {
	if m.Type == labels.MatchEqual {
		return c.postings(ctx, shards, m.Name, m.Value, true)
	}

	var it *roaring.Bitmap

	for _, baseTime := range shards {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		values, allBuffers, err := c.store.SelectValueForName(ctx, baseTime, m.Name)
		if errors.Is(err, gocql.ErrNotFound) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("get labels values: %w", err)
		}

		for i, val := range values {
			if m.Matches(val) {
				bitset := roaring.NewBTreeBitmap()

				err = bitset.UnmarshalBinary(allBuffers[i])
				if err != nil {
					return nil, fmt.Errorf("unmarshal bitmap: %w", err)
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

func (c *CassandraIndex) inversePostingsForMatcher(
	ctx context.Context,
	shards []int32,
	m *labels.Matcher,
) (*roaring.Bitmap, error) {
	if m.Type == labels.MatchNotEqual && m.Value != "" {
		inverse, err := m.Inverse()
		if err != nil {
			return nil, fmt.Errorf("inverse matcher: %w", err)
		}

		return c.postingsForMatcher(ctx, shards, inverse)
	}

	var it *roaring.Bitmap

	for _, baseTime := range shards {
		values, allBuffers, err := c.store.SelectValueForName(ctx, baseTime, m.Name)
		if errors.Is(err, gocql.ErrNotFound) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("get labels values: %w", err)
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

func (c *CassandraIndex) cassandraGetExpirationList(ctx context.Context, day time.Time) (*roaring.Bitmap, error) {
	tmp := roaring.NewBTreeBitmap()

	buffer, err := c.store.SelectExpiration(ctx, day)
	if errors.Is(err, gocql.ErrNotFound) {
		return tmp, nil
	} else if err != nil {
		return nil, fmt.Errorf("get list of expiration per day: %w", err)
	}

	err = tmp.UnmarshalBinary(buffer)

	return tmp, err
}

// ShardForTime return the shard number for give timestamp (second from epoc)
// The shard number should only be useful for debugging or InternalUpdatePostingShards.
func ShardForTime(ts int64) int32 {
	shardSize := int32(postingShardSize.Hours())

	return (int32(ts/3600) / shardSize) * shardSize
}

func (c *CassandraIndex) getTimeShards(ctx context.Context, start, end time.Time, returnAll bool) ([]int32, error) {
	if err := validatedTime(start, end); err != nil {
		return nil, err
	}

	shardSize := int32(postingShardSize.Hours())
	startShard := ShardForTime(start.Unix())
	endShard := ShardForTime(end.Unix())

	var (
		existingShards *roaring.Bitmap
		err            error
	)

	if !returnAll {
		existingShards, err = c.getExistingShards(ctx, false)
		if err != nil {
			return nil, err
		}

		if min, ok := existingShards.Min(); ok && startShard < int32(min) {
			startShard = int32(min)
		}

		if max := existingShards.Max(); endShard > int32(max) {
			endShard = int32(max)
		}
	}

	if startShard > endShard {
		return nil, nil
	}

	results := make([]int32, 0, (endShard-startShard)/shardSize+1)
	current := startShard

	for current <= endShard {
		results = append(results, current)
		current += shardSize
	}

	return results, nil
}

// selectLabelsList2ID is a thin wrapper around store SelectLabelsList2ID.
// It handle submitting parallel queries when len(ids) is too big.
func (c *CassandraIndex) selectLabelsList2ID(
	ctx context.Context,
	sortedLabelsListString []string,
) (map[string]types.MetricID, error) {
	if len(sortedLabelsListString) < maxCQLInValue {
		return c.store.SelectLabelsList2ID(ctx, sortedLabelsListString)
	}

	var (
		l       sync.Mutex
		results map[string]types.MetricID
	)

	return results, c.concurrentTasks(
		ctx,
		concurrentRead,
		func(ctx context.Context, work chan<- func() error) error {
			start := 0
			for start < len(sortedLabelsListString) {
				end := start + maxCQLInValue
				if end > len(sortedLabelsListString) {
					end = len(sortedLabelsListString)
				}

				subSortedLabelsListString := sortedLabelsListString[start:end]

				start += maxCQLInValue

				task := func() error {
					tmp, err := c.store.SelectLabelsList2ID(ctx, subSortedLabelsListString)
					if err != nil {
						return err
					}

					l.Lock()

					if len(results) == 0 {
						results = tmp
					} else {
						for k, v := range tmp {
							results[k] = v
						}
					}

					l.Unlock()

					return nil
				}

				select {
				case work <- task:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)
}

// selectIDS2LabelsAndExpiration is a thin wrapper around store SelectIDS2LabelsAndExpiration.
// It handle submitting parallel queries when len(ids) is too big.
func (c *CassandraIndex) selectIDS2LabelsAndExpiration(
	ctx context.Context,
	ids []types.MetricID,
) (map[types.MetricID]labels.Labels, map[types.MetricID]time.Time, error) {
	if len(ids) < maxCQLInValue {
		return c.store.SelectIDS2LabelsAndExpiration(ctx, ids)
	}

	var (
		l        sync.Mutex
		results  map[types.MetricID]labels.Labels
		results2 map[types.MetricID]time.Time
	)

	return results, results2, c.concurrentTasks(
		ctx,
		concurrentRead,
		func(ctx context.Context, work chan<- func() error) error {
			start := 0
			for start < len(ids) {
				end := start + maxCQLInValue
				if end > len(ids) {
					end = len(ids)
				}

				subIds := ids[start:end]

				start += maxCQLInValue

				task := func() error {
					tmp, tmp2, err := c.store.SelectIDS2LabelsAndExpiration(ctx, subIds)
					if err != nil {
						return err
					}

					l.Lock()

					if len(results) == 0 {
						results = tmp
					} else {
						for k, v := range tmp {
							results[k] = v
						}
					}

					if len(results2) == 0 {
						results2 = tmp2
					} else {
						for k, v := range tmp2 {
							results2[k] = v
						}
					}

					l.Unlock()

					return nil
				}

				select {
				case work <- task:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)
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
			log.Warn().Err(err).Msg("Can't get time to live from labels, using default")

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
type postingIter interface {
	HasNext() bool
	Next() (string, []byte)
	Err() error
}

type cassandraStore struct {
	session    *gocql.Session
	schemaLock sync.Locker
	metrics    *metrics
}

type cassandraByteIter struct {
	Iter   *gocql.Iter
	buffer []byte
	value  string
	err    error
}

func (i *cassandraByteIter) HasNext() bool {
	if i.Iter.Scan(&i.value, &i.buffer) {
		return true
	}

	i.err = i.Iter.Close()

	return false
}

func (i cassandraByteIter) Next() (string, []byte) {
	return i.value, i.buffer
}

func (i cassandraByteIter) Err() error {
	if errors.Is(i.err, gocql.ErrNotFound) {
		i.err = nil
	}

	return i.err
}

// createTables create all Cassandra tables.
func (s cassandraStore) Init(ctx context.Context) error {
	s.schemaLock.Lock()
	defer s.schemaLock.Unlock()

	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
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
		if err := s.session.Query(query).WithContext(ctx).Consistency(gocql.All).Exec(); err != nil {
			return err
		}
	}

	return nil
}

// InternalDropTables drop tables used by the index.
// This should only be used in test & benchmark.
func InternalDropTables(ctx context.Context, session *gocql.Session) error {
	queries := []string{
		"DROP TABLE IF EXISTS index_labels2id",
		"DROP TABLE IF EXISTS index_postings",
		"DROP TABLE IF EXISTS index_id2labels",
		"DROP TABLE IF EXISTS index_expiration",
	}
	for _, query := range queries {
		if err := session.Query(query).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (s cassandraStore) InsertPostings(
	ctx context.Context,
	shard int32,
	name string,
	value string,
	bitset []byte,
) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_postings (shard, name, value, bitset) VALUES (?, ?, ?, ?)",
		shard, name, value, bitset,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) InsertID2Labels(
	ctx context.Context,
	id types.MetricID,
	sortedLabels labels.Labels,
	expiration time.Time,
) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_id2labels (id, labels, expiration_date) VALUES (?, ?, ?)",
		id, sortedLabels, expiration,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) InsertLabels2ID(ctx context.Context, sortedLabelsString string, id types.MetricID) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_labels2id (labels, id) VALUES (?, ?)",
		sortedLabelsString, id,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeleteLabels2ID(ctx context.Context, sortedLabelsString string) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_labels2id WHERE labels = ?",
		sortedLabelsString,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeleteID2Labels(ctx context.Context, id types.MetricID) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_id2labels WHERE id = ?",
		id,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeleteExpiration(ctx context.Context, day time.Time) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_expiration WHERE day = ?",
		day,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeletePostings(ctx context.Context, shard int32, name string, value string) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"DELETE FROM index_postings WHERE shard = ? AND name = ? AND value = ?",
		shard, name, value,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) InsertExpiration(ctx context.Context, day time.Time, bitset []byte) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	return s.session.Query(
		"INSERT INTO index_expiration (day, bitset) VALUES (?, ?)",
		day, bitset,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) SelectLabelsList2ID(
	ctx context.Context,
	sortedLabelsListString []string,
) (map[string]types.MetricID, error) {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT labels, id FROM index_labels2id WHERE labels IN ?",
		sortedLabelsListString,
	).WithContext(ctx).Iter()

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

func (s cassandraStore) SelectIDS2LabelsAndExpiration(
	ctx context.Context,
	ids []types.MetricID,
) (map[types.MetricID]labels.Labels, map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT id, labels, expiration_date FROM index_id2labels WHERE id IN ?",
		ids,
	).WithContext(ctx).Iter()

	var (
		lbls       labels.Labels
		expiration time.Time
		id         int64
	)

	results := make(map[types.MetricID]labels.Labels, len(ids))
	results2 := make(map[types.MetricID]time.Time, len(ids))

	for iter.Scan(&id, &lbls, &expiration) {
		results[types.MetricID(id)] = lbls
		results2[types.MetricID(id)] = expiration
	}

	err := iter.Close()

	return results, results2, err
}

func (s cassandraStore) SelectExpiration(ctx context.Context, day time.Time) ([]byte, error) {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	query := s.session.Query(
		"SELECT bitset FROM index_expiration WHERE day = ?",
		day,
	).WithContext(ctx)

	var buffer []byte
	err := query.Scan(&buffer)

	return buffer, err
}

func (s cassandraStore) UpdateID2LabelsExpiration(ctx context.Context, id types.MetricID, expiration time.Time) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	query := s.session.Query(
		"UPDATE index_id2labels SET expiration_date = ? WHERE id = ?",
		expiration,
		int64(id),
	).WithContext(ctx)

	return query.Exec()
}

func (s cassandraStore) SelectPostingByName(ctx context.Context, shard int32, name string) postingIter {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT value, bitset FROM index_postings WHERE shard = ? AND name = ?",
		shard, name,
	).WithContext(ctx).Iter()

	return &cassandraByteIter{
		Iter: iter,
	}
}

func (s cassandraStore) SelectPostingByNameValue(
	ctx context.Context,
	shard int32,
	name string,
	value string,
) (buffer []byte, err error) {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	query := s.session.Query(
		"SELECT bitset FROM index_postings WHERE shard = ? AND name = ? AND value = ?",
		shard, name, value,
	).WithContext(ctx)

	err = query.Scan(&buffer)

	return
}

func (s cassandraStore) SelectValueForName(ctx context.Context, shard int32, name string) ([]string, [][]byte, error) {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	iter := s.session.Query(
		"SELECT value, bitset FROM index_postings WHERE shard = ? AND name = ?",
		shard, name,
	).WithContext(ctx).Iter()

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

func validatedTime(start time.Time, end time.Time) error {
	if start.Before(minTime) {
		return fmt.Errorf("%w: start time is too early", errTimeOutOfRange)
	}

	if start.After(maxTime) {
		return fmt.Errorf("%w: start time is too late", errTimeOutOfRange)
	}

	if end.Before(minTime) {
		return fmt.Errorf("%w: end time is too early", errTimeOutOfRange)
	}

	if end.After(maxTime) {
		return fmt.Errorf("%w: end time is too late", errTimeOutOfRange)
	}

	return nil
}
