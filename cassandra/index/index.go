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
	"regexp/syntax"
	"sort"
	"squirreldb/cassandra/connection"
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
	// Simple regex composed of less than maxOpAlertnateSubsForSimpleRegex OR
	// clauses are simplified to multiple non regex matchers.
	maxOpAlertnateSubsForSimpleRegex = 10
)

const (
	metricCreationLockTimeToLive  = 15 * time.Second
	metricExpiratorLockTimeToLive = 10 * time.Minute
)

// Update TTL of index entries in Cassandra every update delay.
// The actual TTL used in Cassandra is the metric data TTL + update delay.
// With long delay, there will be less updates on Cassandra, but entries will stay
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
	// The expiration of entries in Cassandra starts everyday at 00:00 UTC + expirationStartOffset.
	expirationStartOffset = 6 * time.Hour
	// The expiration is checked every expirationCheckInterval.
	expirationCheckInterval = 15 * time.Minute
)

//nolint:gochecknoglobals
var (
	// Shards <= 0 are invalid.
	indexMinValidTime = time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)
	indexMaxValidTime = time.Date(100000, 1, 1, 0, 0, 0, 0, time.UTC)
)

var (
	errTimeOutOfRange           = errors.New("time out of range")
	errBitmapEmpty              = errors.New("the result bitmap is empty")
	errNotASimpleRegex          = errors.New("not a simple regex")
	errNewMetricLockNotAcquired = errors.New("newMetricGlobalLock is not acquired")
	errMetricDoesNotExist       = errors.New("metric doesn't exist")
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

	// The three locks used in index are:
	// * lookupIDMutex is a local lock and only protect local (in-memory) value.
	// * newMetricGlobalLock is a global lock and protect values in Cassandra. It protect any
	//   value that could be written during metric creation. Since metric creation is done
	//   synchroniously when writing points, this lock should not be held for too long.
	// * expirationGlobalLock is a global lock and protect values in Cassandra. Unlike newMetricGlobalLock
	//   it protect value that can't we written on metric creation.
	lookupIDMutex            sync.Mutex
	newMetricGlobalLock      types.TryLocker
	expirationGlobalLock     types.TryLocker
	expirationUpdateRequests map[time.Time]expirationUpdateRequest
	expirationBackoff        backoff.BackOff
	nextExpirationAt         time.Time
	labelsToID               map[uint64][]idData
	idInShard                map[int32]*roaring.Bitmap
	idInShardLastAccess      map[int32]time.Time
	existingShards           *roaring.Bitmap

	idsToLabels          *labelsLookupCache
	postingsCache        *postingsCache
	shardExpirationCache *shardExpirationCache
	metrics              *metrics
	logger               zerolog.Logger
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
	DeletePostingsByNames(ctx context.Context, shard int32, names []string) error
}

const (
	globalAllPostingLabel = "__global__all|metrics__" // we use the "|" since it's invalid for prometheus label name
	allPostingLabel       = "__all|metrics__"
	postinglabelName      = "__label|names__" // kept known labels name as label value
	// ID are added in two-phase in postings. This one is updated first. See updatePostings.
	maybePostingLabel = "__maybe|metrics__"
	// existingShardsLabel is the label name used to store all known shards in postings.
	existingShardsLabel = "__shard|exists__"
	// expirationShardLabel is the label name used to store the expiration of a shard.
	expirationShardLabel = "__shard|expiration"
	postingShardSize     = 7 * 24 * time.Hour
	shardDateFormat      = "2006-01-02"
	// globalShardNumber value is an impossible value for a normal shard,
	// because postingShardSize is a multiple of 2 hours making odd shard number
	// impossible.
	// Index is sharded by time for postings. The shard number (an int32) is the
	// rounded to postingShardSize number of hours since epoc (1970).
	globalShardNumber = -1
)

// New creates a new CassandraIndex object.
func New(
	ctx context.Context,
	reg prometheus.Registerer,
	connection *connection.Connection,
	options Options,
	logger zerolog.Logger,
) (*CassandraIndex, error) {
	metrics := newMetrics(reg)

	return initialize(
		ctx,
		cassandraStore{
			connection: connection,
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
		shardExpirationCache:     &shardExpirationCache{},
		expirationUpdateRequests: make(map[time.Time]expirationUpdateRequest),
		expirationBackoff:        expBackoff,
		newMetricGlobalLock:      options.LockFactory.CreateLock(newMetricLockName, metricCreationLockTimeToLive),
		expirationGlobalLock:     options.LockFactory.CreateLock(expireMetricLockName, metricExpiratorLockTimeToLive),
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
	c.applyExpirationUpdateRequests(ctx, now)
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

		c.logger.Warn().Err(err).Msgf("Retrying in %v", nextBackoff.Round(time.Second))
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
					tmp, err := c.postings(ctx, []int32{shard}, maybePostingLabel, maybePostingLabel, false)
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
func (c *CassandraIndex) Dump(ctx context.Context, w io.Writer) error {
	allPosting, err := c.postings(ctx, []int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel, false)
	if err != nil {
		return err
	}

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	if err := c.dumpPostings(ctx, csvWriter, allPosting); err != nil {
		return err
	}

	return ctx.Err()
}

func (c *CassandraIndex) DumpByLabels(
	ctx context.Context,
	w io.Writer,
	start, end time.Time,
	matchers []*labels.Matcher,
) error {
	metrics, err := c.Search(ctx, start, end, matchers)
	if err != nil {
		fmt.Fprintf(w, "fail to search labels: %v", err)

		return nil
	}

	ids := make([]types.MetricID, 0, metrics.Count())
	for metrics.Next() {
		ids = append(ids, metrics.At().ID)
	}

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	return c.dumpBulk(ctx, csvWriter, ids)
}

func (c *CassandraIndex) DumpByExpirationDate(ctx context.Context, w io.Writer, expirationDate time.Time) error {
	expirationBitmap, err := c.cassandraGetExpirationList(ctx, expirationDate.Truncate(24*time.Hour))
	if err != nil {
		return fmt.Errorf("unable to get metric expiration list: %w", err)
	}

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	if err := c.dumpPostings(ctx, csvWriter, expirationBitmap); err != nil {
		return err
	}

	return ctx.Err()
}

func (c *CassandraIndex) DumpByShard(ctx context.Context, w io.Writer, shard time.Time) error {
	allPosting, err := c.postings(ctx, []int32{shardForTime(shard.Unix())}, allPostingLabel, allPostingLabel, false)
	if err != nil {
		return fmt.Errorf("unable to get metric expiration list: %w", err)
	}

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	if err := c.dumpPostings(ctx, csvWriter, allPosting); err != nil {
		return err
	}

	return ctx.Err()
}

func (c *CassandraIndex) DumpByPosting(
	ctx context.Context,
	w io.Writer,
	shard time.Time,
	name string,
	value string,
) error {
	shardID := shardForTime(shard.Unix())
	if shard.IsZero() {
		shardID = globalShardNumber
	}

	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	if value == "" {
		return c.dumpByPostingByName(ctx, csvWriter, shardID, name)
	}

	allPosting, err := c.postings(ctx, []int32{shardID}, name, value, false)
	if err != nil {
		return fmt.Errorf("unable to get metric expiration list: %w", err)
	}

	if err := c.dumpPostings(ctx, csvWriter, allPosting); err != nil {
		return err
	}

	return ctx.Err()
}

func (c *CassandraIndex) dumpByPostingByName(
	ctx context.Context,
	csvWriter *csv.Writer,
	shardID int32,
	name string,
) error {
	iter := c.store.SelectPostingByName(ctx, shardID, name)
	defer iter.Close()

	for iter.HasNext() {
		tmp := roaring.NewBTreeBitmap()
		labelValue, buffer := iter.Next()

		err := tmp.UnmarshalBinary(buffer)
		if err != nil {
			return fmt.Errorf("unmarshal fail on %s=%s: %w", name, labelValue, err)
		}

		if tmp.Count() == 0 {
			csvLine := []string{
				"-1",
				fmt.Sprintf("warning: posting %s=%s is empty", name, labelValue),
			}

			err := csvWriter.Write(csvLine)
			if err != nil {
				return err
			}
		} else {
			if err := c.dumpPostings(ctx, csvWriter, tmp); err != nil {
				return err
			}
		}
	}

	return ctx.Err()
}

func (c *CassandraIndex) dumpPostings(ctx context.Context, w *csv.Writer, posting *roaring.Bitmap) error {
	iter := posting.Iterator()
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

		err := c.dumpBulk(ctx, w, pendingIDs)
		if err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (c *CassandraIndex) dumpBulk(ctx context.Context, w *csv.Writer, ids []types.MetricID) error {
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
				"",
			}
		case !expirationOk:
			csvLine = []string{
				strconv.FormatInt(int64(id), 10),
				lbls.String(),
				"Missing expiration which shouldn't be possible !",
			}
		default:
			csvLine = []string{
				strconv.FormatInt(int64(id), 10),
				lbls.String(),
				expiration.String(),
			}
		}

		err := w.Write(csvLine)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CassandraIndex) InfoGlobal(ctx context.Context, w io.Writer) error {
	allPosting, err := c.postings(ctx, []int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel, false)
	if err != nil {
		return err
	}

	shards, err := c.postings(ctx, []int32{globalShardNumber}, existingShardsLabel, existingShardsLabel, false)
	if err != nil {
		return fmt.Errorf("get postings for existing shards: %w", err)
	}

	fmt.Fprintf(w, "Number of metrics in global all Postings: %d\n", allPosting.Count())
	fmt.Fprintf(w, "Number of shards (shard size %s): %d\n", postingShardSize, shards.Count())

	it := shards.Iterator()
	for ctx.Err() == nil {
		shard, eof := it.Next()
		if eof {
			break
		}

		postingShard, err := c.postings(ctx, []int32{int32(shard)}, allPostingLabel, allPostingLabel, false)
		if err != nil {
			return err
		}

		maybepostingShard, err := c.postings(ctx, []int32{int32(shard)}, maybePostingLabel, maybePostingLabel, false)
		if err != nil {
			return err
		}

		labelNamesCount := 0
		iter := c.store.SelectPostingByName(ctx, int32(shard), postinglabelName)

		for iter.HasNext() {
			labelNamesCount++
		}

		iter.Close()

		shardExpiration, _, err := c.getShardExpirationFromStore(ctx, int32(shard))

		// Some shard expiration metrics may not exist if the shard were created before the
		// expiration was introduced. In this case the expiration will be shown at 0001-01-01 00:00:00.
		if err != nil && !errors.Is(err, errMetricDoesNotExist) {
			return err
		}

		fmt.Fprintf(
			w,
			"Shard %s (ID %d) has %d metrics (and %d in maybePosting), %d label names and expires at %s\n",
			timeForShard(int32(shard)).Format(shardDateFormat),
			shard,
			postingShard.Count(),
			maybepostingShard.Count(),
			labelNamesCount,
			shardExpiration,
		)
	}

	return nil
}

func (c *CassandraIndex) InfoByID(ctx context.Context, w io.Writer, id types.MetricID) error {
	labelsMap, expiration, err := c.store.SelectIDS2LabelsAndExpiration(ctx, []types.MetricID{id})
	if err != nil {
		return err
	}

	allPosting, err := c.postings(ctx, []int32{globalShardNumber}, globalAllPostingLabel, globalAllPostingLabel, false)
	if err != nil {
		return err
	}

	shards, err := c.postings(ctx, []int32{globalShardNumber}, existingShardsLabel, existingShardsLabel, false)
	if err != nil {
		return fmt.Errorf("get postings for existing shards: %w", err)
	}

	var lbls labels.Labels

	if len(labelsMap) == 0 {
		fmt.Fprintf(w, "Metric ID %d not found in id2labels and isInGlobal=%v\n", id, allPosting.Contains(uint64(id)))
	} else {
		lbls = labelsMap[id]

		fmt.Fprintf(
			w,
			"Metric ID %d has label %s with expiration %s and isInGlobal=%v\n",
			id,
			lbls,
			expiration[id],
			allPosting.Contains(uint64(id)),
		)

		sortedLabels := sortLabels(lbls)
		sortedLabelsString := sortedLabels.String()
		resp, err := c.store.SelectLabelsList2ID(ctx, []string{sortedLabelsString})
		if err != nil {
			return err
		}

		if len(resp) == 0 {
			fmt.Fprintf(w, "Labels %s not found in labels2id\n", sortedLabelsString)
		} else {
			fmt.Fprintf(w, "Labels %s found in labels2id and ID is %d\n", sortedLabelsString, resp[sortedLabelsString])
		}
	}

	it := shards.Iterator()
	for ctx.Err() == nil {
		shard, eof := it.Next()
		if eof {
			break
		}

		postingShard, err := c.postings(ctx, []int32{int32(shard)}, allPostingLabel, allPostingLabel, false)
		if err != nil {
			return err
		}

		maybepostingShard, err := c.postings(ctx, []int32{int32(shard)}, maybePostingLabel, maybePostingLabel, false)
		if err != nil {
			return err
		}

		inPosting := postingShard.Contains(uint64(id))
		inMaybe := maybepostingShard.Contains(uint64(id))

		fmt.Fprintf(
			w,
			"Shard %s (ID %d) has the metric in posting=%v, maybePosting=%v\n",
			timeForShard(int32(shard)).Format(shardDateFormat),
			shard,
			inPosting,
			inMaybe,
		)

		if len(labelsMap) > 0 && (inPosting || inMaybe) {
			missingPostings := make([]string, 0)

			for _, l := range lbls {
				posting, err := c.postings(ctx, []int32{int32(shard)}, l.Name, l.Value, false)
				if err != nil {
					return err
				}

				if !posting.Contains(uint64(id)) {
					missingPostings = append(missingPostings, fmt.Sprintf("%s=%s", l.Name, l.Value))
				}
			}

			if len(missingPostings) > 0 {
				fmt.Fprintf(
					w,
					"Shard %s: the following postings are missing: %s\n",
					timeForShard(int32(shard)).Format(shardDateFormat),
					strings.Join(missingPostings, ", "),
				)
			} else {
				fmt.Fprintf(w, "Shard %s: all postings are present\n", timeForShard(int32(shard)).Format(shardDateFormat))
			}
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

// verify perform some checks on index consistency.
// It could apply fixes and could acquire the newMetricGlobalLock to ensure a consitent read of the index.
// The lock is required as a metric creation / expiration during the verification process could
// return a false-positive, but holding the lock will block metric creation during the whole verification process.
// When any issue is found, the hadIssue will be true (and description of the issue is written to w). Note
// that some issue might get fixed automatically (e.g. expired metrics that are not yet processed, partial write should
// be fixed when expiration is applied for them, ...).
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
		c.newMetricGlobalLock.Lock()
		defer c.newMetricGlobalLock.Unlock()
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
			bulkHadIssue, err := c.verifyBulk(ctx, now, w, doFix, pendingIds, bulkDeleter, allPosting, allGoodIds)
			if err != nil {
				return hadIssue, err
			}

			hadIssue = hadIssue || bulkHadIssue
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
		fmt.Fprintf(w, "Checking shard %s (ID %d)\n", timeForShard(shard).Format(shardDateFormat), shard)

		shardHadIssue, err := c.verifyShard(ctx, w, doFix, shard, allPosting)
		if err != nil {
			return hadIssue, err
		}

		hadIssue = hadIssue || shardHadIssue

		if respf, ok := w.(http.Flusher); ok {
			respf.Flush()
		}
	}

	return hadIssue, ctx.Err()
}

// verifyMissingShard search from now+3 weeks to 100 weeks before this points for shard not present in existingShards.
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

		queryShard := []int32{shardForTime(current.Unix())}

		it, err := c.postings(ctx, queryShard, maybePostingLabel, maybePostingLabel, false)
		if err != nil {
			return 0, shards, err
		}

		if it != nil && it.Any() && !shards.Contains(uint64(queryShard[0])) {
			errorCount++

			fmt.Fprintf(
				w,
				"Shard %s for time %v isn't in all shards",
				timeForShard(queryShard[0]).Format(shardDateFormat),
				current.String(),
			)

			if doFix {
				_, err = shards.AddN(uint64(queryShard[0]))
				if err != nil {
					return 0, shards, fmt.Errorf("update bitmap: %w", err)
				}
			}
		}

		current = current.Add(-postingShardSize)
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

			fmt.Fprintf(w, "Shard %s is listed in all shards but don't exists\n", timeForShard(shard).Format(shardDateFormat))

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
) (hadIssue bool, err error) {
	id2Labels, id2expiration, err := c.selectIDS2LabelsAndExpiration(ctx, ids)
	if err != nil {
		return hadIssue, fmt.Errorf("get labels: %w", err)
	}

	allLabelsString := make([]string, 0, len(ids))

	for _, id := range ids {
		lbls, ok := id2Labels[id]
		if !ok {
			fmt.Fprintf(w, "ID %10d does not exists in ID2Labels, partial write ?\n", id)

			hadIssue = true

			if doFix {
				bulkDeleter.PrepareDelete(id, nil, false)
			}

			continue
		}

		allLabelsString = append(allLabelsString, lbls.String())

		_, ok = id2expiration[id]
		if !ok {
			fmt.Fprintf(
				w,
				"ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify\n",
				id,
				lbls.String(),
			)

			hadIssue = true

			continue
		}
	}

	if ctx.Err() != nil {
		return hadIssue, ctx.Err()
	}

	labels2ID, err := c.selectLabelsList2ID(ctx, allLabelsString)
	if err != nil {
		return hadIssue, fmt.Errorf("get labels2ID: %w", err)
	}

	for _, id := range ids {
		if ctx.Err() != nil {
			return hadIssue, ctx.Err()
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

			hadIssue = true

			if doFix {
				bulkDeleter.PrepareDelete(id, lbls, false)
			}

			continue
		}

		if id != id2 { //nolint:nestif
			tmp, tmp2, err := c.selectIDS2LabelsAndExpiration(ctx, []types.MetricID{id2})
			if err != nil {
				return hadIssue, fmt.Errorf("get labels from store: %w", err)
			}

			lbls2 := tmp[id2]
			expiration2, ok := tmp2[id2]

			if !ok && lbls2 != nil {
				fmt.Fprintf(
					w,
					"ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify",
					id2,
					lbls2.String(),
				)

				hadIssue = true

				continue
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

				hadIssue = true

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

				hadIssue = true

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

				hadIssue = true

				// Assume that metric2 is better. It has id2labels, labels2id and in all postings
				if doFix {
					bulkDeleter.PrepareDelete(id, lbls, true)
				}
			}

			continue
		}

		if now.After(expiration.Add(24 * time.Hour)) {
			fmt.Fprintf(w, "ID %10d (%v) should have expired on %v\n", id, lbls.String(), expiration)

			hadIssue = true

			if doFix {
				bulkDeleter.PrepareDelete(id, lbls, false)
			}

			continue
		}

		_, err = allGoodIds.AddN(uint64(id))
		if err != nil {
			return hadIssue, fmt.Errorf("update bitmap: %w", err)
		}
	}

	return hadIssue, nil
}

// check that postings for given shard is consistent.
func (c *CassandraIndex) verifyShard( //nolint:maintidx
	ctx context.Context,
	w io.Writer,
	doFix bool,
	shard int32,
	allPosting *roaring.Bitmap,
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
			"shard %s is empty (automatic cleanup may fix this)!\n",
			timeForShard(shard).Format(shardDateFormat),
		)
	}

	if !localMaybe.Any() {
		hadIssue = true

		fmt.Fprintf(
			w,
			"shard %s is empty!\n",
			timeForShard(shard).Format(shardDateFormat),
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
			"shard %s: ID %d is present in localAll but not in localMaybe!\n",
			timeForShard(shard).Format(shardDateFormat),
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
			"shard %s: ID %d is present in localMaybe but not in localAll (automatic cleanup may fix this)!\n",
			timeForShard(shard).Format(shardDateFormat),
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
		{name: "global all IDs", it: allPosting},
		{name: "shard all IDs", it: localAll},
		{name: "shard maybe present IDs", it: localMaybe},
	}

	labelNames[postinglabelName] = true

	iter := c.store.SelectPostingByName(ctx, shard, postinglabelName)
	defer iter.Close()

	for iter.HasNext() {
		labelValue, buffer := iter.Next()

		if _, ok := labelNames[labelValue]; !ok { //nolint:nestif
			hadIssue = true

			fmt.Fprintf(
				w,
				"shard %s: postinglabelName has extra name=%s\n",
				timeForShard(shard).Format(shardDateFormat),
				labelValue,
			)

			if doFix {
				lbl := labels.Label{
					Name:  postinglabelName,
					Value: labelValue,
				}
				tmp := roaring.NewBTreeBitmap()

				err := tmp.UnmarshalBinary(buffer)
				if err != nil {
					return hadIssue, fmt.Errorf("unmarshal fail: %w", err)
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

				updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, tmp.Slice()...)
			}
		}
	}

	for name := range labelNames {
		if ctx.Err() != nil {
			break
		}

		iter := c.store.SelectPostingByName(ctx, shard, name)
		defer iter.Close()

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
					"shard %s: extra posting for %s=%s exists (with %d IDs)\n",
					timeForShard(shard).Format(shardDateFormat),
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
						"shard %s: missing ID %d in posting for %s=%s\n",
						timeForShard(shard).Format(shardDateFormat),
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
						"shard %s: extra ID %d in posting for %s=%s (present in maybe=%v allId=%v globalAll=%v)\n",
						timeForShard(shard).Format(shardDateFormat),
						id,
						name,
						labelValue,
						localMaybe.Contains(id),
						localAll.Contains(id),
						allPosting.Contains(id),
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
						"shard %s: posting for %s=%s has ID %d which is not in %s!\n",
						timeForShard(shard).Format(shardDateFormat),
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
			"shard %s: posting %s=%s was expected to exists\n",
			timeForShard(shard).Format(shardDateFormat),
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

// postings returns the ids matching the given Label name & value.
// If value is the empty string, it matches any values (but the label must be set).
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
		defer iter.Close()

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

		if ok := c.newMetricGlobalLock.TryLock(ctx, 15*time.Second); !ok {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}

			return nil, nil, errNewMetricLockNotAcquired
		}

		done, err := c.createMetrics(ctx, now, pending, false)

		c.newMetricGlobalLock.Unlock()

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
		ttlSeconds := req.TTLSeconds
		if ttlSeconds == 0 {
			ttlSeconds = int64(c.options.DefaultTimeToLive.Seconds())
		}

		labelsKey := req.Labels.Hash()
		data, found := c.getIDData(labelsKey, req.Labels)

		shards, err := c.getTimeShards(ctx, req.Start, req.End, true)
		if err != nil {
			c.lookupIDMutex.Unlock()

			return nil, nil, err
		}

		if found && data.cassandraEntryExpiration.Before(now.Add(2*backgroundCheckInterval)) {
			// This entry will expire soon. To reduce risk of using invalid cache (due to race
			// condition with another SquirrelDB delete metrics), first refresh the expiration,
			// and then refresh entry from Cassandra (ignore the cache).
			wantedEntryExpiration := now.Add(time.Duration(ttlSeconds) * time.Second)
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

		entries[i] = lookupEntry{
			idData:       data,
			labelsKey:    labelsKey,
			ttl:          ttlSeconds,
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

// updateShardExpiration updates the shards expiration if
// the new expiration is after the current expiration.
func (c *CassandraIndex) updateShardExpiration(
	ctx context.Context,
	now time.Time,
	shard int32,
	newExpiration time.Time,
) error {
	// Get current shard expiration.
	currentExpiration, expirationID, err := c.getShardExpiration(ctx, shard)
	if errors.Is(err, errMetricDoesNotExist) {
		// The shard expiration metric does not exist, create it.
		return c.createShardExpirationMetric(ctx, now, shard, newExpiration)
	}

	if err != nil {
		return err
	}

	if currentExpiration.After(newExpiration) {
		// Nothing to do, the current expiration is already after the new expiration.
		return nil
	}

	// Update the shard expiration.
	err = c.refreshExpiration(ctx, expirationID, currentExpiration, newExpiration)
	if err != nil {
		return fmt.Errorf("refresh shard expiration: %w", err)
	}

	c.shardExpirationCache.Set(shard, newExpiration, expirationID)

	return nil
}

// labelsForShardExpiration returns the labels for the shard expiration metric.
func labelsForShardExpiration(shard int32) labels.Labels {
	return labels.Labels{
		{
			Name:  expirationShardLabel,
			Value: strconv.Itoa(int(shard)),
		},
	}
}

// getShardExpiration returns the expiration and metric ID for a shard.
// It uses the shard expiration cache, or the store if the shard is not in the cache.
func (c *CassandraIndex) getShardExpiration(
	ctx context.Context,
	shard int32,
) (time.Time, types.MetricID, error) {
	// Try to get the expiration from the cache.
	currentExpiration, expirationID, found := c.shardExpirationCache.Get(shard)
	if found {
		return currentExpiration, expirationID, nil
	}

	// If it failed, get the expiration from the store and update the cache.
	currentExpiration, expirationID, err := c.getShardExpirationFromStore(ctx, shard)
	if err != nil {
		return time.Time{}, 0, err
	}

	c.shardExpirationCache.Set(shard, currentExpiration, expirationID)

	return currentExpiration, expirationID, nil
}

// getShardExpirationFromStore returns the expiration and metric ID for a shard
// from the store. It doesn't use the cache.
func (c *CassandraIndex) getShardExpirationFromStore(
	ctx context.Context,
	shard int32,
) (time.Time, types.MetricID, error) {
	// Get the shard expiration metric ID from the shard label.
	shardLabelsString := labelsForShardExpiration(shard).String()

	labelsToID, err := c.store.SelectLabelsList2ID(ctx, []string{shardLabelsString})
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("select labels2id: %w", err)
	}

	expirationID, ok := labelsToID[shardLabelsString]
	if !ok {
		return time.Time{}, 0, errMetricDoesNotExist
	}

	// Get the metric expiration from the metric ID.
	_, idToExpiration, err := c.store.SelectIDS2LabelsAndExpiration(ctx, []types.MetricID{expirationID})
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("select id2labels: %w", err)
	}

	currentExpiration, ok := idToExpiration[expirationID]
	if !ok {
		// The metric is present in labels2id but not in id2labels.
		// This should not be possible because when the metric is created it's first
		// inserted in IDsToLabel.
		c.logger.Warn().Msgf("Failed to get current expiration for shard %s, forcing expiration update", timeForShard(shard))

		// Force expiration update and don't set the expiration in the cache.
		currentExpiration = time.Time{}

		return currentExpiration, expirationID, nil
	}

	return currentExpiration, expirationID, nil
}

// createShardExpirationMetric creates a new metric for the shard expiration.
func (c *CassandraIndex) createShardExpirationMetric(
	ctx context.Context,
	now time.Time,
	shard int32,
	expiration time.Time,
) error {
	c.logger.Debug().Msgf("Creating shard %s expiration metric with expiration %s", timeForShard(shard), expiration)

	// Choose the TTL to make the metric expire at the given expiration.
	ttl := int64(expiration.Sub(now).Seconds())

	shardLabels := labelsForShardExpiration(shard)
	expirationEntry := lookupEntry{
		sortedLabels:       shardLabels,
		sortedLabelsString: shardLabels.String(),
		ttl:                ttl,
	}

	if ok := c.newMetricGlobalLock.TryLock(ctx, 15*time.Second); !ok {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		return errNewMetricLockNotAcquired
	}

	_, err := c.createMetrics(ctx, now, []lookupEntry{expirationEntry}, false)

	c.newMetricGlobalLock.Unlock()

	if err != nil {
		return fmt.Errorf("create shard expiration metric: %w", err)
	}

	return nil
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
// The lock newMetricGlobalLock is assumed to be held.
// Some care should be taken to avoid assigning the same ID from two SquirrelDB instance, so:
//
// * To avoid race-condition, redo a check that metrics are not yet registered now that the lock is acquired
// * Read the all-metric postings. From there we find a free ID
// * Update Cassandra tables to store this new metrics. The insertion is done in the following order:
//   - First an entry is added to the expiration table. This ensure that in case of crash in this process,
//     the ID will eventually be freed.
//   - Then it updates:
//   - the all-metric postings. This effectively reserve the ID.
//   - the id2labels tables (it gives informations needed to cleanup other postings)
//   - finally insert in labels2id, which is done last because it's this table that determines if
//     a metrics exists.
//
// If the above process crashes and partially writes some values, it's still in a good state because:
//   - For the insertion, it's the last entry (in labels2id) that matters.
//     The creation will be retried when the point is retried.
//   - For reading, even if the metric ID may match search (as soon as it's in some posting, it may happen),
//     since no data points could be written and empty result are stripped, they won't be in results.
//   - For writing using __metric_id__ labels, it may indeed succeed if the partial write reached id2labels...
//     BUT to have the metric ID, client must first do a succesfull read to get the ID. So this shouldn't happen.
//
// The expiration tables are used to know which metrics are likely to expire on a given date.
// They are grouped by day (that is, the tables contain one row per day, each row being the day
// and the list of metric IDs that may expire on this day).
// A background process will process each past day from these tables and for each metrics:
//   - Check if the metrics is actually expired. It may not be the case, if the metrics continued to get points.
//     It does this check using a field of the table id2labels which is refreshed.
//   - If expired, delete entry for this metric from the index (the opposite of creation)
//   - If not expired, add the metric IDs to the new expiration day in the table.
//   - Once finished, delete the processed day.
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

		_, err = allPosting.AddN(uint64(newID))
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
//   - if updateCache is true, gather all shard impacted and refresh the idInShard cache
//     (avoid re-use issue after metric creation)
//   - For each update in a shard, if the ID is already present, skip it
//   - Then update list of existings shards
//   - Add ID to maybe-present posting list (this is used in cleanup)
//   - Add ID to postings
//   - Add ID to presence list per shard
//
// This insertion order all to recover some failure:
//   - No write will happen because ID is inserted to the presence list. Meaning
//     reads won't get inconsistent result.
//     It also means that retry of write will fix the issue,
//   - The insert in maybe-present allow cleanup to known if an ID (may) need cleanup in the shard.
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
		if ok := c.newMetricGlobalLock.TryLock(ctx, 15*time.Second); !ok {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return errNewMetricLockNotAcquired
		}

		err := c.applyUpdatePostingShards(ctx, maybePresent, updates, precense)

		c.newMetricGlobalLock.Unlock()

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
// It implements an inverted index (as used in full-text search). The idea is that word
// are a couple LabelName=LabelValue. As in an inverted index, it use this "word" to
// query a posting table which return the list of document ID (metric ID here) that
// has this "word".
//
// In normal full-text search, the document(s) with the most match will be return, here
// it return the "document" (metric ID) that exactly match all matchers.
//
// Finally since Matcher could be other thing than LabelName=LabelValue (like not equal or using regular expression)
// there is a first pass that convert them to something that works with the inverted index: it queries all values
// for the given label name then for each value, it the value match the filter convert to an simple equal matcher.
// Then this simple equal matcher could be used with the posting of the inverted index (e.g. name!="cpu" will be
// converted in something like name="memory" || name="disk" || name="...").
//
// There is still two additional special case: when label should be defined (regardless of the value, e.g. name!="") or
// when the label should NOT be defined (e.g. name="").
// In those case, it use the ability of our posting table to query for metric ID that has a LabelName regardless of
// the values.
//   - For label must be defined, it increments the number of Matcher satified if the metric has the label.
//     In principle it's the same as if it expanded it to all possible values (e.g. with name!="" it avoids expanding
//     to name="memory" || name="disk" and directly ask for name=*)
//   - For label must NOT be defined, it query for all metric IDs that has this label, then increments the number of
//     Matcher satified if currently found metrics are not in the list of metrics having this label.
//     Note: this means that it must already have found some metrics (and that this filter is applied at the end)
//     but PromQL forbids to only have label-not-defined matcher, so some other matcher must exists.
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

	result, err := c.idsForMatchers(ctx, shards, matchers, 3)
	if err != nil {
		return nil, err
	}

	c.metrics.SearchMetrics.Add(float64(result.Count()))

	return result, nil
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
				// This may delete too many entries, but:
				// 1) normally only 1 entry matches the hash
				// 2) it's a cache, we don't lose data
				delete(c.labelsToID, key)

				size -= len(idsData)

				break
			}
		}
	}

	c.metrics.CacheSize.WithLabelValues("lookup-id").Set(float64(size))
}

func (c *CassandraIndex) applyExpirationUpdateRequests(ctx context.Context, now time.Time) {
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

	err := c.applyExpirationUpdateRequestsLock(ctx, now, expireUpdates)
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

func (c *CassandraIndex) applyExpirationUpdateRequestsLock(
	ctx context.Context,
	now time.Time,
	expireUpdates []expirationUpdateRequest,
) error {
	// Update current shard expiration with the highest expiration.
	maxExpiration := time.Time{}
	for _, update := range expireUpdates {
		if update.Day.After(maxExpiration) {
			maxExpiration = update.Day
		}
	}

	err := c.updateShardExpiration(ctx, now, shardForTime(now.Unix()), maxExpiration)
	if err != nil {
		return fmt.Errorf("update shard expiration: %w", err)
	}

	c.newMetricGlobalLock.Lock()
	defer c.newMetricGlobalLock.Unlock()

	err = c.concurrentTasks(
		ctx,
		concurrentInsert,
		func(ctx context.Context, work chan<- func() error) error {
			for _, req := range expireUpdates {
				// We need to create a copy of AddIDs/RemoveIDs, because expirationUpdate will mutate them
				// (because AddN/RemoveN mutate them).
				// We can't have mutation of them, because in case of C* error, we need to restore the initial state.
				newReq := expirationUpdateRequest{
					Day:       req.Day,
					AddIDs:    make([]uint64, len(req.AddIDs)),
					RemoveIDs: make([]uint64, len(req.RemoveIDs)),
				}

				copy(newReq.AddIDs, req.AddIDs)
				copy(newReq.RemoveIDs, req.RemoveIDs)

				task := func() error {
					return c.expirationUpdate(ctx, newReq)
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

	return err
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

	if ok := c.newMetricGlobalLock.TryLock(ctx, 15*time.Second); !ok {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return nil, errNewMetricLockNotAcquired
	}

	done, err := c.createMetrics(ctx, time.Now(), requests, true)

	c.newMetricGlobalLock.Unlock()

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
func (c *CassandraIndex) InternalForceExpirationTimestamp(ctx context.Context, value time.Time) error {
	if acquired := c.expirationGlobalLock.TryLock(ctx, 0); !acquired {
		return errors.New("lock held, please retry")
	}

	defer c.expirationGlobalLock.Unlock()

	return c.options.States.Write(ctx, expireMetricStateName, value.Format(time.RFC3339))
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

func (c *CassandraIndex) expirationLastProcessedDay(ctx context.Context) (time.Time, error) {
	var fromTimeStr string

	_, err := c.options.States.Read(ctx, expireMetricStateName, &fromTimeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to get last processed day for metrics expiration: %w", err)
	}

	if fromTimeStr != "" {
		lastProcessedDay, _ := time.Parse(time.RFC3339, fromTimeStr)

		return lastProcessedDay, nil
	}

	return time.Time{}, nil
}

// cassandraExpire remove all entry in Cassandra that have expired.
func (c *CassandraIndex) cassandraExpire(ctx context.Context, now time.Time) (bool, error) {
	if acquired := c.expirationGlobalLock.TryLock(ctx, 0); !acquired {
		return false, nil
	}
	defer c.expirationGlobalLock.Unlock()

	start := time.Now()

	defer func() {
		c.metrics.ExpireTotalSeconds.Observe(time.Since(start).Seconds())
	}()

	lastProcessedDay, err := c.expirationLastProcessedDay(ctx)
	if err != nil {
		return false, err
	}

	maxTime := now.Truncate(24 * time.Hour).Add(-24 * time.Hour)

	if lastProcessedDay.IsZero() {
		lastProcessedDay = maxTime

		err := c.options.States.Write(ctx, expireMetricStateName, lastProcessedDay.Format(time.RFC3339))
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

	// We don't need the newMetricGlobalLock lock here, because newly created metrics
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

		_, err = bitmap.RemoveN(results...)
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

	err = c.options.States.Write(ctx, expireMetricStateName, candidateDay.Format(time.RFC3339))
	if err != nil {
		return false, fmt.Errorf("unable to set last processed day for metrics expiration: %w", err)
	}

	return true, nil
}

// cassandraCheckExpire actually checks for metric expired or not, and perform changes.
// It assumes that Cassandra lock expirationGlobalLock is taken.
//
// This method performs changes at the end, because changes will require the Cassandra lock newMetricGlobalLock,
// and we want to hold this lock only a very short time.
//
// This purge will also remove entries from the in-memory cache.
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

	allLabelsString := make([]string, 0, len(idToLabels))
	for _, k := range idToLabels {
		allLabelsString = append(allLabelsString, k.String())
	}

	labelsToID, err := c.selectLabelsList2ID(ctx, allLabelsString)
	if err != nil {
		return fmt.Errorf("get labels2id from store : %w", err)
	}

	for _, id := range metricIDs {
		expire, ok := expires[id]

		switch {
		case !ok:
			// This shouldn't happen. It means that metric were partially created.
			// Cleanup this metric from all posting if ever it's present in this list.
			c.metrics.ExpireGhostMetric.Inc()

			bulkDelete.PrepareDelete(id, nil, false)

			continue
		case labelsToID[idToLabels[id].String()] != id:
			// This is another case of partial write.
			// Once more, we need to cleanup the metric, but we must NOT delete it from labels2id.
			c.metrics.ExpireGhostMetric.Inc()

			bulkDelete.PrepareDelete(id, idToLabels[id], true)

			continue
		case expire.After(now):
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

		// If the metric represents a shard expiration, delete the shard.
		shardStr := idToLabels[id].Get(expirationShardLabel)
		if shardStr != "" {
			shardInt, err := strconv.Atoi(shardStr)
			if err != nil {
				return fmt.Errorf("convert shard expiration label to int: %w", err)
			}

			// There is no overflow when converting to int32 because the shard was int32 when inserted.
			shard := int32(shardInt) //nolint:gosec

			err = c.deleteShard(ctx, shard)
			if err != nil {
				return fmt.Errorf("delete shard %s: %w", timeForShard(shard), err)
			}
		}

		bulkDelete.PrepareDelete(id, idToLabels[id], false)
	}

	c.newMetricGlobalLock.Lock()
	defer c.newMetricGlobalLock.Unlock()

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

// deleteShard deletes all postings associated to a shard.
func (c *CassandraIndex) deleteShard(ctx context.Context, shard int32) error {
	c.logger.Info().Msgf("Deleting shard %s", timeForShard(shard))

	// We can't run a query like "DELETE from index_postings WHERE shard = 283752"
	// because our partition key is (shard, name), so we have to know the label names to delete.

	// Delete all labels except postinglabelName which will be deleted only after the
	// other labels are successfully deleted to be able to recover if the delete failed.
	labelsToDelete := []string{allPostingLabel, maybePostingLabel}

	// Find all label names to delete.
	iter := c.store.SelectPostingByName(ctx, shard, postinglabelName)
	defer iter.Close()

	for iter.HasNext() {
		labelName, _ := iter.Next()
		labelsToDelete = append(labelsToDelete, labelName)
	}

	if iter.Err() != nil {
		return fmt.Errorf("select postings by name: %w", iter.Err())
	}

	err := c.deletePostingsByNames(ctx, shard, labelsToDelete)

	// If the postings were not found it means they were already deleted.
	if err != nil && !errors.Is(err, gocql.ErrNotFound) {
		return fmt.Errorf("delete labels from postings: %w", err)
	}

	// Delete postinglabelName separately.
	err = c.deletePostingsByNames(ctx, shard, []string{postinglabelName})
	if err != nil && !errors.Is(err, gocql.ErrNotFound) {
		return fmt.Errorf("delete special labels from postings: %w", err)
	}

	// Remove the shard from existing shards.
	updateRequest := postingUpdateRequest{
		Shard: globalShardNumber,
		Label: labels.Label{
			Name:  existingShardsLabel,
			Value: existingShardsLabel,
		},
		RemoveIDs: []uint64{uint64(shard)},
	}

	_, err = c.postingUpdate(ctx, updateRequest)
	if errors.Is(err, errBitmapEmpty) {
		err = nil
	}

	if err != nil {
		return fmt.Errorf("remove existing shards: %w", err)
	}

	return nil
}

// deletePostingsByNames is a thin wrapper around store DeletePostingsByNames.
// It handle submitting parallel queries when len(names) is too big.
func (c *CassandraIndex) deletePostingsByNames(
	ctx context.Context,
	shard int32,
	names []string,
) error {
	if len(names) < maxCQLInValue {
		return c.store.DeletePostingsByNames(ctx, shard, names)
	}

	err := c.concurrentTasks(
		ctx,
		concurrentDelete,
		func(ctx context.Context, work chan<- func() error) error {
			for start := 0; start < len(names); start += maxCQLInValue {
				end := start + maxCQLInValue
				if end > len(names) {
					end = len(names)
				}

				subNames := names[start:end]

				task := func() error {
					return c.store.DeletePostingsByNames(ctx, shard, subNames)
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

	return err
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

	// AddN / removeN will mutage the list. This list can't be mutated (it is shared by multiple req by bulk deleted,
	// the slice is reused for other update...). It safest is to always copy it here.
	var bufferIDs []uint64

	if len(job.AddIDs) > len(job.RemoveIDs) {
		bufferIDs = make([]uint64, 0, len(job.AddIDs))
	} else {
		bufferIDs = make([]uint64, 0, len(job.RemoveIDs))
	}

	bufferIDs = bufferIDs[:len(job.AddIDs)]
	copy(bufferIDs, job.AddIDs)

	_, err = bitmap.AddN(bufferIDs...)
	if err != nil {
		return nil, fmt.Errorf("update bitmap: %w", err)
	}

	bufferIDs = bufferIDs[:len(job.RemoveIDs)]
	copy(bufferIDs, job.RemoveIDs)

	_, err = bitmap.RemoveN(bufferIDs...)
	if err != nil {
		return nil, fmt.Errorf("update bitmap: %w", err)
	}

	if !bitmap.Any() {
		err := c.store.DeletePostings(ctx, job.Shard, job.Label.Name, job.Label.Value)

		// If the postings were not found it means they were already deleted.
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
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

// expirationUpdate apply an expirationUpdateRequest, which is a request to move metricID between day at which
// they are expected to expire.
// Be aware that job.AddIDs and job.RemoveIDs are mutated (do a copy if the slice is shared / reused after this call).
// The lock newMetricGlobalLock must be held while calling this function.
func (c *CassandraIndex) expirationUpdate(ctx context.Context, job expirationUpdateRequest) error {
	bitmapExpiration, err := c.cassandraGetExpirationList(ctx, job.Day)
	if err != nil {
		return err
	}

	_, err = bitmapExpiration.AddN(job.AddIDs...)
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

	_, err = bitmapExpiration.RemoveN(job.RemoveIDs...)
	if err != nil {
		return fmt.Errorf("update bitmap: %w", err)
	}

	if !bitmapExpiration.Any() {
		err := c.store.DeleteExpiration(ctx, job.Day)
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}

		return err
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
) (*metricsLabels, error) {
	results, checkMatches, err := c.postingsForMatchers(ctx, shards, matchers, directCheckThreshold)
	if err != nil {
		return nil, err
	}

	ids := bitsetToIDs(results)

	result := &metricsLabels{
		c:   c,
		ctx: ctx,
		ids: ids,
	}

	if checkMatches {
		result.labelsList, err = c.lookupLabels(ctx, ids, time.Now())
		if err != nil {
			return nil, err
		}

		newIds := make([]types.MetricID, 0, len(ids))
		newLabels := make([]labels.Labels, 0, len(ids))

		for i, id := range ids {
			lbls := result.labelsList[i]

			if matcherMatches(matchers, lbls) {
				newIds = append(newIds, id)
				newLabels = append(newLabels, lbls)
			}
		}

		result.ids = newIds
		result.labelsList = newLabels
	}

	return result, nil
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

// postingsForMatcher returns the ids that match one matcher.
// This method will not return postings for missing labels.
func (c *CassandraIndex) postingsForMatcher(
	ctx context.Context,
	shards []int32,
	m *labels.Matcher,
) (*roaring.Bitmap, error) {
	if m.Type == labels.MatchEqual {
		return c.postings(ctx, shards, m.Name, m.Value, true)
	}

	it := roaring.NewBTreeBitmap()

	// Try to convert simple OR regex to multiple MatchEqual matchers,
	// keep the usual regex behavior if there is an error.
	matchers, err := simplifyRegex(m)
	if err == nil {
		// Make the bitmap of the union of all matchers.
		for _, matcher := range matchers {
			bitset, err := c.postings(ctx, shards, matcher.Name, matcher.Value, true)
			if err != nil {
				return nil, err
			}

			it.UnionInPlace(bitset)
		}

		return it, nil
	}

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

				it.UnionInPlace(bitset)
			}
		}
	}

	return it, nil
}

// simplifyRegex converts a regex matcher into multiple equal matchers,
// it returns an error if the regex can't be converted.
// For example:
// __name__=~"(probe_ssl_last_chain_expiry_timestamp_seconds|probe_ssl_validation_success)
// -> [__name__="(probe_ssl_last_chain_expiry_timestamp_seconds), __name__="(probe_ssl_validation_success)].
func simplifyRegex(matcher *labels.Matcher) ([]*labels.Matcher, error) {
	regex, err := syntax.Parse(matcher.Value, syntax.Perl)
	if err != nil {
		return nil, err
	}

	alternateRegex, prefix, err := getOpAlternate(regex, "")
	if err != nil {
		return nil, err
	}

	// When their is too many OR clauses, we keep with usual regexp behavior.
	if len(alternateRegex.Sub) > maxOpAlertnateSubsForSimpleRegex {
		return nil, errNotASimpleRegex
	}

	matchers := make([]*labels.Matcher, 0, len(alternateRegex.Sub))

	for _, literalRegex := range alternateRegex.Sub {
		if literalRegex.Op != syntax.OpLiteral {
			return nil, errNotASimpleRegex
		}

		newMatcher, err := labels.NewMatcher(labels.MatchEqual, matcher.Name, prefix+string(literalRegex.Rune))
		if err != nil {
			return nil, err
		}

		matchers = append(matchers, newMatcher)
	}

	return matchers, nil
}

// getOpAlternate returns the OpAlternate inside a regex composed only of OR clauses,
// the prefix used by all OR clauses, and an error if the regex is not simple.
func getOpAlternate(regex *syntax.Regexp, prefix string) (*syntax.Regexp, string, error) {
	switch regex.Op { //nolint:exhaustive
	// "a|b|c"
	case syntax.OpAlternate:
		return regex, prefix, nil
	// Support regex with a capture group, e.g. "(a|b|c)"
	case syntax.OpCapture:
		if len(regex.Sub) != 1 {
			return nil, "", errNotASimpleRegex
		}

		return getOpAlternate(regex.Sub[0], prefix)
	// Support regex with repeated prefix, e.g. "(prefix_a|prefix_b)"
	case syntax.OpConcat:
		if len(regex.Sub) != 2 {
			return nil, "", errNotASimpleRegex
		}

		prefixRegex := regex.Sub[0]
		if prefixRegex.Op != syntax.OpLiteral {
			return nil, "", errNotASimpleRegex
		}

		return getOpAlternate(regex.Sub[1], string(prefixRegex.Rune))
	default:
		return nil, "", errNotASimpleRegex
	}
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

// shardForTime return the shard number for give timestamp (second from epoc)
// The shard number should only be useful for debugging or InternalUpdatePostingShards.
func shardForTime(ts int64) int32 {
	shardSize := int32(postingShardSize.Hours())

	return (int32(ts/3600) / shardSize) * shardSize
}

// timeForShard return the time for given shard ID.
func timeForShard(shard int32) time.Time {
	return time.Unix(int64(shard)*3600, 0)
}

func (c *CassandraIndex) getTimeShards(ctx context.Context, start, end time.Time, returnAll bool) ([]int32, error) {
	if err := validatedTime(start, end); err != nil {
		return nil, err
	}

	shardSize := int32(postingShardSize.Hours())
	startShard := shardForTime(start.Unix())
	endShard := shardForTime(end.Unix())

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
		if returnAll || existingShards.Contains(uint64(current)) {
			results = append(results, current)
		}

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
			for start := 0; start < len(sortedLabelsListString); start += maxCQLInValue {
				end := start + maxCQLInValue
				if end > len(sortedLabelsListString) {
					end = len(sortedLabelsListString)
				}

				subSortedLabelsListString := sortedLabelsListString[start:end]

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
			for start := 0; start < len(ids); start += maxCQLInValue {
				end := start + maxCQLInValue
				if end > len(ids) {
					end = len(ids)
				}

				subIds := ids[start:end]

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

// sortLabels returns the labels.Label list sorted by name.
func sortLabels(labelList labels.Labels) labels.Labels {
	sortedLabels := labelList.Copy()
	sort.Sort(sortedLabels)

	return sortedLabels
}

func bitsetToIDs(it *roaring.Bitmap) []types.MetricID {
	if it == nil {
		return nil
	}

	resultInts := it.Slice()
	results := make([]types.MetricID, len(resultInts))

	for i, v := range resultInts {
		results[i] = types.MetricID(v)
	}

	return results
}

// HasNext() must always be called once before each Next() (and next called once). Close() must be called at the end.
type postingIter interface {
	HasNext() bool
	Next() (string, []byte)
	Err() error
	Close()
}

type cassandraStore struct {
	connection *connection.Connection
	schemaLock sync.Locker
	metrics    *metrics
}

type cassandraByteIter struct {
	Iter    *gocql.Iter
	session *connection.SessionWrapper
	buffer  []byte
	value   string
	err     error
}

func (i *cassandraByteIter) HasNext() bool {
	if i.err != nil {
		return false
	}

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

func (i *cassandraByteIter) Close() {
	if i.session != nil {
		i.session.Close()
	}
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

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	for _, query := range queries {
		if err := session.Query(query).Consistency(gocql.All).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}

	return nil
}

// InternalDropTables drop tables used by the index.
// This should only be used in test & benchmark.
func InternalDropTables(ctx context.Context, connection *connection.Connection) error {
	queries := []string{
		"DROP TABLE IF EXISTS index_labels2id",
		"DROP TABLE IF EXISTS index_postings",
		"DROP TABLE IF EXISTS index_id2labels",
		"DROP TABLE IF EXISTS index_expiration",
	}

	session, err := connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

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

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
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

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
		"INSERT INTO index_id2labels (id, labels, expiration_date) VALUES (?, ?, ?)",
		id, sortedLabels, expiration,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) InsertLabels2ID(ctx context.Context, sortedLabelsString string, id types.MetricID) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
		"INSERT INTO index_labels2id (labels, id) VALUES (?, ?)",
		sortedLabelsString, id,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeleteLabels2ID(ctx context.Context, sortedLabelsString string) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
		"DELETE FROM index_labels2id WHERE labels = ?",
		sortedLabelsString,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeleteID2Labels(ctx context.Context, id types.MetricID) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
		"DELETE FROM index_id2labels WHERE id = ?",
		id,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeleteExpiration(ctx context.Context, day time.Time) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
		"DELETE FROM index_expiration WHERE day = ?",
		day,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeletePostings(ctx context.Context, shard int32, name string, value string) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
		"DELETE FROM index_postings WHERE shard = ? AND name = ? AND value = ?",
		shard, name, value,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) DeletePostingsByNames(ctx context.Context, shard int32, names []string) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
		"DELETE FROM index_postings WHERE shard = ? AND name IN ?",
		shard, names,
	).WithContext(ctx).Exec()
}

func (s cassandraStore) InsertExpiration(ctx context.Context, day time.Time, bitset []byte) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	return session.Query(
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

	session, err := s.connection.Session()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	iter := session.Query(
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

	err = iter.Close()

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

	session, err := s.connection.Session()
	if err != nil {
		return nil, nil, err
	}

	defer session.Close()

	iter := session.Query(
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

	err = iter.Close()

	return results, results2, err
}

func (s cassandraStore) SelectExpiration(ctx context.Context, day time.Time) ([]byte, error) {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	query := session.Query(
		"SELECT bitset FROM index_expiration WHERE day = ?",
		day,
	).WithContext(ctx)

	var buffer []byte
	err = query.Scan(&buffer)

	return buffer, err
}

func (s cassandraStore) UpdateID2LabelsExpiration(ctx context.Context, id types.MetricID, expiration time.Time) error {
	start := time.Now()

	defer func() {
		s.metrics.CassandraQueriesSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())
	}()

	session, err := s.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(
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

	session, err := s.connection.Session()
	if err != nil {
		return &cassandraByteIter{err: err}
	}

	iter := session.Query(
		"SELECT value, bitset FROM index_postings WHERE shard = ? AND name = ?",
		shard, name,
	).WithContext(ctx).Iter()

	return &cassandraByteIter{
		Iter:    iter,
		session: session,
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

	session, err := s.connection.Session()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	query := session.Query(
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

	session, err := s.connection.Session()
	if err != nil {
		return nil, nil, err
	}

	defer session.Close()

	iter := session.Query(
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

	err = iter.Close()

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
	if start.Before(indexMinValidTime) {
		return fmt.Errorf("%w: start time is too early", errTimeOutOfRange)
	}

	if start.After(indexMaxValidTime) {
		return fmt.Errorf("%w: start time is too late", errTimeOutOfRange)
	}

	if end.Before(indexMinValidTime) {
		return fmt.Errorf("%w: end time is too early", errTimeOutOfRange)
	}

	if end.After(indexMaxValidTime) {
		return fmt.Errorf("%w: end time is too late", errTimeOutOfRange)
	}

	return nil
}
