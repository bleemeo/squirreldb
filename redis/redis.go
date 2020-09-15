package redis

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	goredis "github.com/go-redis/redis/v7"

	"bytes"
	"encoding/binary"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

const (
	metricKeyPrefix   = "squirreldb-metric-"
	offsetKeyPrefix   = "squirreldb-offset-"
	deadlineKeyPrefix = "squirreldb-flushdeadline-"
	knownMetricsKey   = "squirreldb-known-metrics"
	transfertKey      = "squirreldb-transfert-metrics"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[redis] ", log.LstdFlags)

type Options struct {
	Addresses []string
}

type Redis struct {
	l             sync.Mutex
	lastReload    time.Time
	singleClient  *goredis.Client
	clusterClient *goredis.ClusterClient
}

type serializedPoints struct {
	Timestamp  int64
	Value      float64
	TimeToLive int64
}

const serializedSize = 24
const defaultTTL = 24 * time.Hour

// New creates a new Redis object.
func New(options Options) *Redis {
	clusterClient := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: options.Addresses,
	})

	redis := &Redis{
		clusterClient: clusterClient,
	}

	if len(options.Addresses) == 1 {
		client := goredis.NewClient(&goredis.Options{
			Addr: options.Addresses[0],
		})
		redis.singleClient = client
	}

	return redis
}

func (r *Redis) fixClient() error {
	r.l.Lock()
	defer r.l.Unlock()

	if r.singleClient == nil || r.clusterClient == nil {
		return nil
	}

	singleErr := r.singleClient.Ping().Err()
	clusterErr := r.clusterClient.Ping().Err()
	infoErr := r.singleClient.ClusterInfo().Err()

	switch {
	case clusterErr == nil:
		logger.Println("detected cluster")

		r.singleClient = nil
	case singleErr == nil && infoErr != nil:
		logger.Println("detected single")

		r.clusterClient = nil
	default:
		return clusterErr
	}

	return nil
}

// when using redis cluster and a master is down, go-redis took quiet some time
// to discovery it.
// If shouldRetry return true, it means it tried to refresh the cluster nodes state
// and last command may be retried.
func (r *Redis) shouldRetry() bool {
	r.l.Lock()
	defer r.l.Unlock()

	if r.lastReload.IsZero() || time.Since(r.lastReload) > 10*time.Second {
		err := r.clusterClient.ReloadState()
		r.lastReload = time.Now()

		return err == nil
	}

	return false
}

func (r *Redis) pipeline() (goredis.Pipeliner, error) {
	if err := r.fixClient(); err != nil {
		return nil, err
	}

	if r.clusterClient != nil {
		return r.clusterClient.Pipeline(), nil
	}

	return r.singleClient.Pipeline(), nil
}

func (r *Redis) sAdd(key string, members ...interface{}) error {
	if err := r.fixClient(); err != nil {
		return err
	}

	if r.clusterClient != nil {
		_, err := r.clusterClient.SAdd(key, members...).Result()

		return err
	}

	_, err := r.singleClient.SAdd(key, members...).Result()

	return err
}

func (r *Redis) sRem(key string, members ...interface{}) error {
	if err := r.fixClient(); err != nil {
		return err
	}

	if r.clusterClient != nil {
		_, err := r.clusterClient.SRem(key, members...).Result()

		return err
	}

	_, err := r.singleClient.SRem(key, members...).Result()

	return err
}

func (r *Redis) sPopN(key string, count int64) ([]string, error) {
	if err := r.fixClient(); err != nil {
		return nil, err
	}

	if r.clusterClient != nil {
		return r.clusterClient.SPopN(key, count).Result()
	}

	return r.singleClient.SPopN(key, count).Result()
}

func (r *Redis) sMembers(key string) ([]string, error) {
	if err := r.fixClient(); err != nil {
		return nil, err
	}

	if r.clusterClient != nil {
		return r.clusterClient.SMembers(key).Result()
	}

	return r.singleClient.SMembers(key).Result()
}

func metricID2String(id types.MetricID) string {
	return strconv.FormatInt(int64(id), 36)
}

func string2MetricID(input string) (types.MetricID, error) {
	v, err := strconv.ParseInt(input, 36, 0)
	return types.MetricID(v), err
}

// Append implement batch.TemporaryStore interface.
func (r *Redis) Append(points []types.MetricData) ([]int, error) {
	start := time.Now()

	defer func() {
		operationSecondsAdd.Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	pipe, err := r.pipeline()
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.IntCmd, len(points))
	results := make([]int, len(points))

	var addedPoints int

	for i, data := range points {
		addedPoints += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return nil, err
		}

		key := metricKeyPrefix + metricID2String(data.ID)

		commands[i] = pipe.Append(key, string(values))
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		if r.clusterClient != nil && r.shouldRetry() {
			_, err = pipe.Exec()
		}

		if err != nil && err != goredis.Nil {
			return nil, err
		}
	}

	for i := range points {
		tmp, err := commands[i].Result()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			results[i] = int(tmp / serializedSize)
		}
	}

	operationPointssAdd.Add(float64(addedPoints))

	return results, nil
}

// GetSetPointsAndOffset implement batch.TemporaryStore interface.
func (r *Redis) GetSetPointsAndOffset(points []types.MetricData, offsets []int) ([]types.MetricData, error) {
	start := time.Now()

	defer func() {
		operationSecondsSet.Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	if len(points) != len(offsets) {
		return nil, fmt.Errorf("GetSetPointsAndOffset: len(points) == %d must be equal to len(offsets) == %d", len(points), len(offsets))
	}

	pipe, err := r.pipeline()
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.StringCmd, len(points))
	results := make([]types.MetricData, len(points))
	ids := make([]string, len(points))

	var writtenPointsCount int

	for i, data := range points {
		writtenPointsCount += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return nil, err
		}

		idStr := metricID2String(data.ID)
		metricKey := metricKeyPrefix + idStr
		offsetKey := offsetKeyPrefix + idStr

		commands[i] = pipe.GetSet(metricKey, string(values))
		ids[i] = idStr

		pipe.Expire(metricKey, defaultTTL)
		pipe.Set(offsetKey, strconv.FormatInt(int64(offsets[i]), 10), defaultTTL)
	}

	pipe.SAdd(knownMetricsKey, ids)

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		if r.clusterClient != nil && r.shouldRetry() {
			_, err = pipe.Exec()
		}

		if err != nil && err != goredis.Nil {
			return nil, err
		}
	}

	for i, data := range points {
		tmp, err := commands[i].Bytes()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			results[i], err = dataFromValues(data.ID, tmp)
			if err != nil {
				return nil, err
			}
		}
	}

	operationPointssSet.Add(float64(writtenPointsCount))

	return results, nil
}

// ReadPointsAndOffset implement batch.TemporaryStore interface.
func (r *Redis) ReadPointsAndOffset(ids []types.MetricID) ([]types.MetricData, []int, error) { // nolint: gocognit
	start := time.Now()

	defer func() {
		operationSecondsGet.Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil, nil, nil
	}

	metrics := make([]types.MetricData, len(ids))
	writeOffsets := make([]int, len(ids))
	metricCommands := make([]*goredis.StringCmd, len(ids))
	offsetCommands := make([]*goredis.StringCmd, len(ids))

	pipe, err := r.pipeline()
	if err != nil {
		return nil, nil, err
	}

	var readPointsCount int

	for i, id := range ids {
		metricKey := metricKeyPrefix + metricID2String(id)
		offsetKey := offsetKeyPrefix + metricID2String(id)

		metricCommands[i] = pipe.Get(metricKey)
		offsetCommands[i] = pipe.Get(offsetKey)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		if r.clusterClient != nil && r.shouldRetry() {
			_, err = pipe.Exec()
		}

		if err != nil && err != goredis.Nil {
			return nil, nil, err
		}
	}

	for i, id := range ids {
		values, err := metricCommands[i].Bytes()

		if (err != nil) && (err != goredis.Nil) {
			return nil, nil, err
		}

		if err != goredis.Nil {
			metrics[i], err = dataFromValues(id, values)

			if err != nil {
				return nil, nil, err
			}

			readPointsCount += len(values) / serializedSize
		} else {
			metrics[i] = types.MetricData{
				ID: id,
			}
		}

		writeOffsets[i], err = offsetCommands[i].Int()

		if (err != nil) && (err != goredis.Nil) {
			return nil, nil, err
		}
	}

	operationPointssGet.Add(float64(readPointsCount))

	return metrics, writeOffsets, nil
}

// MarkToExpire implement batch.TemporaryStore interface.
func (r *Redis) MarkToExpire(ids []types.MetricID, ttl time.Duration) error {
	start := time.Now()

	defer func() {
		operationSecondsExpire.Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil
	}

	idsStr := make([]string, len(ids))

	pipe, err := r.pipeline()
	if err != nil {
		return err
	}

	for i, id := range ids {
		idStr := metricID2String(id)
		metricKey := metricKeyPrefix + idStr
		offsetKey := offsetKeyPrefix + idStr
		deadlineKey := deadlineKeyPrefix + idStr

		idsStr[i] = idStr

		pipe.Expire(metricKey, ttl)
		pipe.Expire(offsetKey, ttl)
		pipe.Expire(deadlineKey, ttl)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		if r.clusterClient != nil && r.shouldRetry() {
			_, err = pipe.Exec()
		}

		if err != nil && err != goredis.Nil {
			return err
		}
	}

	if err := r.sRem(knownMetricsKey, idsStr); err != nil {
		return err
	}

	return nil
}

// GetSetFlushDeadline implement batch.TemporaryStore interface.
func (r *Redis) GetSetFlushDeadline(deadlines map[types.MetricID]time.Time) (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsSetDeadline.Observe(time.Since(start).Seconds())
	}()

	if len(deadlines) == 0 {
		return nil, nil
	}

	pipe, err := r.pipeline()
	if err != nil {
		return nil, err
	}

	commands := make(map[types.MetricID]*goredis.StringCmd, len(deadlines))
	results := make(map[types.MetricID]time.Time, len(deadlines))

	for id, deadline := range deadlines {
		deadlineKey := deadlineKeyPrefix + metricID2String(id)

		commands[id] = pipe.GetSet(deadlineKey, deadline.Format(time.RFC3339))

		pipe.Expire(deadlineKey, defaultTTL)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		if r.clusterClient != nil && r.shouldRetry() {
			_, err = pipe.Exec()
		}

		if err != nil && err != goredis.Nil {
			return nil, err
		}
	}

	for id := range deadlines {
		tmp, err := commands[id].Result()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			results[id], err = time.Parse(time.RFC3339, tmp)
			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// AddToTransfert implement batch.TemporaryStore interface.
func (r *Redis) AddToTransfert(ids []types.MetricID) error {
	if len(ids) == 0 {
		return nil
	}

	strings := make([]string, len(ids))

	for i, id := range ids {
		strings[i] = metricID2String(id)
	}

	err := r.sAdd(transfertKey, strings)
	if err != nil && err != goredis.Nil && r.clusterClient != nil && r.shouldRetry() {
		err = r.sAdd(transfertKey, strings)
	}

	return err
}

// GetTransfert implement batch.TemporaryStore interface.
func (r *Redis) GetTransfert(count int) (map[types.MetricID]time.Time, error) {
	result, err := r.sPopN(transfertKey, int64(count))
	if err != nil && err != goredis.Nil && r.clusterClient != nil && r.shouldRetry() {
		result, err = r.sPopN(transfertKey, int64(count))
	}

	if err != nil && err != goredis.Nil {
		return nil, err
	}

	return r.getFlushDeadline(result)
}

// GetAllKnownMetrics implement batch.TemporaryStore interface.
func (r *Redis) GetAllKnownMetrics() (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsKnownMetrics.Observe(time.Since(start).Seconds())
	}()

	result, err := r.sMembers(knownMetricsKey)
	if err != nil && err != goredis.Nil && r.clusterClient != nil && r.shouldRetry() {
		result, err = r.sMembers(knownMetricsKey)
	}

	if err != nil && err != goredis.Nil {
		return nil, err
	}

	return r.getFlushDeadline(result)
}

func (r *Redis) getFlushDeadline(ids []string) (map[types.MetricID]time.Time, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	pipe, err := r.pipeline()
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.StringCmd, len(ids))
	results := make(map[types.MetricID]time.Time, len(ids))

	for i, idStr := range ids {
		deadlineKey := deadlineKeyPrefix + idStr

		commands[i] = pipe.Get(deadlineKey)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		if r.clusterClient != nil && r.shouldRetry() {
			_, err = pipe.Exec()
		}

		if err != nil && err != goredis.Nil {
			return nil, err
		}
	}

	for i, idStr := range ids {
		id, err := string2MetricID(idStr)
		if err != nil {
			return nil, err
		}

		tmp, err := commands[i].Result()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			results[id], err = time.Parse(time.RFC3339, tmp)

			if err != nil {
				return nil, err
			}
		} else {
			// The metric is known but don't have any deadline key ?
			// This could happen as race-condition on Redis cluster. On
			// this setup, the update of differrent key are not longer
			// atomic (because keys could be on different server).

			// Assume this key is expired, this will recover its situtation
			results[id] = time.Time{}
		}
	}

	return results, nil
}

// Return data from bytes values.
func dataFromValues(id types.MetricID, values []byte) (types.MetricData, error) {
	data := types.MetricData{}
	buffer := bytes.NewReader(values)

	dataSerialized := make([]serializedPoints, len(values)/24)

	err := binary.Read(buffer, binary.BigEndian, &dataSerialized)
	if err != nil {
		return data, err
	}

	data.Points = make([]types.MetricPoint, len(dataSerialized))
	for i, point := range dataSerialized {
		data.ID = id
		data.Points[i] = types.MetricPoint{
			Timestamp: point.Timestamp,
			Value:     point.Value,
		}
		data.TimeToLive = compare.MaxInt64(data.TimeToLive, point.TimeToLive)
	}

	return data, nil
}

// Return bytes values from data.
func valuesFromData(data types.MetricData) ([]byte, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(len(data.Points) * 24)

	dataSerialized := make([]serializedPoints, len(data.Points))
	for i, point := range data.Points {
		dataSerialized[i] = serializedPoints{
			Timestamp:  point.Timestamp,
			Value:      point.Value,
			TimeToLive: data.TimeToLive,
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, dataSerialized); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
