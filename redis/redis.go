package redis

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"squirreldb/compare"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
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

	bufferPool           sync.Pool
	serializedPointsPool sync.Pool
}

type serializedPoints struct {
	Timestamp  int64
	Value      float64
	TimeToLive int64
}

const (
	serializedSize = 24
	defaultTTL     = 24 * time.Hour
)

// New creates a new Redis object.
func New(options Options) *Redis {
	clusterClient := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: options.Addresses,
	})

	redis := &Redis{
		clusterClient: clusterClient,
	}
	redis.initPool()

	if len(options.Addresses) == 1 {
		client := goredis.NewClient(&goredis.Options{
			Addr: options.Addresses[0],
		})
		redis.singleClient = client
	}

	return redis
}

func (r *Redis) initPool() {
	r.bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	r.serializedPointsPool = sync.Pool{
		New: func() interface{} {
			return make([]serializedPoints, 1024)
		},
	}
}

func (r *Redis) getBuffer() *bytes.Buffer {
	result := r.bufferPool.Get().(*bytes.Buffer)
	result.Reset()

	return result
}

func (r *Redis) getSerializedPoints() []serializedPoints {
	result := r.serializedPointsPool.Get().([]serializedPoints)

	return result
}

func (r *Redis) fixClient(ctx context.Context) error {
	r.l.Lock()
	defer r.l.Unlock()

	if r.singleClient == nil || r.clusterClient == nil {
		return nil
	}

	singleErr := r.singleClient.Ping(ctx).Err()
	clusterErr := r.clusterClient.Ping(ctx).Err()
	infoErr := r.singleClient.ClusterInfo(ctx).Err()

	switch {
	case clusterErr == nil:
		logger.Println("detected cluster")

		r.singleClient = nil
	case singleErr == nil && infoErr != nil:
		logger.Println("detected single")

		r.clusterClient = nil
	default:
		return fmt.Errorf("ping redis: %w", clusterErr)
	}

	return nil
}

// when using redis cluster and a master is down, go-redis took quiet some time
// to discovery it.
// If shouldRetry return true, it means it tried to refresh the cluster nodes state
// and last command may be retried.
func (r *Redis) shouldRetry(ctx context.Context) bool {
	r.l.Lock()
	defer r.l.Unlock()

	if r.lastReload.IsZero() || time.Since(r.lastReload) > 10*time.Second {
		r.clusterClient.ReloadState(ctx)
		r.lastReload = time.Now()

		// ReloadState is asynchronious :(
		// Wait a bit in order for ReloadState to run
		time.Sleep(100 * time.Millisecond)

		return true
	}

	return false
}

func (r *Redis) pipeline(ctx context.Context) (goredis.Pipeliner, error) {
	if err := r.fixClient(ctx); err != nil {
		return nil, err
	}

	if r.clusterClient != nil {
		return r.clusterClient.Pipeline(), nil
	}

	return r.singleClient.Pipeline(), nil
}

func (r *Redis) sAdd(ctx context.Context, key string, members ...interface{}) error {
	if err := r.fixClient(ctx); err != nil {
		return err
	}

	if r.clusterClient != nil {
		_, err := r.clusterClient.SAdd(ctx, key, members...).Result()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}

		return nil
	}

	_, err := r.singleClient.SAdd(ctx, key, members...).Result()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	return nil
}

func (r *Redis) sRem(ctx context.Context, key string, members ...interface{}) error {
	if err := r.fixClient(ctx); err != nil {
		return err
	}

	if r.clusterClient != nil {
		_, err := r.clusterClient.SRem(ctx, key, members...).Result()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}

		return nil
	}

	_, err := r.singleClient.SRem(ctx, key, members...).Result()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	return nil
}

func (r *Redis) sPopN(ctx context.Context, key string, count int64) ([]string, error) {
	if err := r.fixClient(ctx); err != nil {
		return nil, err
	}

	if r.clusterClient != nil {
		return r.clusterClient.SPopN(ctx, key, count).Result()
	}

	return r.singleClient.SPopN(ctx, key, count).Result()
}

func (r *Redis) sMembers(ctx context.Context, key string) ([]string, error) {
	if err := r.fixClient(ctx); err != nil {
		return nil, err
	}

	if r.clusterClient != nil {
		return r.clusterClient.SMembers(ctx, key).Result()
	}

	return r.singleClient.SMembers(ctx, key).Result()
}

func metricID2String(id types.MetricID) string {
	return strconv.FormatInt(int64(id), 36)
}

func string2MetricID(input string) (types.MetricID, error) {
	v, err := strconv.ParseInt(input, 36, 0)
	if err != nil {
		return 0, fmt.Errorf("convert metric ID: %w", err)
	}

	return types.MetricID(v), nil
}

// Append implement batch.TemporaryStore interface.
func (r *Redis) Append(ctx context.Context, points []types.MetricData) ([]int, error) {
	start := time.Now()

	defer func() {
		operationSecondsAdd.Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	pipe, err := r.pipeline(ctx)
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.IntCmd, len(points))
	results := make([]int, len(points))

	var addedPoints int

	buffer := r.getBuffer()
	tmp := r.getSerializedPoints()

	defer r.bufferPool.Put(buffer)
	defer r.serializedPointsPool.Put(tmp) // nolint: staticcheck

	for i, data := range points {
		addedPoints += len(data.Points)

		values, err := valuesFromData(data, buffer, tmp)
		if err != nil {
			return nil, err
		}

		key := metricKeyPrefix + metricID2String(data.ID)

		commands[i] = pipe.Append(ctx, key, string(values))
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.clusterClient != nil && r.shouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis failed: %w", err)
		}
	}

	for i := range points {
		tmp, err := commands[i].Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}

		if !errors.Is(err, goredis.Nil) {
			results[i] = int(tmp / serializedSize)
		}
	}

	operationPointssAdd.Add(float64(addedPoints))

	return results, nil
}

// GetSetPointsAndOffset implement batch.TemporaryStore interface.
func (r *Redis) GetSetPointsAndOffset(ctx context.Context, points []types.MetricData, offsets []int) ([]types.MetricData, error) {
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

	pipe, err := r.pipeline(ctx)
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.StringCmd, len(points))
	results := make([]types.MetricData, len(points))
	ids := make([]string, len(points))

	var writtenPointsCount int

	buffer := r.getBuffer()
	tmp := r.getSerializedPoints()

	defer r.bufferPool.Put(buffer)
	defer r.serializedPointsPool.Put(tmp) // nolint: staticcheck

	for i, data := range points {
		writtenPointsCount += len(data.Points)

		values, err := valuesFromData(data, buffer, tmp)
		if err != nil {
			return nil, err
		}

		idStr := metricID2String(data.ID)
		metricKey := metricKeyPrefix + idStr
		offsetKey := offsetKeyPrefix + idStr

		commands[i] = pipe.GetSet(ctx, metricKey, string(values))
		ids[i] = idStr

		pipe.Expire(ctx, metricKey, defaultTTL)
		pipe.Set(ctx, offsetKey, strconv.FormatInt(int64(offsets[i]), 10), defaultTTL)
	}

	pipe.SAdd(ctx, knownMetricsKey, ids)

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.clusterClient != nil && r.shouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}
	}

	workBuffer := r.getSerializedPoints()
	defer r.serializedPointsPool.Put(workBuffer) // nolint: staticcheck

	for i, data := range points {
		tmp, err := commands[i].Bytes()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}

		if !errors.Is(err, goredis.Nil) {
			results[i], err = dataFromValues(data.ID, tmp, workBuffer)
			if err != nil {
				return nil, err
			}
		}
	}

	operationPointssSet.Add(float64(writtenPointsCount))

	return results, nil
}

// ReadPointsAndOffset implement batch.TemporaryStore interface.
func (r *Redis) ReadPointsAndOffset(ctx context.Context, ids []types.MetricID) ([]types.MetricData, []int, error) {
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

	pipe, err := r.pipeline(ctx)
	if err != nil {
		return nil, nil, err
	}

	var readPointsCount int

	for i, id := range ids {
		metricKey := metricKeyPrefix + metricID2String(id)
		offsetKey := offsetKeyPrefix + metricID2String(id)

		metricCommands[i] = pipe.Get(ctx, metricKey)
		offsetCommands[i] = pipe.Get(ctx, offsetKey)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.clusterClient != nil && r.shouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, nil, fmt.Errorf("get from Redis: %w", err)
		}
	}

	tmp := r.getSerializedPoints()

	defer r.serializedPointsPool.Put(tmp) // nolint: staticcheck

	for i, id := range ids {
		values, err := metricCommands[i].Bytes()

		if (err != nil) && (!errors.Is(err, goredis.Nil)) {
			return nil, nil, fmt.Errorf("redis: %w", err)
		}

		if !errors.Is(err, goredis.Nil) {
			metrics[i], err = dataFromValues(id, values, tmp)

			if err != nil {
				return nil, nil, err
			}

			readPointsCount += len(values) / serializedSize
		} else {
			// err == goredis.Nil. No points for this metrics
			metrics[i] = types.MetricData{
				ID: id,
			}
		}

		writeOffsets[i], err = offsetCommands[i].Int()

		if (err != nil) && (!errors.Is(err, goredis.Nil)) {
			return nil, nil, fmt.Errorf("update bitmap: %w", err)
		}
	}

	operationPointssGet.Add(float64(readPointsCount))

	return metrics, writeOffsets, nil
}

// MarkToExpire implement batch.TemporaryStore interface.
func (r *Redis) MarkToExpire(ctx context.Context, ids []types.MetricID, ttl time.Duration) error {
	start := time.Now()

	defer func() {
		operationSecondsExpire.Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil
	}

	idsStr := make([]string, len(ids))

	pipe, err := r.pipeline(ctx)
	if err != nil {
		return err
	}

	for i, id := range ids {
		idStr := metricID2String(id)
		metricKey := metricKeyPrefix + idStr
		offsetKey := offsetKeyPrefix + idStr
		deadlineKey := deadlineKeyPrefix + idStr

		idsStr[i] = idStr

		pipe.Expire(ctx, metricKey, ttl)
		pipe.Expire(ctx, offsetKey, ttl)
		pipe.Expire(ctx, deadlineKey, ttl)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.clusterClient != nil && r.shouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return fmt.Errorf("redis: %w", err)
		}
	}

	if err := r.sRem(ctx, knownMetricsKey, idsStr); err != nil {
		return err
	}

	return nil
}

// GetSetFlushDeadline implement batch.TemporaryStore interface.
func (r *Redis) GetSetFlushDeadline(ctx context.Context, deadlines map[types.MetricID]time.Time) (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsSetDeadline.Observe(time.Since(start).Seconds())
	}()

	if len(deadlines) == 0 {
		return nil, nil
	}

	pipe, err := r.pipeline(ctx)
	if err != nil {
		return nil, err
	}

	commands := make(map[types.MetricID]*goredis.StringCmd, len(deadlines))
	results := make(map[types.MetricID]time.Time, len(deadlines))

	for id, deadline := range deadlines {
		deadlineKey := deadlineKeyPrefix + metricID2String(id)

		commands[id] = pipe.GetSet(ctx, deadlineKey, deadline.Format(time.RFC3339))

		pipe.Expire(ctx, deadlineKey, defaultTTL)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.clusterClient != nil && r.shouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}
	}

	for id := range deadlines {
		tmp, err := commands[id].Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}

		if !errors.Is(err, goredis.Nil) {
			results[id], err = time.Parse(time.RFC3339, tmp)
			if err != nil {
				return nil, fmt.Errorf("parse time: %w", err)
			}
		}
	}

	return results, nil
}

// AddToTransfert implement batch.TemporaryStore interface.
func (r *Redis) AddToTransfert(ctx context.Context, ids []types.MetricID) error {
	if len(ids) == 0 {
		return nil
	}

	strings := make([]string, len(ids))

	for i, id := range ids {
		strings[i] = metricID2String(id)
	}

	err := r.sAdd(ctx, transfertKey, strings)
	if err != nil && !errors.Is(err, goredis.Nil) && r.clusterClient != nil && r.shouldRetry(ctx) {
		err = r.sAdd(ctx, transfertKey, strings)
	}

	return err
}

// GetTransfert implement batch.TemporaryStore interface.
func (r *Redis) GetTransfert(ctx context.Context, count int) (map[types.MetricID]time.Time, error) {
	result, err := r.sPopN(ctx, transfertKey, int64(count))
	if err != nil && !errors.Is(err, goredis.Nil) && r.clusterClient != nil && r.shouldRetry(ctx) {
		result, err = r.sPopN(ctx, transfertKey, int64(count))
	}

	if err != nil && !errors.Is(err, goredis.Nil) {
		return nil, err
	}

	return r.getFlushDeadline(ctx, result)
}

// GetAllKnownMetrics implement batch.TemporaryStore interface.
func (r *Redis) GetAllKnownMetrics(ctx context.Context) (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsKnownMetrics.Observe(time.Since(start).Seconds())
	}()

	result, err := r.sMembers(ctx, knownMetricsKey)
	if err != nil && !errors.Is(err, goredis.Nil) && r.clusterClient != nil && r.shouldRetry(ctx) {
		result, err = r.sMembers(ctx, knownMetricsKey)
	}

	if err != nil && !errors.Is(err, goredis.Nil) {
		return nil, err
	}

	return r.getFlushDeadline(ctx, result)
}

func (r *Redis) getFlushDeadline(ctx context.Context, ids []string) (map[types.MetricID]time.Time, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	pipe, err := r.pipeline(ctx)
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.StringCmd, len(ids))
	results := make(map[types.MetricID]time.Time, len(ids))

	for i, idStr := range ids {
		deadlineKey := deadlineKeyPrefix + idStr

		commands[i] = pipe.Get(ctx, deadlineKey)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.clusterClient != nil && r.shouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}
	}

	for i, idStr := range ids {
		id, err := string2MetricID(idStr)
		if err != nil {
			return nil, err
		}

		tmp, err := commands[i].Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}

		if !errors.Is(err, goredis.Nil) {
			results[id], err = time.Parse(time.RFC3339, tmp)

			if err != nil {
				return nil, fmt.Errorf("parse time: %w", err)
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
func dataFromValues(id types.MetricID, values []byte, dataSerialized []serializedPoints) (types.MetricData, error) {
	data := types.MetricData{}
	buffer := bytes.NewReader(values)
	pointCount := len(values) / serializedSize

	if cap(dataSerialized) < pointCount {
		dataSerialized = make([]serializedPoints, pointCount)
	} else {
		dataSerialized = dataSerialized[:pointCount]
	}

	err := binary.Read(buffer, binary.BigEndian, &dataSerialized)
	if err != nil {
		return data, fmt.Errorf("deserialize points: %w", err)
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
func valuesFromData(data types.MetricData, buffer *bytes.Buffer, dataSerialized []serializedPoints) ([]byte, error) {
	if buffer == nil || len(data.Points) > 1024 {
		buffer = new(bytes.Buffer)
	} else {
		buffer.Reset()
	}

	buffer.Grow(len(data.Points) * serializedSize)

	if cap(dataSerialized) < len(data.Points) {
		dataSerialized = make([]serializedPoints, len(data.Points))
	} else {
		dataSerialized = dataSerialized[:len(data.Points)]
	}

	for i, point := range data.Points {
		dataSerialized[i] = serializedPoints{
			Timestamp:  point.Timestamp,
			Value:      point.Value,
			TimeToLive: data.TimeToLive,
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, dataSerialized); err != nil {
		return nil, fmt.Errorf("deserialize points: %w", err)
	}

	return buffer.Bytes(), nil
}
