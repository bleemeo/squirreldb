package temporarystore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"squirreldb/compare"
	"squirreldb/redis/client"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultMetricKeyPrefix   = "squirreldb-metric-"
	defaultOffsetKeyPrefix   = "squirreldb-offset-"
	defaultDeadlineKeyPrefix = "squirreldb-flushdeadline-"
	defaultKnownMetricsKey   = "squirreldb-known-metrics"
	defaultTransfertKey      = "squirreldb-transfert-metrics"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[redis] ", log.LstdFlags)

type Options struct {
	Addresses []string
	Keyspace  string
}

type Redis struct {
	client  *client.Client
	metrics *metrics

	bufferPool           sync.Pool
	serializedPointsPool sync.Pool

	metricKeyPrefix   string
	offsetKeyPrefix   string
	deadlineKeyPrefix string
	knownMetricsKey   string
	transfertKey      string
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
func New(ctx context.Context, reg prometheus.Registerer, options Options) (*Redis, error) {
	redis := &Redis{
		client: &client.Client{
			Addresses: options.Addresses,
		},
		metrics: newMetrics(reg),
	}
	redis.initPool()

	redis.metricKeyPrefix = options.Keyspace + defaultMetricKeyPrefix
	redis.offsetKeyPrefix = options.Keyspace + defaultOffsetKeyPrefix
	redis.deadlineKeyPrefix = options.Keyspace + defaultDeadlineKeyPrefix
	redis.knownMetricsKey = options.Keyspace + defaultKnownMetricsKey
	redis.transfertKey = options.Keyspace + defaultTransfertKey

	cluster, err := redis.client.IsCluster(ctx)
	if err != nil {
		return nil, err
	}

	if cluster {
		logger.Println("detected cluster")
	} else {
		logger.Println("detected single")
	}

	return redis, nil
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
	result, ok := r.bufferPool.Get().(*bytes.Buffer)
	if !ok {
		return new(bytes.Buffer)
	}

	result.Reset()

	return result
}

func (r *Redis) getSerializedPoints() []serializedPoints {
	result, ok := r.serializedPointsPool.Get().([]serializedPoints)
	if !ok {
		return nil
	}

	return result
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
		r.metrics.OperationSeconds.WithLabelValues("add").Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	pipe, err := r.client.Pipeline(ctx)
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.IntCmd, len(points))
	results := make([]int, len(points))

	var addedPoints int

	buffer := r.getBuffer()
	tmp := r.getSerializedPoints()

	defer r.bufferPool.Put(buffer)
	defer r.serializedPointsPool.Put(tmp)

	for i, data := range points {
		addedPoints += len(data.Points)

		values, err := valuesFromData(data, buffer, tmp)
		if err != nil {
			return nil, err
		}

		key := r.metricKeyPrefix + metricID2String(data.ID)

		commands[i] = pipe.Append(ctx, key, string(values))
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.client.ShouldRetry(ctx) {
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

	r.metrics.OperationPoints.WithLabelValues("add").Add(float64(addedPoints))

	return results, nil
}

// GetSetPointsAndOffset implement batch.TemporaryStore interface.
func (r *Redis) GetSetPointsAndOffset( //nolint:gocyclo,cyclop
	ctx context.Context,
	points []types.MetricData,
	offsets []int,
) ([]types.MetricData, error) {
	start := time.Now()

	defer func() {
		r.metrics.OperationSeconds.WithLabelValues("set").Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	if len(points) != len(offsets) {
		msg := "GetSetPointsAndOffset: len(points) == %d must be equal to len(offsets) == %d"

		return nil, fmt.Errorf(msg, len(points), len(offsets))
	}

	pipe, err := r.client.Pipeline(ctx)
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
	defer r.serializedPointsPool.Put(tmp)

	for i, data := range points {
		writtenPointsCount += len(data.Points)

		values, err := valuesFromData(data, buffer, tmp)
		if err != nil {
			return nil, err
		}

		idStr := metricID2String(data.ID)
		metricKey := r.metricKeyPrefix + idStr
		offsetKey := r.offsetKeyPrefix + idStr

		commands[i] = pipe.GetSet(ctx, metricKey, string(values))
		ids[i] = idStr

		pipe.Expire(ctx, metricKey, defaultTTL)
		pipe.Set(ctx, offsetKey, strconv.FormatInt(int64(offsets[i]), 10), defaultTTL)
	}

	pipe.SAdd(ctx, r.knownMetricsKey, ids)

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.client.ShouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: %w", err)
		}
	}

	workBuffer := r.getSerializedPoints()
	defer r.serializedPointsPool.Put(workBuffer)

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

	r.metrics.OperationPoints.WithLabelValues("set").Add(float64(writtenPointsCount))

	return results, nil
}

// ReadPointsAndOffset implement batch.TemporaryStore interface.
func (r *Redis) ReadPointsAndOffset( //nolint:gocyclo,cyclop
	ctx context.Context,
	ids []types.MetricID,
) ([]types.MetricData, []int, error) {
	start := time.Now()

	defer func() {
		r.metrics.OperationSeconds.WithLabelValues("get").Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil, nil, nil
	}

	metrics := make([]types.MetricData, len(ids))
	writeOffsets := make([]int, len(ids))
	metricCommands := make([]*goredis.StringCmd, len(ids))
	offsetCommands := make([]*goredis.StringCmd, len(ids))

	pipe, err := r.client.Pipeline(ctx)
	if err != nil {
		return nil, nil, err
	}

	var readPointsCount int

	for i, id := range ids {
		metricKey := r.metricKeyPrefix + metricID2String(id)
		offsetKey := r.offsetKeyPrefix + metricID2String(id)

		metricCommands[i] = pipe.Get(ctx, metricKey)
		offsetCommands[i] = pipe.Get(ctx, offsetKey)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.client.ShouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, nil, fmt.Errorf("get from Redis: %w", err)
		}
	}

	tmp := r.getSerializedPoints()

	defer r.serializedPointsPool.Put(tmp)

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

	r.metrics.OperationPoints.WithLabelValues("get").Add(float64(readPointsCount))

	return metrics, writeOffsets, nil
}

// MarkToExpire implement batch.TemporaryStore interface.
func (r *Redis) MarkToExpire(ctx context.Context, ids []types.MetricID, ttl time.Duration) error {
	start := time.Now()

	defer func() {
		r.metrics.OperationSeconds.WithLabelValues("expire").Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil
	}

	idsStr := make([]string, len(ids))

	pipe, err := r.client.Pipeline(ctx)
	if err != nil {
		return err
	}

	for i, id := range ids {
		idStr := metricID2String(id)
		metricKey := r.metricKeyPrefix + idStr
		offsetKey := r.offsetKeyPrefix + idStr
		deadlineKey := r.deadlineKeyPrefix + idStr

		idsStr[i] = idStr

		pipe.Expire(ctx, metricKey, ttl)
		pipe.Expire(ctx, offsetKey, ttl)
		pipe.Expire(ctx, deadlineKey, ttl)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.client.ShouldRetry(ctx) {
			_, err = pipe.Exec(ctx)
		}

		if err != nil && !errors.Is(err, goredis.Nil) {
			return fmt.Errorf("redis: %w", err)
		}
	}

	return r.client.SRem(ctx, r.knownMetricsKey, idsStr)
}

// GetSetFlushDeadline implement batch.TemporaryStore interface.
func (r *Redis) GetSetFlushDeadline(
	ctx context.Context,
	deadlines map[types.MetricID]time.Time,
) (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		r.metrics.OperationSeconds.WithLabelValues("set-deadline").Observe(time.Since(start).Seconds())
	}()

	if len(deadlines) == 0 {
		return nil, nil
	}

	pipe, err := r.client.Pipeline(ctx)
	if err != nil {
		return nil, err
	}

	commands := make(map[types.MetricID]*goredis.StringCmd, len(deadlines))
	results := make(map[types.MetricID]time.Time, len(deadlines))

	for id, deadline := range deadlines {
		deadlineKey := r.deadlineKeyPrefix + metricID2String(id)

		commands[id] = pipe.GetSet(ctx, deadlineKey, deadline.Format(time.RFC3339))

		pipe.Expire(ctx, deadlineKey, defaultTTL)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.client.ShouldRetry(ctx) {
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

	err := r.client.SAdd(ctx, r.transfertKey, strings)
	if err != nil && !errors.Is(err, goredis.Nil) && r.client.ShouldRetry(ctx) {
		err = r.client.SAdd(ctx, r.transfertKey, strings)
	}

	return err
}

// GetTransfert implement batch.TemporaryStore interface.
func (r *Redis) GetTransfert(ctx context.Context, count int) (map[types.MetricID]time.Time, error) {
	result, err := r.client.SPopN(ctx, r.transfertKey, int64(count))
	if err != nil && !errors.Is(err, goredis.Nil) && r.client.ShouldRetry(ctx) {
		result, err = r.client.SPopN(ctx, r.transfertKey, int64(count))
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
		r.metrics.OperationSeconds.WithLabelValues("known-metrics").Observe(time.Since(start).Seconds())
	}()

	result, err := r.client.SMembers(ctx, r.knownMetricsKey)
	if err != nil && !errors.Is(err, goredis.Nil) && r.client.ShouldRetry(ctx) {
		result, err = r.client.SMembers(ctx, r.knownMetricsKey)
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

	pipe, err := r.client.Pipeline(ctx)
	if err != nil {
		return nil, err
	}

	commands := make([]*goredis.StringCmd, len(ids))
	results := make(map[types.MetricID]time.Time, len(ids))

	for i, idStr := range ids {
		deadlineKey := r.deadlineKeyPrefix + idStr

		commands[i] = pipe.Get(ctx, deadlineKey)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		if r.client.ShouldRetry(ctx) {
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
