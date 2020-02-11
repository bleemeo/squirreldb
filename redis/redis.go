package redis

import (
	"fmt"
	"strconv"

	goredis "github.com/go-redis/redis"

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

type Options struct {
	Address string
}

type Redis struct {
	client *goredis.Client
}

type serializedPoints struct {
	Timestamp  int64
	Value      float64
	TimeToLive int64
}

const serializedSize = 24
const defaultTTL = 24 * time.Hour

// New creates a new Redis object
func New(options Options) *Redis {
	client := goredis.NewClient(&goredis.Options{
		Addr: options.Address,
	})

	redis := &Redis{
		client: client,
	}

	return redis
}

func metricID2String(id types.MetricID) string {
	return strconv.FormatInt(int64(id), 36)
}

func string2MetricID(input string) (types.MetricID, error) {
	v, err := strconv.ParseInt(input, 36, 0)
	return types.MetricID(v), err
}

// Append implement batch.TemporaryStore interface
func (r *Redis) Append(points []types.MetricData) ([]int, error) {
	start := time.Now()

	defer func() {
		operationSecondsAdd.Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
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
		return nil, err
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

// GetSetPointsAndOffset implement batch.TemporaryStore interface
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

	pipe := r.client.Pipeline()
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
		return nil, err
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

// ReadPointsAndOffset implement batch.TemporaryStore interface
func (r *Redis) ReadPointsAndOffset(ids []types.MetricID) ([]types.MetricData, []int, error) {
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
	pipe := r.client.Pipeline()

	var readPointsCount int

	for i, id := range ids {
		metricKey := metricKeyPrefix + metricID2String(id)
		offsetKey := offsetKeyPrefix + metricID2String(id)

		metricCommands[i] = pipe.Get(metricKey)
		offsetCommands[i] = pipe.Get(offsetKey)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return nil, nil, err
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
		}

		writeOffsets[i], err = offsetCommands[i].Int()

		if (err != nil) && (err != goredis.Nil) {
			return nil, nil, err
		}
	}

	operationPointssGet.Add(float64(readPointsCount))

	return metrics, writeOffsets, nil
}

// MarkToExpire implement batch.TemporaryStore interface
func (r *Redis) MarkToExpire(ids []types.MetricID, ttl time.Duration) error {
	start := time.Now()

	defer func() {
		operationSecondsExpire.Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil
	}

	idsStr := make([]string, len(ids))
	pipe := r.client.Pipeline()

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

	pipe.SRem(knownMetricsKey, idsStr)

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return err
	}

	return nil
}

// GetSetFlushDeadline implement batch.TemporaryStore interface
func (r *Redis) GetSetFlushDeadline(deadlines map[types.MetricID]time.Time) (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsSetDeadline.Observe(time.Since(start).Seconds())
	}()

	if len(deadlines) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	commands := make(map[types.MetricID]*goredis.StringCmd, len(deadlines))
	results := make(map[types.MetricID]time.Time, len(deadlines))

	for id, deadline := range deadlines {
		deadlineKey := deadlineKeyPrefix + metricID2String(id)

		commands[id] = pipe.GetSet(deadlineKey, deadline.Format(time.RFC3339))

		pipe.Expire(deadlineKey, defaultTTL)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return nil, err
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

// AddToTransfert implement batch.TemporaryStore interface
func (r *Redis) AddToTransfert(ids []types.MetricID) error {
	if len(ids) == 0 {
		return nil
	}

	strings := make([]string, len(ids))

	for i, id := range ids {
		strings[i] = metricID2String(id)
	}

	_, err := r.client.SAdd(transfertKey, strings).Result()

	return err
}

// GetTransfert implement batch.TemporaryStore interface
func (r *Redis) GetTransfert(count int) (map[types.MetricID]time.Time, error) {
	result, err := r.client.SPopN(transfertKey, int64(count)).Result()

	if err != nil && err != goredis.Nil {
		return nil, err
	}

	return r.getFlushDeadline(result)
}

// GetAllKnownMetrics implement batch.TemporaryStore interface
func (r *Redis) GetAllKnownMetrics() (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsKnownMetrics.Observe(time.Since(start).Seconds())
	}()

	result, err := r.client.SMembers(knownMetricsKey).Result()

	if err != nil && err != goredis.Nil {
		return nil, err
	}

	return r.getFlushDeadline(result)
}

func (r *Redis) getFlushDeadline(ids []string) (map[types.MetricID]time.Time, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	commands := make([]*goredis.StringCmd, len(ids))
	results := make(map[types.MetricID]time.Time, len(ids))

	for i, idStr := range ids {
		deadlineKey := deadlineKeyPrefix + idStr

		commands[i] = pipe.Get(deadlineKey)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return nil, err
	}

	for i, idStr := range ids {
		tmp, err := commands[i].Result()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			id, err := string2MetricID(idStr)
			if err != nil {
				return nil, err
			}

			results[id], err = time.Parse(time.RFC3339, tmp)

			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// Return data from bytes values
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

// Return bytes values from data
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
