package redis

import (
	"fmt"
	"strconv"

	goredis "github.com/go-redis/redis"
	gouuid "github.com/gofrs/uuid"

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

		key := metricKeyPrefix + data.UUID.String()

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
	uuids := make([]string, len(points))

	var writtenPointsCount int

	for i, data := range points {
		writtenPointsCount += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return nil, err
		}

		uuidStr := data.UUID.String()
		metricKey := metricKeyPrefix + uuidStr
		offsetKey := offsetKeyPrefix + uuidStr

		commands[i] = pipe.GetSet(metricKey, string(values))
		uuids[i] = uuidStr

		pipe.Expire(metricKey, defaultTTL)
		pipe.Set(offsetKey, strconv.FormatInt(int64(offsets[i]), 10), defaultTTL)
	}

	pipe.SAdd(knownMetricsKey, uuids)

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return nil, err
	}

	for i, data := range points {
		tmp, err := commands[i].Bytes()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			results[i], err = dataFromValues(data.UUID, tmp)
			if err != nil {
				return nil, err
			}
		}
	}

	operationPointssSet.Add(float64(writtenPointsCount))

	return results, nil
}

// ReadPointsAndOffset implement batch.TemporaryStore interface
func (r *Redis) ReadPointsAndOffset(uuids []gouuid.UUID) ([]types.MetricData, []int, error) {
	start := time.Now()

	defer func() {
		operationSecondsGet.Observe(time.Since(start).Seconds())
	}()

	if len(uuids) == 0 {
		return nil, nil, nil
	}

	metrics := make([]types.MetricData, len(uuids))
	writeOffsets := make([]int, len(uuids))
	metricCommands := make([]*goredis.StringCmd, len(uuids))
	offsetCommands := make([]*goredis.StringCmd, len(uuids))
	pipe := r.client.Pipeline()

	var readPointsCount int

	for i, uuid := range uuids {
		metricKey := metricKeyPrefix + uuid.String()
		offsetKey := offsetKeyPrefix + uuid.String()

		metricCommands[i] = pipe.Get(metricKey)
		offsetCommands[i] = pipe.Get(offsetKey)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return nil, nil, err
	}

	for i, uuid := range uuids {
		values, err := metricCommands[i].Bytes()

		if (err != nil) && (err != goredis.Nil) {
			return nil, nil, err
		}

		if err != goredis.Nil {
			metrics[i], err = dataFromValues(uuid, values)

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
func (r *Redis) MarkToExpire(uuids []gouuid.UUID, ttl time.Duration) error {
	start := time.Now()

	defer func() {
		operationSecondsExpire.Observe(time.Since(start).Seconds())
	}()

	if len(uuids) == 0 {
		return nil
	}

	uuidsStr := make([]string, len(uuids))
	pipe := r.client.Pipeline()

	for i, uuid := range uuids {
		uuidStr := uuid.String()
		metricKey := metricKeyPrefix + uuidStr
		offsetKey := offsetKeyPrefix + uuidStr
		deadlineKey := deadlineKeyPrefix + uuidStr

		uuidsStr[i] = uuidStr

		pipe.Expire(metricKey, ttl)
		pipe.Expire(offsetKey, ttl)
		pipe.Expire(deadlineKey, ttl)
	}

	pipe.SRem(knownMetricsKey, uuidsStr)

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return err
	}

	return nil
}

// GetSetFlushDeadline implement batch.TemporaryStore interface
func (r *Redis) GetSetFlushDeadline(deadlines map[gouuid.UUID]time.Time) (map[gouuid.UUID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsSetDeadline.Observe(time.Since(start).Seconds())
	}()

	if len(deadlines) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	commands := make(map[gouuid.UUID]*goredis.StringCmd, len(deadlines))
	results := make(map[gouuid.UUID]time.Time, len(deadlines))

	for uuid, deadline := range deadlines {
		deadlineKey := deadlineKeyPrefix + uuid.String()

		commands[uuid] = pipe.GetSet(deadlineKey, deadline.Format(time.RFC3339))

		pipe.Expire(deadlineKey, defaultTTL)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return nil, err
	}

	for uuid := range deadlines {
		tmp, err := commands[uuid].Result()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			results[uuid], err = time.Parse(time.RFC3339, tmp)
			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// AddToTransfert implement batch.TemporaryStore interface
func (r *Redis) AddToTransfert(uuids []gouuid.UUID) error {
	if len(uuids) == 0 {
		return nil
	}

	strings := make([]string, len(uuids))

	for i, uuid := range uuids {
		strings[i] = uuid.String()
	}

	_, err := r.client.SAdd(transfertKey, strings).Result()

	return err
}

// GetTransfert implement batch.TemporaryStore interface
func (r *Redis) GetTransfert(count int) (map[gouuid.UUID]time.Time, error) {
	result, err := r.client.SPopN(transfertKey, int64(count)).Result()

	if err != nil && err != goredis.Nil {
		return nil, err
	}

	return r.getFlushDeadline(result)
}

// GetAllKnownMetrics implement batch.TemporaryStore interface
func (r *Redis) GetAllKnownMetrics() (map[gouuid.UUID]time.Time, error) {
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

func (r *Redis) getFlushDeadline(uuids []string) (map[gouuid.UUID]time.Time, error) {
	if len(uuids) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	commands := make([]*goredis.StringCmd, len(uuids))
	results := make(map[gouuid.UUID]time.Time, len(uuids))

	for i, uuidStr := range uuids {
		deadlineKey := deadlineKeyPrefix + uuidStr

		commands[i] = pipe.Get(deadlineKey)
	}

	if _, err := pipe.Exec(); err != nil && err != goredis.Nil {
		return nil, err
	}

	for i, uuidStr := range uuids {
		tmp, err := commands[i].Result()
		if err != nil && err != goredis.Nil {
			return nil, err
		}

		if err != goredis.Nil {
			uuid, err := gouuid.FromString(uuidStr)
			if err != nil {
				return nil, err
			}

			results[uuid], err = time.Parse(time.RFC3339, tmp)

			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// Return data from bytes values
func dataFromValues(uuid gouuid.UUID, values []byte) (types.MetricData, error) {
	data := types.MetricData{}
	buffer := bytes.NewReader(values)

	dataSerialized := make([]serializedPoints, len(values)/24)

	err := binary.Read(buffer, binary.BigEndian, &dataSerialized)
	if err != nil {
		return data, err
	}

	data.Points = make([]types.MetricPoint, len(dataSerialized))
	for i, point := range dataSerialized {
		data.UUID = uuid
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
