package redis

import (
	goredis "github.com/go-redis/redis"
	gouuid "github.com/gofrs/uuid"

	"bytes"
	"encoding/binary"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

const keyPrefix = "squirreldb-"

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

// Append appends the specified metrics
func (r *Redis) Append(newMetrics, existingMetrics []types.MetricData, timeToLive int64) error {
	start := time.Now()

	defer func() {
		operationSecondsAdd.Observe(time.Since(start).Seconds())
	}()

	if (len(newMetrics) == 0) && (len(existingMetrics) == 0) {
		return nil
	}

	pipe := r.client.Pipeline()
	timeToLiveDuration := time.Duration(timeToLive) * time.Second

	var addedPoints int

	for _, data := range newMetrics {
		addedPoints += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return err
		}

		key := keyPrefix + data.UUID.String()

		pipe.Append(key, string(values))
		pipe.Expire(key, timeToLiveDuration)
	}

	for _, data := range existingMetrics {
		addedPoints += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return err
		}

		key := keyPrefix + data.UUID.String()

		pipe.Append(key, string(values))
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	operationPointssAdd.Add(float64(addedPoints))

	return nil
}

// Get return the requested metrics
func (r *Redis) Get(uuids []gouuid.UUID) (map[gouuid.UUID]types.MetricData, error) {
	start := time.Now()

	defer func() {
		operationSecondsGet.Observe(time.Since(start).Seconds())
	}()

	if len(uuids) == 0 {
		return nil, nil
	}

	metrics := make(map[gouuid.UUID]types.MetricData)
	commands := make([]*goredis.StringCmd, len(uuids))
	pipe := r.client.Pipeline()

	var readPointsCount int

	for i, uuid := range uuids {
		key := keyPrefix + uuid.String()
		commands[i] = pipe.Get(key)
	}

	if _, err := pipe.Exec(); err != nil {
		return nil, err
	}

	for i, uuid := range uuids {
		values, err := commands[i].Bytes()

		if (err != nil) && (err != goredis.Nil) {
			return nil, err
		}

		data, err := dataFromValues(values)

		if err != nil {
			return nil, err
		}

		metrics[uuid] = data
		readPointsCount += len(data.Points)
	}

	operationPointssGet.Add(float64(readPointsCount))

	return metrics, nil
}

// Set sets the specified metrics
func (r *Redis) Set(metrics []types.MetricData, timeToLive int64) error {
	start := time.Now()

	defer func() {
		operationSecondsSet.Observe(time.Since(start).Seconds())
	}()

	if len(metrics) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	timeToLiveDuration := time.Duration(timeToLive) * time.Second

	var writtenPointsCount int

	for _, data := range metrics {
		writtenPointsCount += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return err
		}

		key := keyPrefix + data.UUID.String()

		pipe.Set(key, string(values), timeToLiveDuration)
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	operationPointssSet.Add(float64(writtenPointsCount))

	return nil
}

// Return data from bytes values
func dataFromValues(values []byte) (types.MetricData, error) {
	data := types.MetricData{}
	buffer := bytes.NewReader(values)

	dataSerialized := make([]serializedPoints, len(values)/24)

	err := binary.Read(buffer, binary.BigEndian, &dataSerialized)
	if err != nil {
		return data, err
	}

	data.Points = make([]types.MetricPoint, len(dataSerialized))
	for i, point := range dataSerialized {
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
