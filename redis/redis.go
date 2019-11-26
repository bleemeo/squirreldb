package redis

import (
	goredis "github.com/go-redis/redis"

	"bytes"
	"encoding/binary"
	"io"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

const keyPrefix = "squirreldb"

type Options struct {
	Address string
}

type Redis struct {
	client *goredis.Client
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
func (r *Redis) Append(newMetrics, existingMetrics map[types.MetricUUID]types.MetricData, timeToLive int64) error {
	if (len(newMetrics) == 0) && (len(existingMetrics) == 0) {
		return nil
	}

	pipe := r.client.Pipeline()
	timeToLiveDuration := time.Duration(timeToLive) * time.Second

	for uuid, data := range newMetrics {
		values, err := valuesFromData(data)

		if err != nil {
			return err
		}

		key := keyPrefix + "-" + uuid.String()

		pipe.Append(key, string(values))
		pipe.Expire(key, timeToLiveDuration)
	}

	for uuid, data := range existingMetrics {
		values, err := valuesFromData(data)

		if err != nil {
			return err
		}

		key := keyPrefix + "-" + uuid.String()

		pipe.Append(key, string(values))
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	return nil
}

// Get return the requested metrics
func (r *Redis) Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error) {
	if len(uuids) == 0 {
		return nil, nil
	}

	metrics := make(map[types.MetricUUID]types.MetricData)

	for _, uuid := range uuids {
		key := keyPrefix + "-" + uuid.String()
		values, err := r.client.Get(key).Bytes()

		if (err != nil) && (err != goredis.Nil) {
			return nil, err
		}

		data, err := dataFromValues(values)

		if err != nil {
			return nil, err
		}

		metrics[uuid] = data
	}

	return metrics, nil
}

// Set sets the specified metrics
func (r *Redis) Set(metrics map[types.MetricUUID]types.MetricData, timeToLive int64) error {
	if len(metrics) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	timeToLiveDuration := time.Duration(timeToLive) * time.Second

	for uuid, data := range metrics {
		values, err := valuesFromData(data)

		if err != nil {
			return err
		}

		key := keyPrefix + "-" + uuid.String()

		pipe.Set(key, string(values), timeToLiveDuration)
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	return nil
}

// Return data from bytes values
func dataFromValues(values []byte) (types.MetricData, error) {
	data := types.MetricData{}
	buffer := bytes.NewReader(values)

forLoop:
	for {
		var pointData struct {
			Timestamp  int64
			Value      float64
			TimeToLive int64
		}

		err := binary.Read(buffer, binary.BigEndian, &pointData)

		switch err {
		case nil:
			point := types.MetricPoint{
				Timestamp: pointData.Timestamp,
				Value:     pointData.Value,
			}

			data.Points = append(data.Points, point)
			data.TimeToLive = compare.MaxInt64(data.TimeToLive, pointData.TimeToLive)
		case io.EOF:
			break forLoop
		default:
			return types.MetricData{}, err
		}
	}

	return data, nil
}

// Return bytes values from data
func valuesFromData(data types.MetricData) ([]byte, error) {
	buffer := new(bytes.Buffer)

	for _, point := range data.Points {
		pointData := []interface{}{
			point.Timestamp,
			point.Value,
			data.TimeToLive,
		}

		for _, element := range pointData {
			if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
				return nil, err
			}
		}
	}

	return buffer.Bytes(), nil
}
