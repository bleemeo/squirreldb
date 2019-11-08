package redis

import (
	"bytes"
	"encoding/binary"
	goredis "github.com/go-redis/redis"
	"io"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

const (
	keyPrefix = "squirreldb-"
)

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

	redis := Redis{
		client: client,
	}

	return &redis
}

// Append appends metrics to existing entries
func (r *Redis) Append(newMetrics, existingMetrics types.Metrics, timeToLive int64) error {
	pipe := r.client.Pipeline()

	for uuid, metricData := range newMetrics {
		data, err := encode(metricData)

		if err != nil {
			return err
		}

		key := keyPrefix + uuid.String()

		pipe.Append(key, string(data))
	}

	for uuid, metricData := range existingMetrics {
		data, err := encode(metricData)

		if err != nil {
			return err
		}

		key := keyPrefix + uuid.String()

		pipe.Append(key, string(data))
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	return nil
}

// Get returns requested metrics
func (r *Redis) Get(uuids types.MetricUUIDs) (types.Metrics, error) {
	metrics := make(types.Metrics)
	pipe := r.client.Pipeline()

	for _, uuid := range uuids {
		key := keyPrefix + uuid.String()
		data, err := r.client.Get(key).Bytes()

		if err != nil {
			return nil, err
		}

		if _, err := pipe.Exec(); err != nil {
			return nil, err
		}

		metricData, err := decode(data)

		if err != nil {
			return nil, err
		}

		metrics[uuid] = metricData
	}

	return metrics, nil
}

// Set set metrics (overwrite existing entries)
func (r *Redis) Set(newMetrics, existingMetrics types.Metrics, timeToLive int64) error {
	pipe := r.client.Pipeline()
	timeToLiveDuration := time.Duration(timeToLive) * time.Second

	for uuid, metricData := range newMetrics {
		data, err := encode(metricData)

		if err != nil {
			return err
		}

		key := keyPrefix + uuid.String()

		pipe.Set(key, string(data), timeToLiveDuration)
	}

	for uuid, metricData := range existingMetrics {
		data, err := encode(metricData)

		if err != nil {
			return err
		}

		key := keyPrefix + uuid.String()

		pipe.Set(key, string(data), timeToLiveDuration)
	}

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	return nil
}

// Encode MetricData
func encode(metricData types.MetricData) ([]byte, error) {
	buffer := new(bytes.Buffer)

	for _, point := range metricData.Points {
		pointData := []interface{}{
			point.Timestamp,
			point.Value,
			metricData.TimeToLive,
		}

		for _, element := range pointData {
			if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
				return nil, err
			}
		}
	}

	return buffer.Bytes(), nil
}

// Decode MetricData
func decode(data []byte) (types.MetricData, error) {
	metricData := types.MetricData{}
	buffer := bytes.NewReader(data)

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

			metricData.Points = append(metricData.Points, point)
			metricData.TimeToLive = compare.Int64Max(metricData.TimeToLive, pointData.TimeToLive)
		case io.EOF:
			break forLoop
		default:
			return types.MetricData{}, err
		}
	}

	return metricData, nil
}
