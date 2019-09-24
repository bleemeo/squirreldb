package redis

import (
	"bytes"
	"encoding/binary"
	"github.com/go-redis/redis"
	"hamsterdb/types"
	"io"
	"time"
)

type Redis struct {
	client *redis.ClusterClient
}

func NewRedis() *Redis {
	return &Redis{}
}

func (r *Redis) InitCluster(addresses ...string) error {
	r.client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addresses,
	})

	return nil
}

func (r *Redis) append(newPoints, existingPoints map[string][]types.Point, timeToLive time.Duration) error {
	pipeliner := r.client.Pipeline()

	timeToLive *= time.Second

	for key, points := range newPoints {
		// TODO: Handle error
		data, _ := encode(points)

		pipeliner.Append(key, data)
		pipeliner.Expire(key, timeToLive)
	}

	for key, points := range existingPoints {
		// TODO: Handle error
		data, _ := encode(points)

		pipeliner.Append(key, data)
	}

	if _, err := pipeliner.Exec(); err != nil {
		return err
	}

	return nil
}

func (r *Redis) get(key string) ([]types.Point, error) {
	pipeliner := r.client.Pipeline()

	data, err := pipeliner.Get(key).Bytes()

	if err != nil {
		return nil, err
	}

	// TODO: Handle error
	points, _ := decode(data)

	return points, nil
}

func (r *Redis) set(newPoints, existingPoints map[string][]types.Point, timeToLive time.Duration) error {
	pipeliner := r.client.Pipeline()

	timeToLive *= time.Second

	for key, points := range newPoints {
		// TODO: Handle error
		data, _ := encode(points)

		pipeliner.Set(key, data, timeToLive)
	}

	for key, points := range existingPoints {
		// TODO: Handle error
		data, _ := encode(points)

		pipeliner.Set(key, data, timeToLive)
	}

	if _, err := pipeliner.Exec(); err != nil {
		return err
	}

	return nil
}

func encode(points []types.Point) ([]byte, error) {
	buffer := new(bytes.Buffer)

	for _, point := range points {
		pointData := []interface{}{
			point.Time.Unix(),
			point.Value,
		}

		for _, element := range pointData {
			if err := binary.Write(buffer, binary.BigEndian, element); err != nil {
				return nil, err
			}
		}
	}

	return buffer.Bytes(), nil
}

func decode(data []byte) ([]types.Point, error) {
	var points []types.Point
	buffer := bytes.NewReader(data)

	for {
		var pointData struct {
			Timestamp int64
			Value     float64
		}

		err := binary.Read(buffer, binary.BigEndian, &pointData)

		if err == nil {
			pointTime := time.Unix(pointData.Timestamp, 0)

			point := types.Point{
				Time:  pointTime,
				Value: pointData.Value,
			}

			points = append(points, point)
		} else if err != io.EOF {
			return nil, err
		} else {
			return points, nil
		}
	}
}
