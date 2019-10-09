package store

import (
	"squirreldb/types"
	"sync"
	"time"
)

type metric struct {
	Data                types.MetricData
	ExpirationTimestamp int64
}

type Store struct {
	Metrics map[types.MetricUUID]metric
	mutex   sync.Mutex
}

// NewStore creates a new Store object
func NewStore() *Store {
	return &Store{
		Metrics: make(map[types.MetricUUID]metric),
	}
}

// Append is the public function of append()
func (s *Store) Append(newMetrics, actualMetrics map[types.MetricUUID]types.MetricData) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// TODO: Time to live from config
	return s.append(newMetrics, actualMetrics, time.Now(), 750)
}

// Get is the public function of get()
func (s *Store) Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.get(uuids)
}

// Set is the public function of set()
func (s *Store) Set(newMetrics, actualMetrics map[types.MetricUUID]types.MetricData) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// TODO: Time to live from config
	return s.set(newMetrics, actualMetrics, time.Now(), 750)
}

// Appends metrics to existing items and update expiration
func (s *Store) append(newMetrics, actualMetrics map[types.MetricUUID]types.MetricData, now time.Time, timeToLive int64) error {
	expirationTimestamp := now.Unix() + timeToLive

	for uuid, data := range newMetrics {
		metric := s.Metrics[uuid]

		metric.Data.Points = append(metric.Data.Points, data.Points...)
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	for uuid, data := range actualMetrics {
		metric := s.Metrics[uuid]

		metric.Data.Points = append(metric.Data.Points, data.Points...)
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	return nil
}

// Checks each item likely to expire and deletes them if it is the case
func (s *Store) expire(now time.Time) {
	nowUnix := now.Unix()

	for uuid, metric := range s.Metrics {
		if metric.ExpirationTimestamp < nowUnix {
			delete(s.Metrics, uuid)
		}
	}
}

// Returns requested metrics
func (s *Store) get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error) {
	metrics := make(map[types.MetricUUID]types.MetricData)

	for _, uuid := range uuids {
		metric, exists := s.Metrics[uuid]

		if exists {
			metrics[uuid] = metric.Data
		}
	}

	return metrics, nil
}

// Set metrics (overwrite existing items) and expiration
func (s *Store) set(newMetrics, actualMetrics map[types.MetricUUID]types.MetricData, now time.Time, timeToLive int64) error {
	expirationTimestamp := now.Unix() + timeToLive

	for uuid, data := range newMetrics {
		metric := s.Metrics[uuid]

		metric.Data = data
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	for uuid, data := range actualMetrics {
		metric := s.Metrics[uuid]

		metric.Data = data
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	return nil
}
