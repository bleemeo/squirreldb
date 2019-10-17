package store

import (
	"context"
	"log"
	"os"
	"squirreldb/types"
	"sync"
	"time"
)

const (
	ExpiratorInterval = 60
	TimeToLiveOffset  = 150
)

var logger = log.New(os.Stdout, "[store] ", log.LstdFlags)

type metric struct {
	Points              types.MetricPoints
	ExpirationTimestamp int64
}

type Store struct {
	timestampToLive int64

	Metrics map[types.MetricUUID]metric
	mutex   sync.Mutex
}

// New creates a new Store object
func New(batchSize int64, offset int64) *Store {
	return &Store{
		timestampToLive: (batchSize * 2) + offset,
		Metrics:         make(map[types.MetricUUID]metric),
	}
}

// Append is the public function of append()
func (s *Store) Append(newMetrics, actualMetrics types.Metrics) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.append(newMetrics, actualMetrics, time.Now())
}

// Get is the public function of get()
func (s *Store) Get(uuids []types.MetricUUID) (types.Metrics, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.get(uuids)
}

// Set is the public function of set()
func (s *Store) Set(newMetrics, actualMetrics types.Metrics) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.set(newMetrics, actualMetrics, time.Now())
}

// Run calls expire() every expirator interval seconds
// If the context receives a stop signal, the service is stopped
func (s *Store) Run(ctx context.Context) {
	ticker := time.NewTicker(ExpiratorInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.expire(time.Now())
		case <-ctx.Done():
			logger.Println("Run: Stopped")
			return
		}
	}
}

// Appends metrics to existing items and update expiration
func (s *Store) append(newMetrics, actualMetrics types.Metrics, now time.Time) error {
	expirationTimestamp := now.Unix() + +s.timestampToLive

	for uuid, points := range newMetrics {
		metric := s.Metrics[uuid]

		metric.Points = append(metric.Points, points...)
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	for uuid, points := range actualMetrics {
		metric := s.Metrics[uuid]

		metric.Points = append(metric.Points, points...)
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	return nil
}

// Checks each item likely to expire and deletes them if it is the case
func (s *Store) expire(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	nowUnix := now.Unix()

	for uuid, metric := range s.Metrics {
		if metric.ExpirationTimestamp < nowUnix {
			delete(s.Metrics, uuid)
		}
	}
}

// Returns requested metrics
func (s *Store) get(uuids []types.MetricUUID) (types.Metrics, error) {
	metrics := make(types.Metrics)

	for _, uuid := range uuids {
		metric, exists := s.Metrics[uuid]

		if exists {
			metrics[uuid] = metric.Points
		}
	}

	return metrics, nil
}

// Set metrics (overwrite existing items) and expiration
func (s *Store) set(newMetrics, actualMetrics types.Metrics, now time.Time) error {
	expirationTimestamp := now.Unix() + s.timestampToLive

	for uuid, data := range newMetrics {
		metric := s.Metrics[uuid]

		metric.Points = data
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	for uuid, data := range actualMetrics {
		metric := s.Metrics[uuid]

		metric.Points = data
		metric.ExpirationTimestamp = expirationTimestamp

		s.Metrics[uuid] = metric
	}

	return nil
}
