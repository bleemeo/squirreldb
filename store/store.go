package store

import (
	"context"
	"log"
	"os"
	"squirreldb/compare"
	"squirreldb/types"
	"sync"
	"time"
)

const (
	expiratorInterval = 60
)

var logger = log.New(os.Stdout, "[store] ", log.LstdFlags)

type metric struct {
	types.MetricData
	ExpirationTimestamp int64
}

type Store struct {
	metrics map[types.MetricUUID]metric
	mutex   sync.Mutex
}

// New creates a new Store object
func New() *Store {
	return &Store{
		metrics: make(map[types.MetricUUID]metric),
	}
}

// Append is the public function of append()
func (s *Store) Append(newMetrics, existingMetrics types.Metrics, timeToLive int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.append(newMetrics, existingMetrics, time.Now(), timeToLive)
}

// Get is the public function of get()
func (s *Store) Get(uuids types.MetricUUIDs) (types.Metrics, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.get(uuids)
}

// Set is the public function of set()
func (s *Store) Set(newMetrics, existingMetrics types.Metrics, timeToLive int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.set(newMetrics, existingMetrics, time.Now(), timeToLive)
}

// Run calls expire() every expirator interval seconds
// If the context receives a stop signal, the service is stopped
func (s *Store) Run(ctx context.Context) {
	ticker := time.NewTicker(expiratorInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.expire(time.Now())
		case <-ctx.Done():
			logger.Println("Stopped")

			return
		}
	}
}

// Appends metrics to existing items and update expiration
func (s *Store) append(newMetrics, existingMetrics types.Metrics, now time.Time, timeToLive int64) error {
	expirationTimestamp := now.Unix() + timeToLive

	for uuid, metricData := range newMetrics {
		metric := s.metrics[uuid]

		metric.Points = append(metric.Points, metricData.Points...)
		metric.TimeToLive = compare.Int64Max(metric.TimeToLive, metricData.TimeToLive)
		metric.ExpirationTimestamp = expirationTimestamp

		s.metrics[uuid] = metric
	}

	for uuid, metricData := range existingMetrics {
		metric := s.metrics[uuid]

		metric.Points = append(metric.Points, metricData.Points...)
		metric.TimeToLive = compare.Int64Max(metric.TimeToLive, metricData.TimeToLive)
		metric.ExpirationTimestamp = expirationTimestamp

		s.metrics[uuid] = metric
	}

	return nil
}

// Checks each item likely to expire and deletes them if it is the case
func (s *Store) expire(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	nowUnix := now.Unix()

	for uuid, metric := range s.metrics {
		if metric.ExpirationTimestamp < nowUnix {
			delete(s.metrics, uuid)
		}
	}
}

// Returns requested metrics
func (s *Store) get(uuids types.MetricUUIDs) (types.Metrics, error) {
	metrics := make(types.Metrics)

	for _, uuid := range uuids {
		metric, exists := s.metrics[uuid]

		if exists {
			metrics[uuid] = metric.MetricData
		}
	}

	return metrics, nil
}

// Set metrics (overwrite existing items) and expiration
func (s *Store) set(newMetrics, existingMetrics types.Metrics, now time.Time, timeToLive int64) error {
	expirationTimestamp := now.Unix() + timeToLive

	for uuid, metricData := range newMetrics {
		metric := s.metrics[uuid]

		metric.MetricData = metricData
		metric.ExpirationTimestamp = expirationTimestamp

		s.metrics[uuid] = metric
	}

	for uuid, metricData := range existingMetrics {
		metric := s.metrics[uuid]

		metric.MetricData = metricData
		metric.ExpirationTimestamp = expirationTimestamp

		s.metrics[uuid] = metric
	}

	return nil
}
