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

type storeData struct {
	types.MetricData
	ExpirationTimestamp int64
}

type Store struct {
	metrics map[types.MetricUUID]storeData
	mutex   sync.Mutex
}

// New creates a new Store object
func New() *Store {
	store := &Store{
		metrics: make(map[types.MetricUUID]storeData),
	}

	return store
}

// Append is the public method of append
func (s *Store) Append(newMetrics, existingMetrics map[types.MetricUUID]types.MetricData, timeToLive int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.append(newMetrics, existingMetrics, timeToLive, time.Now())
}

// Get is the public method of get
func (s *Store) Get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.get(uuids)
}

// Set is the public method of set
func (s *Store) Set(metrics map[types.MetricUUID]types.MetricData, timeToLive int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.set(metrics, timeToLive, time.Now())
}

// Run starts all Store services
func (s *Store) Run(ctx context.Context) {
	s.runExpirator(ctx)
}

// Starts the expirator service
// If a stop signal is received, the service is stopped
func (s *Store) runExpirator(ctx context.Context) {
	delay := expiratorInterval * time.Second

	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.expire(time.Now())
		case <-ctx.Done():
			logger.Println("Expirator service stopped")
			return
		}
	}
}

// Deletes all expired metrics
func (s *Store) expire(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for uuid, storeData := range s.metrics {
		if storeData.ExpirationTimestamp < now.Unix() {
			delete(s.metrics, uuid)
		}
	}
}

// Appends the specified metrics
func (s *Store) append(newMetrics, existingMetrics map[types.MetricUUID]types.MetricData, timeToLive int64, now time.Time) error {
	if (len(newMetrics) == 0) && (len(existingMetrics) == 0) {
		return nil
	}

	start := time.Now()

	expirationTimestamp := now.Unix() + timeToLive

	for uuid, data := range newMetrics {
		storeData := s.metrics[uuid]

		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)
		storeData.ExpirationTimestamp = expirationTimestamp

		s.metrics[uuid] = storeData

		appendPointsTotal.Add(float64(len(data.Points)))
	}

	for uuid, data := range existingMetrics {
		storeData := s.metrics[uuid]

		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)
		storeData.ExpirationTimestamp = expirationTimestamp

		s.metrics[uuid] = storeData

		appendPointsTotal.Add(float64(len(data.Points)))
	}

	appendSeconds.Observe(time.Since(start).Seconds())

	return nil
}

// Return the requested metrics
func (s *Store) get(uuids []types.MetricUUID) (map[types.MetricUUID]types.MetricData, error) {
	if len(uuids) == 0 {
		return nil, nil
	}

	start := time.Now()

	metrics := make(map[types.MetricUUID]types.MetricData, len(uuids))

	for _, uuid := range uuids {
		storeData, exists := s.metrics[uuid]

		if exists {
			metrics[uuid] = storeData.MetricData

			getPointsTotal.Add(float64(len(storeData.Points)))
		}
	}

	getSeconds.Observe(time.Since(start).Seconds())

	return metrics, nil
}

// Sets the specified metrics
func (s *Store) set(metrics map[types.MetricUUID]types.MetricData, timeToLive int64, now time.Time) error {
	if len(metrics) == 0 {
		return nil
	}

	start := time.Now()

	expirationTimestamp := now.Unix() + timeToLive

	for uuid, data := range metrics {
		storeData := s.metrics[uuid]

		storeData.MetricData = data
		storeData.ExpirationTimestamp = expirationTimestamp

		s.metrics[uuid] = storeData

		setPointsTotal.Add(float64(len(data.Points)))
	}

	setSeconds.Observe(time.Since(start).Seconds())

	return nil
}
