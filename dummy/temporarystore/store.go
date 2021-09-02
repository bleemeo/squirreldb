package temporarystore

import (
	"context"
	"fmt"
	"log"
	"os"
	"squirreldb/compare"
	"squirreldb/debug"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const expiratorInterval = 60

const defaultTTL = 24 * time.Hour

//nolint:gochecknoglobals
var logger = log.New(os.Stdout, "[store] ", log.LstdFlags)

type storeData struct {
	types.MetricData
	WriteOffset    int
	flushDeadline  time.Time
	expirationTime time.Time
}

type Store struct {
	knownMetrics     map[types.MetricID]interface{}
	metricsStore     map[types.MetricID]storeData
	transfertMetrics []types.MetricID
	mutex            sync.Mutex
	metrics          *metrics
}

// New creates a new Store object.
func New(reg prometheus.Registerer) *Store {
	store := &Store{
		metricsStore: make(map[types.MetricID]storeData),
		knownMetrics: make(map[types.MetricID]interface{}),
		metrics:      newMetrics(reg),
	}

	return store
}

// Append implement batch.TemporaryStore interface.
func (s *Store) Append(ctx context.Context, points []types.MetricData) ([]int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(points) == 0 {
		return nil, nil
	}

	pointCount := make([]int, len(points))

	for i, data := range points {
		storeData := s.metricsStore[data.ID]

		storeData.ID = data.ID
		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)
		s.metricsStore[data.ID] = storeData

		s.metrics.PointsTotal.Add(float64(len(data.Points)))

		pointCount[i] = len(storeData.Points)
	}

	s.metrics.MetricsTotal.Set(float64(len(s.metricsStore)))

	return pointCount, nil
}

// GetSetPointsAndOffset implement batch.TemporaryStore interface.
func (s *Store) GetSetPointsAndOffset(
	ctx context.Context,
	points []types.MetricData,
	offsets []int,
) ([]types.MetricData, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.getSetPointsAndOffset(points, offsets, time.Now())
}

func (s *Store) getSetPointsAndOffset(
	points []types.MetricData,
	offsets []int, now time.Time,
) ([]types.MetricData, error) {
	if len(points) == 0 {
		return nil, nil
	}

	if len(points) != len(offsets) {
		msg := "GetSetPointsAndOffset: len(points) == %d must be equal to len(offsets) == %d"

		return nil, fmt.Errorf(msg, len(points), len(offsets))
	}

	expirationTime := now.Add(defaultTTL)
	oldData := make([]types.MetricData, len(points))

	for i, data := range points {
		storeData := s.metricsStore[data.ID]

		oldData[i] = storeData.MetricData

		s.metrics.PointsTotal.Add(float64(len(data.Points) - len(storeData.MetricData.Points)))

		storeData.MetricData = data
		storeData.expirationTime = expirationTime
		storeData.WriteOffset = offsets[i]

		s.metricsStore[data.ID] = storeData
		s.knownMetrics[data.ID] = nil
	}

	s.metrics.MetricsTotal.Set(float64(len(s.metricsStore)))

	return oldData, nil
}

// ReadPointsAndOffset implement batch.TemporaryStore interface.
func (s *Store) ReadPointsAndOffset(ctx context.Context, ids []types.MetricID) ([]types.MetricData, []int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(ids) == 0 {
		return nil, nil, nil
	}

	metrics := make([]types.MetricData, len(ids))
	writeOffsets := make([]int, len(ids))

	for i, id := range ids {
		storeData, exists := s.metricsStore[id]

		if exists {
			metrics[i] = storeData.MetricData
			writeOffsets[i] = storeData.WriteOffset
		}
	}

	return metrics, writeOffsets, nil
}

// MarkToExpire implement batch.TemporaryStore interface.
func (s *Store) MarkToExpire(ctx context.Context, ids []types.MetricID, ttl time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.markToExpire(ids, ttl, time.Now())
}

func (s *Store) markToExpire(ids []types.MetricID, ttl time.Duration, now time.Time) error {
	for _, id := range ids {
		if entry, found := s.metricsStore[id]; found {
			entry.expirationTime = now.Add(ttl)
			s.metricsStore[id] = entry
		}

		delete(s.knownMetrics, id)
	}

	return nil
}

// GetSetFlushDeadline implement batch.TemporaryStore interface.
func (s *Store) GetSetFlushDeadline(
	ctx context.Context,
	deadlines map[types.MetricID]time.Time,
) (map[types.MetricID]time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	results := make(map[types.MetricID]time.Time, len(deadlines))

	for id, deadline := range deadlines {
		state := s.metricsStore[id]
		results[id] = state.flushDeadline
		state.flushDeadline = deadline
		s.metricsStore[id] = state
	}

	return results, nil
}

// AddToTransfert implement batch.TemporaryStore interface.
func (s *Store) AddToTransfert(ctx context.Context, ids []types.MetricID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.transfertMetrics = append(s.transfertMetrics, ids...)

	return nil
}

// GetTransfert implement batch.TemporaryStore interface.
func (s *Store) GetTransfert(ctx context.Context, count int) (map[types.MetricID]time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	results := make(map[types.MetricID]time.Time, count)
	endIndex := count

	if endIndex >= len(s.transfertMetrics) {
		endIndex = len(s.transfertMetrics)
	}

	for i := 0; i < endIndex; i++ {
		id := s.transfertMetrics[i]
		results[id] = s.metricsStore[id].flushDeadline
	}

	copy(s.transfertMetrics, s.transfertMetrics[endIndex:])
	s.transfertMetrics = s.transfertMetrics[:len(s.transfertMetrics)-endIndex]

	return results, nil
}

// GetAllKnownMetrics implement batch.TemporaryStore interface.
func (s *Store) GetAllKnownMetrics(ctx context.Context) (map[types.MetricID]time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	results := make(map[types.MetricID]time.Time, len(s.metricsStore))

	for id := range s.knownMetrics {
		results[id] = s.metricsStore[id].flushDeadline
	}

	return results, nil
}

// Run starts all Store services.
func (s *Store) Run(ctx context.Context) {
	interval := expiratorInterval * time.Second
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			s.expire(time.Now())
		case <-ctx.Done():
			debug.Print(2, logger, "Expirator service stopped")

			return
		}
	}
}

// Deletes all expired metrics.
func (s *Store) expire(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var pointsCount int

	for id, storeData := range s.metricsStore {
		if storeData.expirationTime.Before(now) {
			delete(s.metricsStore, id)
		} else {
			pointsCount += len(storeData.Points)
		}
	}

	s.metrics.PointsTotal.Set(float64(pointsCount))
	s.metrics.MetricsTotal.Set(float64(len(s.metricsStore)))
}
