package memorystore

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

	gouuid "github.com/gofrs/uuid"
)

const expiratorInterval = 60

const defaultTTL = 24 * time.Hour

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[store] ", log.LstdFlags)

type storeData struct {
	types.MetricData
	WriteOffset    int
	flushDeadline  time.Time
	expirationTime time.Time
}

type Store struct {
	knownMetrics     map[gouuid.UUID]interface{}
	metrics          map[gouuid.UUID]storeData
	transfertMetrics []gouuid.UUID
	mutex            sync.Mutex
}

// New creates a new Store object
func New() *Store {
	store := &Store{
		metrics:      make(map[gouuid.UUID]storeData),
		knownMetrics: make(map[gouuid.UUID]interface{}),
	}

	return store
}

// Append implement batch.TemporaryStore interface
func (s *Store) Append(points []types.MetricData) ([]int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(points) == 0 {
		return nil, nil
	}

	pointCount := make([]int, len(points))

	for i, data := range points {
		storeData := s.metrics[data.UUID]

		storeData.UUID = data.UUID
		storeData.Points = append(storeData.Points, data.Points...)
		storeData.TimeToLive = compare.MaxInt64(storeData.TimeToLive, data.TimeToLive)
		s.metrics[data.UUID] = storeData

		pointsTotal.Add(float64(len(data.Points)))

		pointCount[i] = len(storeData.Points)
	}

	metricsTotal.Set(float64(len(s.metrics)))

	return pointCount, nil
}

// GetSetPointsAndOffset implement batch.TemporaryStore interface
func (s *Store) GetSetPointsAndOffset(points []types.MetricData, offsets []int) ([]types.MetricData, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.getSetPointsAndOffset(points, offsets, time.Now())
}

func (s *Store) getSetPointsAndOffset(points []types.MetricData, offsets []int, now time.Time) ([]types.MetricData, error) {
	if len(points) == 0 {
		return nil, nil
	}

	if len(points) != len(offsets) {
		return nil, fmt.Errorf("GetSetPointsAndOffset: len(points) == %d must be equal to len(offsets) == %d", len(points), len(offsets))
	}

	expirationTime := now.Add(defaultTTL)
	oldData := make([]types.MetricData, len(points))

	for i, data := range points {
		storeData := s.metrics[data.UUID]

		oldData[i] = storeData.MetricData

		pointsTotal.Add(float64(len(data.Points) - len(storeData.MetricData.Points)))

		storeData.MetricData = data
		storeData.expirationTime = expirationTime
		storeData.WriteOffset = offsets[i]

		s.metrics[data.UUID] = storeData
		s.knownMetrics[data.UUID] = nil
	}

	metricsTotal.Set(float64(len(s.metrics)))

	return oldData, nil
}

// ReadPointsAndOffset implement batch.TemporaryStore interface
func (s *Store) ReadPointsAndOffset(uuids []gouuid.UUID) ([]types.MetricData, []int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(uuids) == 0 {
		return nil, nil, nil
	}

	metrics := make([]types.MetricData, len(uuids))
	writeOffsets := make([]int, len(uuids))

	for i, uuid := range uuids {
		storeData, exists := s.metrics[uuid]

		if exists {
			metrics[i] = storeData.MetricData
			writeOffsets[i] = storeData.WriteOffset
		}
	}

	return metrics, writeOffsets, nil
}

// MarkToExpire implement batch.TemporaryStore interface
func (s *Store) MarkToExpire(uuids []gouuid.UUID, ttl time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.markToExpire(uuids, ttl, time.Now())
}

func (s *Store) markToExpire(uuids []gouuid.UUID, ttl time.Duration, now time.Time) error {
	for _, uuid := range uuids {
		if entry, found := s.metrics[uuid]; found {
			entry.expirationTime = now.Add(ttl)
			s.metrics[uuid] = entry
		}

		delete(s.knownMetrics, uuid)
	}

	return nil
}

// GetSetFlushDeadline implement batch.TemporaryStore interface
func (s *Store) GetSetFlushDeadline(deadlines map[gouuid.UUID]time.Time) (map[gouuid.UUID]time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	results := make(map[gouuid.UUID]time.Time, len(deadlines))

	for uuid, deadline := range deadlines {
		state := s.metrics[uuid]
		results[uuid] = state.flushDeadline
		state.flushDeadline = deadline
		s.metrics[uuid] = state
	}

	return results, nil
}

// AddToTransfert implement batch.TemporaryStore interface
func (s *Store) AddToTransfert(uuids []gouuid.UUID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.transfertMetrics = append(s.transfertMetrics, uuids...)

	return nil
}

// GetTransfert implement batch.TemporaryStore interface
func (s *Store) GetTransfert(count int) (map[gouuid.UUID]time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	results := make(map[gouuid.UUID]time.Time, count)
	endIndex := count

	if endIndex >= len(s.transfertMetrics) {
		endIndex = len(s.transfertMetrics)
	}

	for i := 0; i < endIndex; i++ {
		uuid := s.transfertMetrics[i]
		results[uuid] = s.metrics[uuid].flushDeadline
	}

	copy(s.transfertMetrics, s.transfertMetrics[endIndex:])
	s.transfertMetrics = s.transfertMetrics[:len(s.transfertMetrics)-endIndex]

	return results, nil
}

// GetAllKnownMetrics implement batch.TemporaryStore interface
func (s *Store) GetAllKnownMetrics() (map[gouuid.UUID]time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	results := make(map[gouuid.UUID]time.Time, len(s.metrics))

	for uuid := range s.knownMetrics {
		results[uuid] = s.metrics[uuid].flushDeadline
	}

	return results, nil
}

// Run starts all Store services
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

// Deletes all expired metrics
func (s *Store) expire(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var pointsCount int

	for uuid, storeData := range s.metrics {
		if storeData.expirationTime.Before(now) {
			delete(s.metrics, uuid)
		} else {
			pointsCount += len(storeData.Points)
		}
	}

	pointsTotal.Set(float64(pointsCount))
	metricsTotal.Set(float64(len(s.metrics)))
}
