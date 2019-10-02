package store

import (
	"context"
	"log"
	"os"
	"squirreldb/config"
	"squirreldb/types"
	"sync"
	"time"
)

var logger = log.New(os.Stdout, "[store] ", log.LstdFlags)

type Data struct {
	Points             []types.Point
	ExpirationDeadline time.Time
}

type Store struct {
	metrics map[string]Data
	mutex   sync.Mutex
}

// NewStore creates a new Store object
func NewStore() *Store {
	return &Store{
		metrics: make(map[string]Data),
	}
}

// Append is the public function of append()
func (s *Store) Append(newPoints, existingPoints map[string][]types.Point) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.append(newPoints, existingPoints, time.Now(), config.StorageTimeToLive)
}

// Get is the public function of get()
func (s *Store) Get(keys []string) (map[string][]types.Point, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.get(keys)
}

// RunExpirator calls expire() every StoreExpiratorInterval seconds
// If the context receives a stop signal, the service is stopped
func (s *Store) RunExpirator(ctx context.Context, wg *sync.WaitGroup) {
	ticker := time.NewTicker(config.StoreExpiratorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.expire(time.Now())
		case <-ctx.Done():
			logger.Println("RunExpirator: Stopped")
			wg.Done()
			return
		}
	}
}

// Set is the public function of set()
func (s *Store) Set(newPoints, existingPoints map[string][]types.Point) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.set(newPoints, existingPoints, time.Now(), config.StorageTimeToLive)
}

// Appends points to existing items and update expiration deadline
func (s *Store) append(newPoints, existingPoints map[string][]types.Point, now time.Time, timeToLive time.Duration) error {
	for key, points := range newPoints {
		item, exists := s.metrics[key]

		if !exists {
			item.Points = points
		} else {
			item.Points = append(item.Points, points...)
		}

		item.ExpirationDeadline = now.Add(timeToLive)
		s.metrics[key] = item
	}

	for key, points := range existingPoints {
		item, exists := s.metrics[key]

		if !exists {
			item.Points = points
		} else {
			item.Points = append(item.Points, points...)
		}

		item.ExpirationDeadline = now.Add(timeToLive)
		s.metrics[key] = item
	}

	return nil
}

// Checks each batch of metric point likely to expire and deletes them if it is the case
func (s *Store) expire(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for key, metric := range s.metrics {
		if metric.ExpirationDeadline.Before(now) {
			delete(s.metrics, key)
		}
	}
}

// Returns requested points
func (s *Store) get(keys []string) (map[string][]types.Point, error) {
	keysPoints := make(map[string][]types.Point)

	for key, data := range s.metrics {
		for i := range keys {
			if keys[i] == key {
				keysPoints[key] = data.Points
			}
		}
	}

	return keysPoints, nil
}

// Set points (overwrite existing items) and set expiration deadline
func (s *Store) set(newPoints, existingPoints map[string][]types.Point, now time.Time, timeToLive time.Duration) error {
	for key, points := range newPoints {
		item := Data{
			Points:             points,
			ExpirationDeadline: now.Add(timeToLive),
		}

		s.metrics[key] = item
	}

	for key, points := range existingPoints {
		item := Data{
			Points:             points,
			ExpirationDeadline: now.Add(timeToLive),
		}

		s.metrics[key] = item
	}

	return nil
}
