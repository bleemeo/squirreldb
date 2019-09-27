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
	Metrics map[string]Data
	mutex   sync.Mutex
}

func NewStore() *Store {
	return &Store{
		Metrics: make(map[string]Data),
	}
}

func (s *Store) Append(newPoints, existingPoints map[string][]types.Point) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.append(newPoints, existingPoints, time.Now(), config.StorageTimeToLive)
}

func (s *Store) Get(key string) ([]types.Point, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.get(key)
}

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

func (s *Store) Set(newPoints, existingPoints map[string][]types.Point) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.set(newPoints, existingPoints, time.Now(), config.StorageTimeToLive)
}

func (s *Store) append(newPoints, existingPoints map[string][]types.Point, now time.Time, timeToLive time.Duration) error {
	for key, points := range newPoints {
		item, exists := s.Metrics[key]

		if !exists {
			item.Points = points
		} else {
			item.Points = append(item.Points, points...)
		}

		item.ExpirationDeadline = now.Add(timeToLive)
		s.Metrics[key] = item
	}

	for key, points := range existingPoints {
		item, exists := s.Metrics[key]

		if !exists {
			item.Points = points
		} else {
			item.Points = append(item.Points, points...)
		}

		item.ExpirationDeadline = now.Add(timeToLive)
		s.Metrics[key] = item
	}

	return nil
}

func (s *Store) expire(now time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for key, metric := range s.Metrics {
		if metric.ExpirationDeadline.Before(now) {
			delete(s.Metrics, key)
		}
	}
}

func (s *Store) get(key string) ([]types.Point, error) {
	return s.Metrics[key].Points, nil
}

func (s *Store) set(newPoints, existingPoints map[string][]types.Point, now time.Time, timeToLive time.Duration) error {
	for key, points := range newPoints {
		item := Data{
			Points:             points,
			ExpirationDeadline: now.Add(timeToLive),
		}

		s.Metrics[key] = item
	}

	for key, points := range existingPoints {
		item := Data{
			Points:             points,
			ExpirationDeadline: now.Add(timeToLive),
		}

		s.Metrics[key] = item
	}

	return nil
}
