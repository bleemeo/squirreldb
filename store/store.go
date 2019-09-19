package store

import (
	"hamsterdb/types"
	"sync"
)

type Store struct {
	Points map[string][]types.Point
	mutex  sync.Mutex
}

func NewStore() *Store {
	return &Store{
		Points: make(map[string][]types.Point),
	}
}

func (s *Store) Append(newPoints, existingPoints map[string][]types.Point) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for key, points := range newPoints {
		item, exists := s.Points[key]

		if !exists {
			item = points
		} else {
			item = append(item, points...)
		}

		s.Points[key] = item
	}

	for key, points := range existingPoints {
		item, exists := s.Points[key]

		if !exists {
			item = points
		} else {
			item = append(item, points...)
		}

		s.Points[key] = item
	}

	return nil
}

func (s *Store) Get(key string) ([]types.Point, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.Points[key], nil
}

func (s *Store) Set(newPoints, existingPoints map[string][]types.Point) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for key, points := range newPoints {
		s.Points[key] = points
	}

	for key, points := range existingPoints {
		s.Points[key] = points
	}

	return nil
}
