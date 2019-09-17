package store

import (
	"hamsterdb/types"
	"sync"
)

type Store struct {
	msPoints map[string]types.MetricPoints
	mutex    sync.Mutex
}

func NewStore() *Store {
	return &Store{
		msPoints: make(map[string]types.MetricPoints),
	}
}

func (store *Store) Append(newMsPoints map[string]types.MetricPoints, currentMsPoints map[string]types.MetricPoints) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	for key, mPoints := range newMsPoints {
		item, itemExists := store.msPoints[key]

		if !itemExists {
			item.Labels = mPoints.Labels
		}

		item.AddPoints(mPoints.Points)

		store.msPoints[key] = item
	}

	for key, mPoints := range currentMsPoints {
		item, itemExists := store.msPoints[key]

		if !itemExists {
			item.Labels = mPoints.Labels
		}

		item.AddPoints(mPoints.Points)

		store.msPoints[key] = item
	}

	return nil
}

func (store *Store) Get(key string) (types.MetricPoints, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	mPoints := store.msPoints[key]

	return mPoints, nil
}

func (store *Store) Set(newMsPoints map[string]types.MetricPoints, currentMsPoints map[string]types.MetricPoints) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	for key, mPoints := range newMsPoints {
		store.msPoints[key] = mPoints
	}

	for key, mPoints := range currentMsPoints {
		store.msPoints[key] = mPoints
	}

	return nil
}
