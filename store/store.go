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
		item, itemExists := store.msPoints[key]

		if !itemExists {
			store.msPoints[key] = mPoints
		} else {
			item.AddPoints(mPoints.Points)

			store.msPoints[key] = item
		}
	}

	for key, mPoints := range currentMsPoints {
		item, itemExists := store.msPoints[key]

		if !itemExists {
			store.msPoints[key] = mPoints
		} else {
			item.AddPoints(mPoints.Points)

			store.msPoints[key] = item
		}
	}

	return nil
}
