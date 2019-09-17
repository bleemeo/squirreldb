package main

import (
	"hamsterdb/batch"
	"hamsterdb/cassandra"
	"hamsterdb/store"
	"hamsterdb/types"
	"log"
	"time"
)

func main() {
	myStore := store.NewStore()
	myCassandra := cassandra.NewCassandra()
	myBatch := batch.NewBatch(myStore, myCassandra)

	metrics1 := []types.MetricPoints{
		{
			Metric: types.Metric{Labels: map[string]string{
				"__name__": "up",
			}},
			Points: []types.Point{
				{
					Time:  time.Now().Add(100 * time.Second),
					Value: 100,
				},
				{
					Time:  time.Now().Add(200 * time.Second),
					Value: 200,
				},
				{
					Time:  time.Now().Add(300 * time.Second),
					Value: 300,
				},
				{
					Time:  time.Now().Add(500 * time.Second),
					Value: 500,
				},
			},
		},
	}
	metrics2 := []types.MetricPoints{
		{
			Metric: types.Metric{Labels: map[string]string{
				"__name__": "up",
			}},
			Points: []types.Point{
				{
					Time:  time.Now().Add(400 * time.Second),
					Value: 400,
				},
				{
					Time:  time.Now().Add(600 * time.Second),
					Value: 600,
				},
				{
					Time:  time.Now().Add(1000 * time.Second),
					Value: 1000,
				},
			},
		},
	}

	if err := myCassandra.InitSession("127.0.0.1:9042"); err != nil {
		log.Fatalf("[cassandra] InitSession: Can't initialize session (%v)", err)
	}

	_ = myBatch.Write(metrics1)
	_ = myBatch.Write(metrics2)

	myCassandra.CloseSession()
}
