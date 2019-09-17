package main

import (
	"hamsterdb/batch"
	"hamsterdb/cassandra"
	"hamsterdb/config"
	"hamsterdb/prometheus"
	"hamsterdb/store"
	"hamsterdb/util"
	"log"
	"time"
)

func main() {
	myStore := store.NewStore()
	myCassandra := cassandra.NewCassandra()
	myBatch := batch.NewBatch(myStore, myCassandra)
	myPrometheus := prometheus.NewPrometheus(myCassandra, myBatch)

	if retryErr := util.Retry(config.CassandraInitSessionAttempts, config.CassandraInitSessionTimeout*time.Second, func() error {
		err := myCassandra.InitSession("cassandra0:9042")

		if err != nil {
			log.Printf("[cassandra] InitSession: Can't initialize session (%v)"+"\n", err)
		}

		return err
	}); retryErr == nil {
		log.Printf("[cassandra] InitSession: Session successfully initialized")
	} else {
		log.Fatalf("[cassandra] InitSession: Failed to initialize session")
	}

	stopChn := make(chan bool)

	go myBatch.RunFlushChecker(stopChn)

	if err := myPrometheus.InitServer(); err != nil {
		log.Fatalf("[prometheus] InitServer: Can't listen and serve (%v)", err)
	}

	stopChn <- true

	myCassandra.CloseSession()
}
