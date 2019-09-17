package main

import (
	"hamsterdb/batch"
	"hamsterdb/cassandra"
	"hamsterdb/config"
	"hamsterdb/prometheus"
	"hamsterdb/store"
	"log"
	"time"
)

func main() {
	myStore := store.NewStore()
	myCassandra := cassandra.NewCassandra()
	myBatch := batch.NewBatch(myStore, myCassandra)
	myPrometheus := prometheus.NewPrometheus(myCassandra, myBatch)

	func(attempts int, timeout time.Duration) {
		for i := attempts; i >= 0; i-- {
			if err := myCassandra.InitSession("cassandra0:9042"); err != nil {
				log.Printf("[cassandra] InitSession: Can't initialize session (%v)"+"\n", err)
				log.Printf("|______________________  Retry in %v (remaining attempts: %d)", timeout, i)
			} else {
				log.Printf("[cassandra] InitSession: Session successfully initialized")
				return
			}

			time.Sleep(timeout)
		}

		log.Fatalf("[cassandra] InitSession: Failed to initialize session")
	}(config.CassandraInitSessionAttempts, config.CassandraInitSessionTimeout*time.Second)

	if err := myPrometheus.InitServer(); err != nil {
		log.Fatalf("[prometheus] InitServer: Can't listen and serve (%v)", err)
	}

	myCassandra.CloseSession()
}
