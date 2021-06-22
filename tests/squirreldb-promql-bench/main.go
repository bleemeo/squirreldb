package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// Returns the API
func InitAPI(url string) v1.API {
	client, err := api.NewClient(api.Config{
		Address: url,
	})
	if err != nil {
		log.Fatalf("Error creating client: %v\n", err)
	}
	return v1.NewAPI(client)
}

// Evaluates a query at a single point in time
func Query(query string, ts time.Time) (model.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, warnings, err := API.Query(ctx, query, ts)
	if len(warnings) > 0 {
		log.Printf("Warnings: %v\n", warnings)
	}
	return result, err
}

// Run a query a maximum number of times during the given time
func testQuery(c chan int, query string, duration time.Duration) {
	nbQueries := 0
	t1 := time.Now()
	t2 := t1

	for t2.Sub(t1) < duration {
		res, err := Query(query, time.Now())
		if err != nil {
			log.Fatalf("Failed to run the query: %v\n", err)
		} else if res.String() == "" {
			log.Fatalln("Query returned no output")
		}
		nbQueries++
		t2 = time.Now()
	}

	c <- nbQueries
}

// Run testQuery in parallel
func testQueryParallel(query string, parallelQueries int, duration time.Duration) {
	log.Printf("Executing query %v with %v goroutines during %v\n", query, parallelQueries, duration)

	resultsChan := make(chan int)
	for i := 0; i < parallelQueries; i++ {
		go testQuery(resultsChan, query, duration)
	}

	nbQueries := 0
	for i := 0; i < parallelQueries; i++ {
		nbQueries += <-resultsChan
	}

	log.Printf("Result: %v query/s\n", nbQueries/int(duration.Seconds()))
}

var API v1.API

func main() {
	urlAPI := flag.String("url", "http://localhost:9201", "SquirrelDB url")
	query := flag.String("query", "node_load5", "Query to benchmark")
	parallelQueries := flag.Int("parallel", 10, "Number of concurrent queries")
	runDuration := flag.Duration("run-time", 10*time.Second, "Duration of the benchmark")

	flag.Parse()
	API = InitAPI(*urlAPI)
	testQueryParallel(*query, *parallelQueries, *runDuration)
}
