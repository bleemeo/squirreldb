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

// Returns the API.
func initAPI(url string) v1.API {
	client, err := api.NewClient(api.Config{
		Address: url,
	})
	if err != nil {
		log.Fatalf("Error creating client: %v\n", err)
	}

	return v1.NewAPI(client)
}

// Evaluates a query at a single point in time.
func query(ctx context.Context, api v1.API, query string, ts time.Time) (model.Value, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, warnings, err := api.Query(ctx, query, ts)
	if len(warnings) > 0 {
		log.Printf("Warnings: %v\n", warnings)
	}

	return result, err
}

// Evaluates a query at a single point in time.
func queryRange(ctx context.Context, api v1.API, query string, queryRange v1.Range) (model.Value, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, warnings, err := api.QueryRange(ctx, query, queryRange)
	if len(warnings) > 0 {
		log.Printf("Warnings: %v\n", warnings)
	}

	return result, err
}

// Run a query a maximum number of times during the given time.
func testQuery(ctx context.Context, api v1.API, c chan int, q string, rangeDuration time.Duration, rangeStep time.Duration, allowEmpty bool) {
	nbQueries := 0

	var (
		res model.Value
		err error
	)

	for ctx.Err() == nil {
		if rangeStep == 0 {
			res, err = query(ctx, api, q, time.Now())
		} else {
			res, err = queryRange(ctx, api, q, v1.Range{
				Start: time.Now().Add(-rangeDuration),
				End:   time.Now(),
				Step:  rangeStep,
			})
		}

		if ctx.Err() != nil {
			break
		}

		if err != nil {
			log.Fatalf("Failed to run the query: %v\n", err)
		} else if !allowEmpty && res.String() == "" {
			log.Fatalln("Query returned no output")
		}

		nbQueries++
	}

	c <- nbQueries
}

// Run testQuery in parallel.
func testQueryParallel(api v1.API, query string, parallelQueries int, duration time.Duration, queryRange time.Duration, stepRange time.Duration, allowEmpty bool) {
	log.Printf("Executing query %v with %v goroutines during %v\n", query, parallelQueries, duration)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	resultsChan := make(chan int)
	for i := 0; i < parallelQueries; i++ {
		go testQuery(ctx, api, resultsChan, query, queryRange, stepRange, allowEmpty)
	}

	nbQueries := 0
	for i := 0; i < parallelQueries; i++ {
		nbQueries += <-resultsChan
	}

	log.Printf("Result: %v query/s\n", nbQueries/int(duration.Seconds()))
}

func main() {
	urlAPI := flag.String("url", "http://localhost:9201", "SquirrelDB url")
	query := flag.String("query", "node_load5", "Query to benchmark")
	parallelQueries := flag.Int("parallel", 10, "Number of concurrent queries")
	runDuration := flag.Duration("run-time", 10*time.Second, "Duration of the benchmark")
	queryRange := flag.Duration("query-range", time.Hour, "Query range duraton. Only used if query-step is non-zero")
	queryStep := flag.Duration("query-step", 0, "Query step. If zero, query is used and not query_range")
	allowEmpty := flag.Bool("allow-empty-response", false, "Allow empty reply. By default it's a fatal error to have an empty response")
	flag.Parse()

	API := initAPI(*urlAPI)
	testQueryParallel(API, *query, *parallelQueries, *runDuration, *queryRange, *queryStep, *allowEmpty)
}
