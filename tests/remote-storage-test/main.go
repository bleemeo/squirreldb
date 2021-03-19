package main

import (
	"context"
	"flag"
	"log"
	"os"
	"squirreldb/daemon"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
)

//nolint: gochecknoglobals
var (
	remoteWrite     = flag.String("write-url", "http://localhost:9201/write", "URL of remote write")
	remoteRead      = flag.String("read-url", "http://localhost:9201/read", "URL of read write")
	threads         = flag.Int("threads", 2, "Number of writing/reading threads")
	scale           = flag.Int("scale", 5, "Scaling factor")
	skipWrite       = flag.Bool("skip-write", false, "Skip write phase")
	skipRead        = flag.Bool("skip-read", false, "Skip read phase")
	nowStr          = flag.String("now", time.Now().Round(10*time.Second).Format(time.RFC3339), "Value for \"now\"")
	startSquirrelDB = flag.Bool("start-bultin-squirreldb", false, "Start a SquirrelDB. read & write URL are ignored in this case")
)

func main() {
	flag.Parse()

	if _, ok := os.LookupEnv("SQUIRRELDB_CASSANDRA_KEYSPACE"); !ok {
		// If not explicitly changed, use squirreldb_test as keyspace. We do
		// not want to touch real data
		os.Setenv("SQUIRRELDB_CASSANDRA_KEYSPACE", "squirreldb_test")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_INTERNAL_REDIS_NAMESPACE"); !ok {
		// If not explicitly changed, use test: as namespace. We do
		// not want to touch real data
		os.Setenv("SQUIRRELDB_INTERNAL_REDIS_NAMESPACE", "test:")
	}

	err := daemon.RunWithSignalHandler(run)

	metricResult, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range metricResult {
		_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	if *startSquirrelDB {
		squirreldb, err := daemon.New()
		if err != nil {
			return err
		}

		err = squirreldb.Start(ctx)
		if err != nil {
			return err
		}

		defer squirreldb.Stop()
	}

	now, err := time.Parse(time.RFC3339, *nowStr)
	if err != nil {
		return err
	}

	log.Printf("now = %v", now)

	if !*skipWrite {
		if err := write(ctx, now); err != nil {
			return err
		}
	}

	if !*skipRead {
		if err := read(ctx, now); err != nil {
			return err
		}
	}

	return nil
}

func time2Millisecond(t time.Time) int64 {
	return t.Unix()*1000 + (t.UnixNano()%1e9)/1e6
}

func makeSample(fromTime time.Time, stepTime time.Duration, fromValue float64, stepValue float64, count int) []prompb.Sample {
	result := make([]prompb.Sample, count)

	for i := range result {
		currentTime := fromTime.Add(stepTime * time.Duration(i))
		result[i] = prompb.Sample{
			Value:     fromValue + stepValue*float64(i),
			Timestamp: time2Millisecond(currentTime),
		}
	}

	return result
}
