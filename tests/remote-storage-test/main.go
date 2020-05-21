package main

import (
	"flag"
	"log"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

// nolint: gochecknoglobals
var (
	remoteWrite = flag.String("write-url", "http://localhost:9201/write", "URL of remote write")
	remoteRead  = flag.String("read-url", "http://localhost:9201/read", "URL of read write")
	seed        = flag.Int64("seed", 42, "Seed used in random generator")
	threads     = flag.Int("threads", 1, "Number of writing/reading threads")
	scale       = flag.Int("scale", 1, "Scaling factor")
	skipWrite   = flag.Bool("skip-write", false, "Skip write phase")
	skipRead    = flag.Bool("skip-read", false, "Skip read phase")
	nowStr      = flag.String("now", time.Now().Round(10*time.Second).Format(time.RFC3339), "Value for \"now\"")
)

func main() {
	flag.Parse()

	now, err := time.Parse(time.RFC3339, *nowStr)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("now = %v", now)

	if !*skipWrite {
		write(now)
	}
	if !*skipRead {
		read(now)
	}
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
