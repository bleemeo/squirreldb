package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"squirreldb/daemon"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
)

//nolint:lll,gochecknoglobals
var (
	remoteWrite = flag.String("write-url", "", "URL of remote write (if both url are unset, start a built-in SquirrelDB and use it)")
	remoteRead  = flag.String("read-url", "", "URL of read write (if both url are unset, start a built-in SquirrelDB and use it)")
	tenant      = flag.String("tenant", "", "Tenant header")
	threads     = flag.Int("threads", 2, "Number of writing/reading threads")
	scale       = flag.Int("scale", 5, "Scaling factor")
	skipWrite   = flag.Bool("skip-write", false, "Skip write phase")
	skipRead    = flag.Bool("skip-read", false, "Skip read phase")
	nowStr      = flag.String("now", time.Now().Round(10*time.Second).Format(time.RFC3339), "Value for \"now\"")
)

func main() {
	daemon.SetTestEnvironment()

	err := daemon.RunWithSignalHandler(run)

	if *remoteRead == "" && *remoteWrite == "" {
		metricResult, _ := prometheus.DefaultGatherer.Gather()
		for _, mf := range metricResult {
			_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
		}
	}

	if err != nil {
		log.Fatal().Err(err).Msg("Run daemon failed")
	}
}

func run(ctx context.Context) error {
	cfg, warnings, err := daemon.Config()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	readURL := *remoteRead
	writeURL := *remoteWrite

	if *remoteRead == "" && *remoteWrite == "" {
		squirreldb := &daemon.SquirrelDB{
			Config: cfg,
			Logger: log.With().Str("component", "daemon").Logger(),
		}

		err = squirreldb.DropCassandraData(ctx, false)
		if err != nil {
			return err
		}

		err = squirreldb.DropTemporaryStore(ctx, false)
		if err != nil {
			return err
		}

		err = squirreldb.Start(ctx)
		if err != nil {
			return err
		}

		readURL = fmt.Sprintf("http://127.0.0.1:%d/api/v1/read", squirreldb.ListenPort())
		writeURL = fmt.Sprintf("http://127.0.0.1:%d/api/v1/write", squirreldb.ListenPort())

		defer squirreldb.Stop()
	}

	if readURL == "" && !*skipRead {
		return errors.New("remote-read url is unset")
	}

	if writeURL == "" && !*skipWrite {
		return errors.New("remote-write url is unset")
	}

	now, err := time.Parse(time.RFC3339, *nowStr)
	if err != nil {
		return err
	}

	log.Printf("now = %v", now)

	if !*skipWrite {
		if err := write(ctx, now, writeURL, *tenant); err != nil {
			return err
		}
	}

	if !*skipRead {
		if err := read(ctx, now, readURL); err != nil {
			return err
		}
	}

	return nil
}

func time2Millisecond(t time.Time) int64 {
	return t.Unix()*1000 + (t.UnixNano()%1e9)/1e6
}

func makeSample(
	fromTime time.Time,
	stepTime time.Duration,
	fromValue float64,
	stepValue float64,
	count int,
) []prompb.Sample {
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
