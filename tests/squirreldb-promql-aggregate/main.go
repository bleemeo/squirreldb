package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"squirreldb/daemon"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
)

var (
	errUnexpectedSeries = errors.New("unexpected series returned")
	errUnexpectedValue  = errors.New("unexpected value")
)

func main() {
	daemon.SetTestEnvironment()

	err := daemon.RunWithSignalHandler(run)
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

	squirreldb := &daemon.SquirrelDB{
		Config:         cfg,
		MetricRegistry: prometheus.DefaultRegisterer,
		Logger:         log.With().Str("component", "daemon").Logger(),
	}

	err = squirreldb.DropCassandraData(ctx, false)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to drop keyspace")
	}

	err = squirreldb.DropTemporaryStore(ctx, false)
	if err != nil {
		return err
	}

	err = squirreldb.Start(ctx)
	if err != nil {
		return err
	}

	defer squirreldb.Stop()

	squirrelDBURL := fmt.Sprintf("http://127.0.0.1:%d", squirreldb.ListenPort())
	minTS := time.Date(2023, 2, 3, 14, 0, 0, 0, time.UTC)
	maxTS := time.Date(2023, 2, 4, 14, 0, 0, 0, time.UTC)

	// Write data with a known average/min/max over 5 minutes.
	// Check the average/min/max with PromQL queries.
	// Pre-aggregate the data.
	// Verify that the PromQL queries return the same results.
	err = write(ctx, minTS, maxTS, squirrelDBURL)
	if err != nil {
		return err
	}

	err = checkPromQL(ctx, minTS, maxTS, squirrelDBURL)
	if err != nil {
		return err
	}

	err = preaggregate(ctx, squirrelDBURL, minTS, minTS.Add(24*time.Hour))
	if err != nil {
		return err
	}

	err = checkPromQL(ctx, minTS, maxTS, squirrelDBURL)
	if err != nil {
		return err
	}

	log.Info().Msg("End of PromQL tests on aggregated data")

	return nil
}

func write(ctx context.Context, minTS, maxTS time.Time, squirrelDBURL string) error {
	step := 10 * time.Second

	// Consecutive values of a metric. With one point per 10s, this is 5 minutes of points,
	// which also corresponds to the aggregation size.
	// These values have an average of 1, a max of 2 and a min of 0 on 5 minutes.
	values := []int{
		2, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 0,
	}

	req := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "cpu_used"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: makeSamples(minTS, step, values, int(maxTS.Sub(minTS)/(5*time.Minute))),
			},
		},
	}

	err := writeSeries(ctx, req, squirrelDBURL)
	if err != nil {
		return err
	}

	return nil
}

// Check that PromQL queries with avg/min/max return the correct data.
func checkPromQL(ctx context.Context, minTS, maxTS time.Time, squirrelDBURL string) error {
	client, err := api.NewClient(api.Config{
		Address: squirrelDBURL,
	})
	if err != nil {
		return fmt.Errorf("create Prometheus client: %w", err)
	}

	api := v1.NewAPI(client)
	step := 10 * time.Minute
	v1Range := v1.Range{
		Start: minTS.Add(step),
		End:   maxTS,
		Step:  step,
	}

	expectedValueByRequest := map[string]float64{
		`min_over_time(cpu_used{instance="server-1"}[30m])`: 0,
		`max_over_time(cpu_used{instance="server-1"}[30m])`: 2,
		`avg_over_time(cpu_used{instance="server-1"}[30m])`: 1,
	}

	for request, expectedValue := range expectedValueByRequest {
		log.Info().Msgf("Running query %s", request)

		rangeValues, warnings, err := api.QueryRange(ctx, request, v1Range)
		for _, warning := range warnings {
			log.Warn().Msgf("PromQL query warning: %s", warning)
		}

		if err != nil {
			return err
		}

		matrix, _ := rangeValues.(model.Matrix)
		if len(matrix) != 1 {
			return fmt.Errorf("%w: got %d series, wanted 1", errUnexpectedSeries, len(matrix))
		}

		for _, sample := range matrix[0].Values {
			// Allow 5% of error.
			if math.Abs(float64(sample.Value)-expectedValue) > 5*expectedValue/100 {
				return fmt.Errorf(
					"%w: got value %f at %s, wanted %f for query %s",
					errUnexpectedValue, sample.Value, sample.Timestamp.Time(), expectedValue, request,
				)
			}
		}
	}

	return nil
}

func preaggregate(ctx context.Context, squirrelDBURL string, from time.Time, to time.Time) error {
	aggregateURL := fmt.Sprintf(
		"%s/debug/preaggregate?from=%s&to=%s",
		squirrelDBURL,
		from.Format("2006-01-02"),
		to.Format("2006-01-02"),
	)

	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, aggregateURL, nil)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Printf("write failed: %v", err)

		return err
	}

	if response.StatusCode >= http.StatusMultipleChoices {
		content, _ := io.ReadAll(response.Body)
		log.Printf("Response code = %d, content: %s", response.StatusCode, content)

		return err
	}

	_, err = io.Copy(io.Discard, response.Body)
	if err != nil {
		log.Printf("Failed to read response: %v", err)

		return err
	}

	return response.Body.Close()
}

// Make repeting samples from the values.
func makeSamples(
	fromTime time.Time,
	stepTime time.Duration,
	values []int,
	count int,
) []prompb.Sample {
	samples := make([]prompb.Sample, 0, count*len(values))

	for i := 0; i < count; i++ {
		for j, value := range values {
			samples = append(samples, prompb.Sample{
				Value:     float64(value),
				Timestamp: fromTime.Add(stepTime * time.Duration(len(values)*i+j)).UnixMilli(),
			})
		}
	}

	return samples
}

func writeSeries(ctx context.Context, req prompb.WriteRequest, squirrelDBURL string) error {
	log.Info().Msg("Writing series")

	if ctx.Err() != nil {
		return ctx.Err()
	}

	body, err := req.Marshal()
	if err != nil {
		log.Printf("Unable to marshal req: %v", err)

		return err
	}

	writeURL := squirrelDBURL + "/api/v1/write"
	compressedBody := snappy.Encode(nil, body)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, writeURL, bytes.NewBuffer(compressedBody))
	if err != nil {
		log.Printf("unable to create request: %v", err)

		return err
	}

	request.Header.Set("Content-Encoding", "snappy")
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Printf("write failed: %v", err)

		return err
	}

	if response.StatusCode >= http.StatusMultipleChoices {
		content, _ := io.ReadAll(response.Body)
		log.Printf("Response code = %d, content: %s", response.StatusCode, content)

		return err
	}

	_, err = io.Copy(io.Discard, response.Body)
	if err != nil {
		log.Printf("Failed to read response: %v", err)

		return err
	}

	response.Body.Close()

	return nil
}
