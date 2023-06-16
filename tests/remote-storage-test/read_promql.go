package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

var errMatrixDiffError = errors.New("matrix mismatch")

type readPromQLRequest struct {
	name       string
	query      string
	rangeParam v1.Range
	response   model.Matrix
}

func readPromQL(ctx context.Context, now time.Time, promQLURL string, tenant string) error { //nolint:maintidx
	var (
		mutex    sync.Mutex
		reqCount int
	)

	start := time.Now()

	log.Print("Starting PromQL read phase")

	workChannel := make(chan readPromQLRequest, *threads)

	group, ctx := errgroup.WithContext(ctx)

	for n := 0; n < *threads; n++ {
		group.Go(func() error {
			count, err := readPromQLWorker(ctx, workChannel, promQLURL, tenant)

			mutex.Lock()
			reqCount += count
			mutex.Unlock()

			// make sure workChannel is drained
			for range workChannel { //nolint:revive
			}

			return err
		})
	}

	workChannel <- readPromQLRequest{
		// This test is mostly the same as on remote read, but:
		// * we can't read with 10 seconds step 1 week of data, so we use 1 minute step
		// * Start/End is inclusive which is not the case for our makeSampleModel
		name:  "read one week data",
		query: fmt.Sprintf(`large_write{nowStr="%s", size="week"}`, *nowStr),
		rangeParam: v1.Range{
			Start: now.Add(-time.Hour * 24 * 7),
			End:   now.Add(-time.Minute),
			Step:  time.Minute,
		},
		response: model.Matrix{
			&model.SampleStream{
				Metric: addTenantIfNotEmptyPromQL(
					model.Metric{
						"__name__": "large_write",
						"nowStr":   model.LabelValue(*nowStr),
						"size":     "week",
					},
					tenant,
				),
				Values: makeSampleModel(
					now.Add(-time.Hour*24*7),
					time.Minute,
					0,
					6,
					3600*24*7/10/6,
				),
			},
		},
	}

	workChannel <- readPromQLRequest{
		// This test should result in identical response because we only avg over a single point.
		// (we don't use 10s since we are perfectly aligned, 10s would take 2 points - PromQL use inclusive boundary).
		name:  "read one week data with avg_over_time",
		query: fmt.Sprintf(`avg_over_time(large_write{nowStr="%s", size="week"}[9s])`, *nowStr),
		rangeParam: v1.Range{
			Start: now.Add(-time.Hour * 24 * 7),
			End:   now,
			Step:  time.Minute,
		},
		response: model.Matrix{
			&model.SampleStream{
				Metric: addTenantIfNotEmptyPromQL(
					model.Metric{
						"nowStr": model.LabelValue(*nowStr),
						"size":   "week",
					},
					tenant,
				),
				Values: makeSampleModel(
					now.Add(-time.Hour*24*7),
					time.Minute,
					0,
					6,
					3600*24*7/10/6,
				),
			},
		},
	}

	workChannel <- readPromQLRequest{
		// Here since we use max and our makeSampleModel is increasing, taking 2 points is fine.
		name:  "read one week data with max_over_time",
		query: fmt.Sprintf(`max_over_time(large_write{nowStr="%s", size="week"}[10s])`, *nowStr),
		rangeParam: v1.Range{
			Start: now.Add(-time.Hour * 24 * 7),
			End:   now.Add(-time.Minute),
			Step:  time.Minute,
		},
		response: model.Matrix{
			&model.SampleStream{
				Metric: addTenantIfNotEmptyPromQL(
					model.Metric{
						"nowStr": model.LabelValue(*nowStr),
						"size":   "week",
					},
					tenant,
				),
				Values: makeSampleModel(
					now.Add(-time.Hour*24*7),
					time.Minute,
					0,
					6,
					3600*24*7/10/6,
				),
			},
		},
	}

	workChannel <- readPromQLRequest{
		// Same test as remote read (just use avg to avoid change in End timestamp)
		name:  "read one minutes from large_write",
		query: fmt.Sprintf(`avg_over_time(large_write{nowStr="%s", size="week"}[9s])`, *nowStr),
		rangeParam: v1.Range{
			Start: now.Add(-time.Minute),
			End:   now,
			Step:  10 * time.Second,
		},
		response: model.Matrix{
			&model.SampleStream{
				Metric: addTenantIfNotEmptyPromQL(
					model.Metric{
						"nowStr": model.LabelValue(*nowStr),
						"size":   "week",
					},
					tenant,
				),
				Values: makeSampleModel(
					now.Add(-time.Minute),
					10*time.Second,
					60474,
					1,
					6,
				),
			},
		},
	}

	workChannel <- readPromQLRequest{
		// Same test as remote read, but PromQL don't support multiple query in one call
		name:  "multiple-query-1of2",
		query: fmt.Sprintf(`sub_second{nowStr="%s"}`, *nowStr),
		rangeParam: v1.Range{
			Start: now.Add(-time.Minute),
			// We only write 1000 points (1 seconds). We remove a milliseconds since End is included
			End:  now.Add(-time.Minute + time.Second - time.Millisecond),
			Step: time.Millisecond,
		},
		response: model.Matrix{
			&model.SampleStream{
				Metric: addTenantIfNotEmptyPromQL(
					model.Metric{
						"__name__": "sub_second",
						"nowStr":   model.LabelValue(*nowStr),
					},
					tenant,
				),
				Values: makeSampleModel(
					now.Add(-time.Minute),
					time.Millisecond,
					0,
					0.001,
					1000,
				),
			},
		},
	}

	workChannel <- readPromQLRequest{
		// Same test as remote read, but PromQL don't support multiple query in one call
		// Use avg_over_time to fix End boundary
		name:  "multiple-query-2of2",
		query: fmt.Sprintf(`avg_over_time(high_precision{nowStr="%s"}[900ms])`, *nowStr),
		rangeParam: v1.Range{
			Start: now.Add(-time.Minute),
			End:   now,
			Step:  time.Second,
		},
		response: model.Matrix{
			&model.SampleStream{
				Metric: addTenantIfNotEmptyPromQL(
					model.Metric{
						"nowStr": model.LabelValue(*nowStr),
					},
					tenant,
				),
				Values: makeSampleModel(
					now.Add(-time.Minute),
					time.Second,
					42.123456789,
					00.000000001,
					60,
				),
			},
		},
	}

	for n := 0; n < *scale; n++ {
		samples := makeSampleModel(
			now.Add(-time.Minute),
			10*time.Second,
			100,
			-0.1,
			6,
		)

		samples5times := makeSampleModel(
			now.Add(-time.Minute),
			10*time.Second,
			5*100,
			5*-0.1,
			6,
		)

		workChannel <- readPromQLRequest{
			name:  fmt.Sprintf("filler-batch-full-%d", n),
			query: fmt.Sprintf(`avg_over_time(filler{nowStr="%s",batch="yes",scale="%d"}[9s])`, *nowStr, n),
			rangeParam: v1.Range{
				Start: now.Add(-time.Minute),
				End:   now,
				Step:  10 * time.Second,
			},
			response: model.Matrix{
				&model.SampleStream{
					Metric: addTenantIfNotEmptyPromQL(
						model.Metric{
							"nowStr": model.LabelValue(*nowStr),
							"batch":  "yes",
							"scale":  model.LabelValue(strconv.FormatInt(int64(n), 10)),
						},
						tenant,
					),
					Values: samples,
				},
			},
		}

		for i := 0; i < 5; i++ {
			metricName := fmt.Sprintf(`filler{nowStr="%s",batch="yes",scale="%d"}`, *nowStr, n)
			workChannel <- readPromQLRequest{
				name: fmt.Sprintf("filler-batch-full-%d-cache", n),
				query: fmt.Sprintf(
					`avg_over_time(%s[9s]) + avg_over_time(%s[9s]) + %s + %s + %s`,
					metricName,
					metricName,
					metricName,
					metricName,
					metricName,
				),
				rangeParam: v1.Range{
					Start: now.Add(-time.Minute),
					End:   now.Add(-time.Second),
					Step:  10 * time.Second,
				},
				response: model.Matrix{
					&model.SampleStream{
						Metric: addTenantIfNotEmptyPromQL(
							model.Metric{
								"nowStr": model.LabelValue(*nowStr),
								"batch":  "yes",
								"scale":  model.LabelValue(strconv.FormatInt(int64(n), 10)),
							},
							tenant,
						),
						Values: samples5times,
					},
				},
			}
		}
	}

	samplesAllSum := makeSampleModel(
		now.Add(-time.Minute),
		10*time.Second,
		100*float64(*scale),
		-0.1*float64(*scale),
		6,
	)

	workChannel <- readPromQLRequest{
		name:  "filler-batch-full-sum",
		query: fmt.Sprintf(`sum by (nowStr) (avg_over_time(filler{nowStr="%s",batch="yes"}[9s]))`, *nowStr),
		rangeParam: v1.Range{
			Start: now.Add(-time.Minute),
			End:   now,
			Step:  10 * time.Second,
		},
		response: model.Matrix{
			&model.SampleStream{
				Metric: addTenantIfNotEmptyPromQL(
					model.Metric{
						"nowStr": model.LabelValue(*nowStr),
					},
					tenant,
				),
				Values: samplesAllSum,
			},
		},
	}

	close(workChannel)

	err := group.Wait()

	duration := time.Since(start)
	log.Printf(
		"Finished PromQL read phase in %s with %d request (%.0f req/seconds)",
		duration,
		reqCount,
		float64(reqCount)/duration.Seconds(),
	)

	return err
}

type headerClient struct {
	api.Client
	SquirrelDBTenantHeader string
}

func (hc headerClient) Do(ctx context.Context, req *http.Request) (*http.Response, []byte, error) {
	req.Header.Set("X-SquirrelDB-Tenant", hc.SquirrelDBTenantHeader)

	return hc.Client.Do(ctx, req)
}

func readPromQLWorker(
	ctx context.Context,
	workChannel chan readPromQLRequest,
	readURL string,
	squirrelDBTenantHeader string,
) (int, error) {
	var (
		lastEqualError error
		count          int
	)

	client, err := api.NewClient(api.Config{
		Address: readURL,
	})
	if err != nil {
		return 0, err
	}

	if squirrelDBTenantHeader != "" {
		client = headerClient{Client: client, SquirrelDBTenantHeader: squirrelDBTenantHeader}
	}

	promQLAPI := v1.NewAPI(client)

	for req := range workChannel {
		count++

		if ctx.Err() != nil {
			if lastEqualError == nil {
				lastEqualError = ctx.Err()
			}

			break
		}

		result, warnings, err := promQLAPI.QueryRange(ctx, req.query, req.rangeParam)
		if err != nil {
			return count, err
		}

		if warnings != nil {
			log.Printf("had warning: %v", warnings)
		}

		if result.Type() != model.ValMatrix {
			return count, fmt.Errorf("unexpected type %s, want %s", result.Type(), model.ValMatrix)
		}

		matrix, ok := result.(model.Matrix)
		if !ok {
			return count, fmt.Errorf("unexpected type it's not a Matrix")
		}

		opts := []cmp.Option{
			// Convert model.SampleValue to float64 (their actual type) for EquateApprox to works.
			cmpopts.AcyclicTransformer("toFloat", func(s model.SampleValue) float64 { return float64(s) }),
			cmpopts.EquateApprox(0.001, 0),
		}

		if diff := cmp.Diff(req.response, matrix, opts...); diff != "" {
			lastEqualError = fmt.Errorf("%s %w (-want +got)\n%s", req.name, errMatrixDiffError, diff)
		}
	}

	return count, lastEqualError
}

func addTenantIfNotEmptyPromQL(metric model.Metric, tenant string) model.Metric {
	if tenant != "" {
		newMetric := make(model.Metric, len(metric)+1)

		for k, v := range metric {
			newMetric[k] = v
		}

		newMetric[tenantLabelName] = model.LabelValue(tenant)

		return newMetric
	}

	return metric
}
