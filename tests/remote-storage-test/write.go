package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/sync/errgroup"
)

func write(ctx context.Context, now time.Time, writeURL, tenant string) error {
	log.Println("Starting write phase")

	workChannel := make(chan prompb.WriteRequest, *threads)

	group, ctx := errgroup.WithContext(ctx)

	for n := 0; n < *threads; n++ {
		group.Go(func() error {
			err := writeWorker(ctx, workChannel, writeURL, tenant)

			// make sure workChannel is drained
			for range workChannel {
			}

			return err
		})
	}

	// First we generate "historical" data, because some store don't like
	// out of order insertion
	workChannel <- prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "large_write"},
					{Name: "size", Value: "week"},
					{Name: "nowStr", Value: *nowStr},
				},
				Samples: makeSample(
					now.Add(-time.Hour*24*7),
					10*time.Second,
					0,
					1,
					3600*24*7/10,
				),
			},
		},
	}
	workChannel <- prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "large_write"},
					{Name: "size", Value: "hour"},
					{Name: "nowStr", Value: *nowStr},
				},
				Samples: makeSample(
					now.Add(-time.Hour),
					10*time.Second,
					0,
					2,
					3600/10,
				),
			},
		},
	}
	workChannel <- prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "sub_second"},
					{Name: "nowStr", Value: *nowStr},
				},
				Samples: makeSample(
					now.Add(-time.Minute),
					time.Millisecond,
					0,
					0.001,
					1000,
				),
			},
		},
	}
	workChannel <- prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "high_precision"},
					{Name: "nowStr", Value: *nowStr},
				},
				Samples: makeSample(
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
		samples := makeSample(
			now.Add(-time.Minute),
			10*time.Second,
			100,
			-0.1,
			6,
		)
		workChannel <- prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "filler"},
						{Name: "batch", Value: "yes"},
						{Name: "scale", Value: strconv.FormatInt(int64(n), 10)},
						{Name: "nowStr", Value: *nowStr},
					},
					Samples: samples,
				},
			},
		}

		for i := 0; i < 6; i++ {
			workChannel <- prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "filler"},
							{Name: "scale", Value: strconv.FormatInt(int64(n), 10)},
							{Name: "nowStr", Value: *nowStr},
							{Name: "batch", Value: "no"},
						},
						Samples: samples[i : i+1],
					},
				},
			}
		}
	}

	close(workChannel)

	err := group.Wait()

	log.Println("Finished write phase")

	return err
}

func writeWorker(ctx context.Context, workChannel chan prompb.WriteRequest, writeURL, tenant string) error {
	for req := range workChannel {
		if ctx.Err() != nil {
			break
		}

		body, err := req.Marshal()
		if err != nil {
			log.Printf("Unable to marshal req: %v", err)

			return err
		}

		compressedBody := snappy.Encode(nil, body)

		request, err := http.NewRequestWithContext(ctx, http.MethodPost, writeURL, bytes.NewBuffer(compressedBody))
		if err != nil {
			log.Printf("unable to create request: %v", err)

			return err
		}

		request.Header.Set("Content-Encoding", "snappy")
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")
		request.Header.Set("X-SquirrelDB-Tenant", tenant)

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
	}

	return ctx.Err()
}
