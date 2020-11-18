package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func write(now time.Time) {
	log.Println("Starting write phase")

	workChannel := make(chan prompb.WriteRequest, *threads)

	var wg sync.WaitGroup

	for n := 0; n < *threads; n++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			writeWorker(workChannel)
		}()
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
	wg.Wait()

	log.Println("Finished write phase")
}

func writeWorker(workChannel chan prompb.WriteRequest) {
	for req := range workChannel {
		body, err := req.Marshal()
		if err != nil {
			log.Fatalf("Unable to marshal req: %v", err)
		}

		compressedBody := snappy.Encode(nil, body)

		request, err := http.NewRequest("POST", *remoteWrite, bytes.NewBuffer(compressedBody)) // nolint: noctx
		if err != nil {
			log.Fatalf("unable to create request: %v", err)
		}

		request.Header.Set("Content-Encoding", "snappy")
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			log.Fatalf("write failed: %v", err)
		}

		if response.StatusCode >= 300 {
			content, _ := ioutil.ReadAll(response.Body)
			log.Fatalf("Response code = %d, content: %s", response.StatusCode, content)
		}

		_, err = io.Copy(ioutil.Discard, response.Body)
		if err != nil {
			log.Fatalf("Failed to read response: %v", err)
		}

		response.Body.Close()
	}
}
