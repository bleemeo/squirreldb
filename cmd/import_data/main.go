// Copyright 2015-2019 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command used to import data generated from the PromQL API.
// The data can be generated with curl with a query or a query range:
// curl 'http://localhost:9201/api/v1/query_range' -d 'query=cpu_used' -d 'start=2023-02-23T10:30:30.781Z' \
// -d 'end=2023-02-23T10:31:00.781Z' -d 'step=10s' | go run ./cmd/import_data
// curl 'http://localhost:9201/api/v1/query' -d 'query=cpu_used[5m]' | go run ./cmd/import_data

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"squirreldb/logger"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
)

//nolint:gochecknoglobals
var (
	writeURL      = pflag.String("write-url", "http://localhost:9201/api/v1/write/", "Prometheus write API URL")
	excludeLabels = pflag.StringSlice("ignore-labels", []string{"server_group"}, "Labels to exclude when writing")
	addTime       = pflag.Duration("add-time", 0, "Add a fixed duration to all timestamps")
)

var errRequestFailed = errors.New("request failed")

type response struct {
	Data data `json:"data"`
}

type data struct {
	Result []metric `json:"result"`
}

type metric struct {
	Labels map[string]string `json:"metric"`
	Values []point           `json:"values"`
}

// point: [timestamp float, value string]
type point [2]interface{}

func main() {
	log.Logger = logger.NewTestLogger(false)

	pflag.Parse()

	err := run()
	if err != nil {
		log.Err(err).Msg("Failed to import data")
	}
}

func run() error {
	byteValue, _ := io.ReadAll(os.Stdin)

	var resp response

	err := json.Unmarshal(byteValue, &resp)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	series, err := metricstoTimeseries(resp.Data.Result)
	if err != nil {
		return fmt.Errorf("failed to parse metrics: %w", err)
	}

	err = writeTimeseries(series)
	if err != nil {
		return fmt.Errorf("failed to write timeseries: %w", err)
	}

	log.Info().Msgf("Successfully wrote %d timeseries", len(series))

	return nil
}

func metricstoTimeseries(metrics []metric) ([]prompb.TimeSeries, error) {
	series := make([]prompb.TimeSeries, 0, len(metrics))

	for _, metric := range metrics {
		labels := make([]prompb.Label, 0, len(metric.Labels))

		for name, value := range metric.Labels {
			if isExcludedLabel(name) {
				continue
			}

			labels = append(labels, prompb.Label{Name: name, Value: value})
		}

		samples := make([]prompb.Sample, 0, len(metric.Values))

		for _, point := range metric.Values {
			tsFloatSeconds, _ := point[0].(float64)
			valueString, _ := point[1].(string)

			ts := time.UnixMilli(int64(tsFloatSeconds * 1000)).Add(*addTime)

			value, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse float value: %w", err)
			}

			samples = append(samples,
				prompb.Sample{
					Timestamp: ts.UnixMilli(),
					Value:     value,
				},
			)
		}

		series = append(series,
			prompb.TimeSeries{
				Labels:  labels,
				Samples: samples,
			},
		)
	}

	return series, nil
}

func isExcludedLabel(name string) bool {
	for _, excludedName := range *excludeLabels {
		if name == excludedName {
			return true
		}
	}

	return false
}

func writeTimeseries(timeseries []prompb.TimeSeries) error {
	req := prompb.WriteRequest{
		Timeseries: timeseries,
	}

	body, err := req.Marshal()
	if err != nil {
		log.Printf("Unable to marshal req: %v", err)

		return err
	}

	compressedBody := snappy.Encode(nil, body)

	request, err := http.NewRequest(http.MethodPost, *writeURL, bytes.NewBuffer(compressedBody)) //nolint:noctx
	if err != nil {
		log.Printf("unable to create request: %v", err)

		return err
	}

	request.Header.Set("Content-Encoding", "snappy")
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %w", errRequestFailed, err)
	}

	defer response.Body.Close()

	if response.StatusCode >= http.StatusMultipleChoices {
		content, _ := io.ReadAll(response.Body)

		return fmt.Errorf("%w: response code = %d, content: %s", errRequestFailed, response.StatusCode, content)
	}

	_, err = io.Copy(io.Discard, response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	return nil
}
