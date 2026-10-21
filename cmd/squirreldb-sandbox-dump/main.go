// Copyright 2015-2026 Bleemeo
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

// Command squirreldb-sandbox-dump dumps samples from a running SquirrelDB (or any
// Prometheus API) in the format used by the samples files of a sandbox dataset.
// See docs/sandbox.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/sandbox/source/file"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

//nolint:gochecknoglobals
var (
	metricFile     = flag.String("metric-file", "", "File with list of metrics")
	promqlEndpoint = flag.String("promql-url", "http://localhost:9201/api/v1/query_range", "URL to PromQL query-range")
	startText      = flag.String(
		"start", time.Now().Add(-time.Hour).Truncate(10*time.Second).Format(time.RFC3339), "Dump from this date",
	)
	endText     = flag.String("end", time.Now().Truncate(10*time.Second).Format(time.RFC3339), "Dump until this date")
	matcherText = flag.String("matchers", "", "Additional PromQL matchers. E.g. instance=\"localhost:9100\"")
	step        = flag.Duration("step", 10*time.Second, "Step between each points")
	maxExample  = flag.Int(
		"max-example", 2, "When multiple series match labels to extract, only take max-example of them",
	)
	dropLabelsText = flag.String(
		"drop-labels", "instance,instance_uuid,__bleemeo_account__", "Comma separated labels to drop from output.",
	)
	reLabelsText      = flag.String("relabels", "", "Set/overide some labels. Use same syntax as matcher: item=\"value\"")
	tenantHeaderValue = flag.String("tenant-header-value", "", "Value for the X-SquirrelDB-Tenant header")
)

var (
	ErrUnexpectedDelay         = errors.New("unexpected delay between points")
	ErrEmptyResponse           = errors.New("empty response")
	ErrUnexpectedResponseType  = errors.New("unexpected response type")
	ErrPointBadLength          = errors.New("points expect 2 fields")
	ErrNoTenantHeaderSpecified = errors.New("no tenant header specified; use the 'tenant-header-value' flag to provide one") //nolint:lll
)

func main() {
	flag.Parse()

	if err := do(context.Background()); err != nil {
		log.Println(err)

		os.Exit(1)
	}
}

type promQLReply struct {
	Status string    `json:"status"`
	Data   dataReply `json:"data"`
}

type dataReply struct {
	ResultType string         `json:"resultType"` //nolint:tagliatelle
	Result     []serieSamples `json:"result"`
}

type serieSamples struct {
	Metric map[string]string `json:"metric"`
	Values []point           `json:"values"`
}

type point struct {
	Timestamp int64  `json:"Timestamp"` //nolint:tagliatelle
	Value     string `json:"Value"`     //nolint:tagliatelle
}

func (p *point) UnmarshalJSON(buf []byte) error {
	tmp := []any{&p.Timestamp, &p.Value}

	if err := json.Unmarshal(buf, &tmp); err != nil {
		return fmt.Errorf("decode point: %w", err)
	}

	if len(tmp) != 2 {
		return fmt.Errorf("%w: got %d fields", ErrPointBadLength, len(tmp))
	}

	return nil
}

func processArgs() (
	matchersString string, relabels labels.Labels, dropLabels []string, start time.Time, end time.Time, err error,
) {
	matchers, err := parser.ParseMetricSelector("{" + *matcherText + "}")
	if err != nil {
		return "", labels.EmptyLabels(), nil, start, end, fmt.Errorf("can't parse matcher: %w", err)
	}

	matchersStrings := make([]string, len(matchers))

	for i, m := range matchers {
		matchersStrings[i] = m.String()
	}

	start, err = time.Parse(time.RFC3339, *startText)
	if err != nil {
		return "", labels.EmptyLabels(), nil, start, end, fmt.Errorf("can't parse start date: %w", err)
	}

	end, err = time.Parse(time.RFC3339, *endText)
	if err != nil {
		return "", labels.EmptyLabels(), nil, start, end, fmt.Errorf("can't parse end date: %w", err)
	}

	if *dropLabelsText != "" {
		dropLabels = strings.Split(*dropLabelsText, ",")
		sort.Strings(dropLabels)
	}

	if *reLabelsText != "" {
		tmp, err := parser.ParseMetricSelector("{" + *reLabelsText + "}")
		if err != nil {
			return "", labels.EmptyLabels(), nil, start, end, fmt.Errorf("can't parse matcher: %w", err)
		}

		tmp2 := make(map[string]string)

		for _, v := range tmp {
			tmp2[v.Name] = v.Value
		}

		relabels = labels.FromMap(tmp2)
	}

	if *tenantHeaderValue == "" {
		return "", labels.EmptyLabels(), nil, start, end, ErrNoTenantHeaderSpecified
	}

	return strings.Join(matchersStrings, ","), relabels, dropLabels, start, end, nil
}

func do(ctx context.Context) error {
	metricName := make(map[string]any)

	matchersString, relabels, dropLabels, start, end, err := processArgs()
	if err != nil {
		return err
	}

	metrics, err := loadMetrics(*metricFile)
	if err != nil {
		return fmt.Errorf("read metric list: %w", err)
	}

	for _, m := range metrics {
		metricName[m.Labels.Get("__name__")] = nil
	}

	log.Printf("Will query %d metrics", len(metricName))

	sortedMetricName := make([]string, 0, len(metricName))

	for name := range metricName {
		sortedMetricName = append(sortedMetricName, name)
	}

	sort.Strings(sortedMetricName)

	writer := json.NewEncoder(os.Stdout)

	for _, name := range sortedMetricName {
		if err := dumpMetric(ctx, writer, name, matchersString, dropLabels, relabels, start, end); err != nil {
			return err
		}
	}

	return nil
}

func loadMetrics(path string) ([]file.Metric, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	return file.ParseMetrics(f, nil)
}

func dumpMetric(ctx context.Context, writer *json.Encoder,
	name string, matchers string, dropLabels []string, relabels labels.Labels, start time.Time, end time.Time,
) error {
	var response promQLReply

	query := fmt.Sprintf("%s{%s}", name, matchers)

	u, err := url.Parse(*promqlEndpoint)
	if err != nil {
		return fmt.Errorf("bad URL for PromQL endoint: %w", err)
	}

	urlQuery := make(url.Values)

	urlQuery.Add("query", query)
	urlQuery.Add("start", start.Format(time.RFC3339))
	urlQuery.Add("end", end.Format(time.RFC3339))
	urlQuery.Add("step", step.String())

	u.RawQuery = urlQuery.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("fail to prepare request: %w", err)
	}

	req.Header.Add("X-SquirrelDB-Tenant", *tenantHeaderValue) //nolint:canonicalheader

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("query for %s: %w", name, err)
	}

	err = json.NewDecoder(resp.Body).Decode(&response)

	_ = resp.Body.Close()

	if err != nil {
		return fmt.Errorf("decode response for %s: %w", name, err)
	}

	return writeResponse(response, dropLabels, relabels, writer)
}

func writeResponse(response promQLReply, dropLabels []string, relabels labels.Labels, writer *json.Encoder) error {
	if response.Status != "success" {
		return fmt.Errorf(
			"%w: had status %s, want success", ErrUnexpectedResponseType, response.Status,
		)
	}

	if response.Data.ResultType != "matrix" {
		return fmt.Errorf(
			"%w: had result type %s, want matrix", ErrUnexpectedResponseType, response.Data.ResultType,
		)
	}

	for i, metric := range response.Data.Result {
		if len(metric.Values) == 0 {
			return ErrEmptyResponse
		}

		if i >= *maxExample {
			break
		}

		samples := convertToSamples(metric, dropLabels, relabels)

		// we make sure each points are separated by step duration
		lastTime := metric.Values[0].Timestamp
		for i, v := range metric.Values {
			if i > 0 && v.Timestamp-lastTime != int64(step.Seconds()) {
				return fmt.Errorf(
					"%v: %w: got %d seconds, want %d", metric.Metric, ErrUnexpectedDelay, v.Timestamp-lastTime, int64(step.Seconds()),
				)
			}

			lastTime = v.Timestamp
			samples.Samples[i] = v.Value
		}

		err := writer.Encode(samples)
		if err != nil {
			return fmt.Errorf("write samples: %w", err)
		}
	}

	return nil
}

func convertToSamples(metric serieSamples, dropLabels []string, relabels labels.Labels) file.SamplesJSON {
	samples := file.SamplesJSON{}
	samples.Labels = file.WithoutLabels(labels.FromMap(metric.Metric), dropLabels...)
	samples.StepSeconds = int(step.Seconds())
	samples.Samples = make([]string, len(metric.Values))

	if relabels.Len() > 0 {
		builder := labels.NewBuilder(samples.Labels)

		relabels.Range(func(l labels.Label) {
			builder.Set(l.Name, l.Value)
		})

		samples.Labels = builder.Labels()
	}

	return samples
}
