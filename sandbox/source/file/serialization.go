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

package file

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"

	"github.com/bleemeo/squirreldb/sandbox/dataset"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// SamplesJSON is the format of one entry of a samples file. A samples file is a
// stream of such JSON objects.
type SamplesJSON struct {
	Labels      labels.Labels `json:"labels"`
	StepSeconds int           `json:"step_seconds"`
	Samples     []string      `json:"samples"`
}

// OpenFunc opens a file referenced by a dataset definition.
type OpenFunc func(name string) (io.ReadCloser, error)

// Load reads the metric lists and samples files of the dataset and associates
// each metric with its samples. Metrics without samples are dropped and a
// warning is returned for each of them.
func Load(files dataset.Files, open OpenFunc) (Store, []error, error) {
	result := Store{}

	for _, m := range files.Metrics {
		matchingLabels := append(slices.Clone(m.MatchingLabels), labels.MetricName)
		slices.Sort(matchingLabels)
		matchingLabels = slices.Compact(matchingLabels)

		tmp, err := loadMetricsFile(open, m.File, matchingLabels)
		if err != nil {
			return result, nil, err
		}

		result.Metrics = append(result.Metrics, tmp...)
	}

	for _, sample := range files.Samples {
		tmp, err := loadSamplesFile(open, sample.File)
		if err != nil {
			return result, nil, fmt.Errorf("%s: %w", sample.File, err)
		}

		result.Samples = append(result.Samples, tmp...)
	}

	warnings := result.matchSamples()

	if len(result.Metrics) == 0 {
		return result, warnings, ErrEmptyConfig
	}

	return result, warnings, nil
}

func loadMetricsFile(open OpenFunc, name string, matchingLabels []string) ([]Metric, error) {
	file, err := open(name)
	if err != nil {
		return nil, fmt.Errorf("open metrics list: %w", err)
	}

	defer file.Close()

	result, err := ParseMetrics(file, matchingLabels)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	return result, nil
}

// ParseMetrics reads a list of metrics, one per line in PromQL syntax (empty
// lines and lines starting with # are ignored). It returns a list of Metric with
// just Labels and MatchingLabels set.
// matchingLabels is the value of Metric.MatchingLabels. It should contain __name__ and must be sorted.
func ParseMetrics(r io.Reader, matchingLabels []string) ([]Metric, error) {
	var result []Metric

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}

		lbls, err := parser.ParseMetric(line)
		if err != nil {
			return result, fmt.Errorf("parse metric: %w", err)
		}

		result = append(result, Metric{Labels: lbls, MatchingLabels: matchingLabels})
	}

	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("read metrics list: %w", err)
	}

	return result, nil
}

func loadSamplesFile(open OpenFunc, name string) ([]Samples, error) {
	file, err := open(name)
	if err != nil {
		return nil, fmt.Errorf("fail to open file: %w", err)
	}

	defer file.Close()

	return ParseSamples(file)
}

// ParseSamples decodes a stream of SamplesJSON.
func ParseSamples(r io.Reader) ([]Samples, error) {
	var result []Samples

	decoder := json.NewDecoder(r)

	for {
		var tmp SamplesJSON

		err := decoder.Decode(&tmp)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return result, fmt.Errorf("decode sample json: %w", err)
		}

		if tmp.StepSeconds <= 0 {
			return result, fmt.Errorf("metric %s: %w", tmp.Labels, ErrInvalidStepSeconds)
		}

		if len(tmp.Samples) == 0 {
			return result, fmt.Errorf("metric %s: %w", tmp.Labels, ErrNoSampleValue)
		}

		samples := make([]float64, len(tmp.Samples))

		for i, v := range tmp.Samples {
			samples[i], err = strconv.ParseFloat(v, 64)
			if err != nil {
				return result, fmt.Errorf("bad sample value: %w", err)
			}
		}

		result = append(result, Samples{Labels: tmp.Labels, StepSeconds: tmp.StepSeconds, Samples: samples})
	}

	return result, nil
}
