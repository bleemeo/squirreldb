// Copyright 2015-2024 Bleemeo
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
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/rs/zerolog/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

const readBatchSize = 1000

var errCantSeekTimestamp = errors.New("can't seek timestamp")

func importData(opts options) error {
	log.Debug().Str("tenant", opts.tenantHeader).Stringer("write-url", opts.writeURL).Str("input-file", opts.inputFile).
		Time("start", opts.start).Time("end", opts.end).Any("metric-selector", opts.labelMatchers).
		Any("pre-aggreg-url", opts.preAggregURL.String()).Msg("Import options")

	var m1, m2 runtime.MemStats

	runtime.GC() // TODO: remove
	runtime.ReadMemStats(&m1)

	t0 := time.Now()

	importedSeries, firstTS, lastTS, timePerSeries, err := importSeries(opts)
	if err != nil {
		return fmt.Errorf("importing series: %w", err)
	}

	runtime.ReadMemStats(&m2) // TODO: remove
	log.Warn().Uint64("total", m2.TotalAlloc-m1.TotalAlloc).Uint64("mallocs", m2.Mallocs-m1.Mallocs).Msg("Memory:")

	log.Info().Msgf(
		"Imported %d serie(s) across a time range of %s from parquet in %s (~%s per series)",
		importedSeries,
		formatTimeRange(time.UnixMilli(firstTS), time.UnixMilli(lastTS)),
		time.Since(t0).Round(time.Millisecond).String(),
		timePerSeries.Round(time.Millisecond).String(),
	)

	return nil
}

// importSeries loads each metric series from the specified parquet file,
// and imports it to SquirrelDB.
// It returns the first and last timestamps seen, or any error.
func importSeries(opts options) (importedSeries int, firstTS, lastTS int64, timePerSeries time.Duration, err error) {
	fr, err := local.NewLocalFileReader(opts.inputFile)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("open input file: %w", err)
	}

	defer func() {
		err := fr.Close()
		if err != nil {
			log.Err(err).Msg("Failed to close parquet file")
		}
	}()

	pr, err := reader.NewParquetReader(fr, nil, parallelWorkers)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("can't initialize parquet reader: %w", err)
	}

	defer pr.ReadStop()

	t0 := time.Now()

	labelsPerColName, matchersPerColName := getLabelsFromMetadata(pr.Footer.KeyValueMetadata)
	deniedColumns, importedSeriesPerColName := filterLabels(matchersPerColName, opts.labelMatchers)
	startTS, endTS := opts.start.UnixMilli(), opts.end.UnixMilli()

	wantedTS, startIdx, endIdx, err := readAllTimestamps(pr, startTS, endTS)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	if len(wantedTS) == 0 {
		return 0, 0, 0, 0, errNoSeriesFound
	}

	if startIdx > 0 {
		err = pr.SkipRows(startIdx)
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("%w: %w", errCantSeekTimestamp, err)
		}
	}

	for colIdx, colName := range pr.SchemaHandler.ValueColumns {
		if colIdx == 0 {
			continue // skip timestamps column
		}

		colNameWithoutPrefix := common.StrToPath(colName)[1] // remove schema root prefix
		if deniedColumns[colNameWithoutPrefix] {
			continue
		}

		samples, err := readSeriesSamples(pr, wantedTS, colIdx, colNameWithoutPrefix, endIdx)
		if err != nil {
			return 0, 0, 0, 0, err
		}

		if len(samples) == 0 {
			log.Debug().Msgf("No values found within the given time range in column %q", colNameWithoutPrefix)

			continue
		}

		timeSeries := prompb.TimeSeries{
			Labels:  labelsPerColName[colNameWithoutPrefix],
			Samples: samples,
		}

		err = remoteWriteSeries(timeSeries, opts)
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("remotely writing series from column n°%d: %w", colIdx, err)
		}

		seriesFirstTS := samples[0].Timestamp
		seriesLastTS := samples[len(samples)-1].Timestamp
		seriesLabels := importedSeriesPerColName[colNameWithoutPrefix]

		if opts.preAggregURL != nil {
			err = triggerPreAggregation(opts.preAggregURL.String(), opts.tenantHeader, seriesFirstTS, seriesLastTS, seriesLabels)
			if err != nil {
				return 0, 0, 0, 0, fmt.Errorf("triggering pre-aggregation: %w", err)
			}
		}

		importedSeries++

		if firstTS == 0 || seriesFirstTS < firstTS {
			firstTS = seriesFirstTS
		}

		if lastTS == 0 || seriesLastTS > lastTS {
			lastTS = seriesLastTS
		}
	}

	if importedSeries == 0 {
		return 0, 0, 0, 0, errNoSeriesFound
	}

	timePerSeries = time.Since(t0) / time.Duration(importedSeries)

	return importedSeries, firstTS, lastTS, timePerSeries, nil
}

// readSeriesSamples loads one metric series from the given parquet reader,
// and returns it as a slice of prompb.Sample.
func readSeriesSamples(
	pr *reader.ParquetReader,
	wantedTS []int64,
	colIdx int,
	colName string,
	endIdx int64,
) ([]prompb.Sample, error) {
	samples := make([]prompb.Sample, 0, len(wantedTS))

ROWS:
	for i := 0; i < len(wantedTS); i += readBatchSize {
		values, _, _, err := pr.ReadColumnByIndex(int64(colIdx), readBatchSize)
		if err != nil {
			return nil, fmt.Errorf("can't read column %q: %w", colName, err)
		}

		for j, v := range values {
			idx := int64(i + j)
			if idx > endIdx {
				break ROWS
			}

			value, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("bad data type %T for column %q index %d", v, colName, idx)
			}

			if math.IsNaN(value) {
				continue
			}

			sample := prompb.Sample{
				Value:     value,
				Timestamp: wantedTS[idx],
			}

			samples = append(samples, sample)
		}
	}

	return samples, nil
}

// remoteWriteSeries sends the given metric series to the specified SquirrelDB.
func remoteWriteSeries(series prompb.TimeSeries, opts options) error {
	writeReq := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{series},
	}

	body, err := writeReq.Marshal()
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	compressedBody := snappy.Encode(nil, body)
	bodyReader := bytes.NewReader(compressedBody)

	req, err := http.NewRequest(http.MethodPost, opts.writeURL.String(), bodyReader) //nolint:noctx
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")

	if opts.tenantHeader != "" {
		req.Header.Set(types.HeaderTenant, opts.tenantHeader)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		content, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("received non-200 response: %d\n%s", resp.StatusCode, content)
	}

	return nil
}

// triggerPreAggregation will send a pre-aggregation request to SquirrelDB for the given series.
func triggerPreAggregation(preAggregURL, tenant string, from, to int64, importedLabels map[string]string) error {
	preAggURL, _ := url.Parse(preAggregURL)
	q := preAggURL.Query()

	q.Set("from", time.UnixMilli(from).Format("2006-01-02"))
	q.Set("to", time.UnixMilli(to).Format("2006-01-02"))

	lbls := make([]string, 0, len(importedLabels))

	for label, value := range importedLabels {
		lbls = append(lbls, label+"="+value)
	}

	q.Set("labels", strings.Join(lbls, ","))

	preAggURL.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, preAggURL.String(), nil) //nolint: noctx
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if tenant != "" {
		req.Header.Set("X-SquirrelDB-Tenant", tenant) //nolint:canonicalheader
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		content, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("received non-200 response: %d\n%s", resp.StatusCode, content)
	}

	return nil
}

func readAllTimestamps(
	pr *reader.ParquetReader,
	startTS, endTS int64,
) (
	allTS []int64,
	firstRowIdx, lastRowIdx int64,
	err error,
) {
	allTS = make([]int64, pr.GetNumRows())

ROWS:
	for i := int64(0); i < pr.GetNumRows(); i += readBatchSize {
		values, _, _, err := pr.ReadColumnByIndex(0, readBatchSize) // 0 is the first column
		if err != nil {
			return nil, 0, 0, err
		}

		// TODO: check last value to fast skip the whole slice ?

		for j, e := range values {
			tsIdx := i + int64(j)

			ts, ok := e.(int64)
			if !ok {
				return nil, 0, 0, fmt.Errorf("invalid timestamp value '%v' at row %d", e, tsIdx)
			}

			if ts < startTS {
				continue
			} else if firstRowIdx == 0 {
				firstRowIdx = tsIdx
			}

			if endTS != 0 && ts > endTS {
				break ROWS
			}

			allTS[tsIdx] = ts
			lastRowIdx = tsIdx
		}
	}

	return allTS[firstRowIdx : lastRowIdx+1], firstRowIdx, lastRowIdx, nil
}

func getLabelsFromMetadata(keyValues []*parquet.KeyValue) (map[string][]prompb.Label, map[string][]*labels.Matcher) {
	prompbLabelsPerCol := make(map[string][]prompb.Label, len(keyValues))
	matchersPerColName := make(map[string][]*labels.Matcher)

	for _, kv := range keyValues {
		matchers, err := parser.ParseMetricSelector("{" + *kv.Value + "}")
		if err != nil {
			log.Err(err).Msgf("Failed to decode labels %#v", *kv.Value)

			return nil, nil
		}

		prompbLabels := make([]prompb.Label, 0, len(matchers))

		for _, label := range matchers {
			prompbLabels = append(prompbLabels, prompb.Label{Name: label.Name, Value: label.Value})
		}

		prompbLabelsPerCol[kv.Key] = prompbLabels
		matchersPerColName[kv.Key] = matchers
	}

	return prompbLabelsPerCol, matchersPerColName
}

// filterLabels generates two collections:
// - deniedColumns: a map of parquet columns that don't match the given selector (label matchers)
// - importedSeriesPerColName: a map of parquet columns containing the metrics labels.
func filterLabels(
	metricPerColName map[string][]*labels.Matcher,
	labelsMatchers []*labels.Matcher,
) (deniedColumns map[string]bool, importedSeriesPerColName map[string]map[string]string) {
	deniedColumns = make(map[string]bool)
	importedSeriesPerColName = make(map[string]map[string]string, len(metricPerColName))

COLUMNS:
	for col, metricMatchers := range metricPerColName {
		for _, givenMatcher := range labelsMatchers {
			for _, metricMatcher := range metricMatchers {
				if givenMatcher.Name == metricMatcher.Name {
					if !givenMatcher.Matches(metricMatcher.Value) {
						deniedColumns[col] = true

						continue COLUMNS
					}
				}
			}
		}

		labelsMap := make(map[string]string, len(metricMatchers))

		for _, label := range metricMatchers {
			labelsMap[label.Name] = label.Value
		}

		importedSeriesPerColName[col] = labelsMap
	}

	return deniedColumns, importedSeriesPerColName
}

// decodeRow extract the timestamp and metric values from the given struct.
func decodeRow(row any, deniedColumns map[string]bool) (ts int64, values map[string]float64) {
	rowRef := reflect.ValueOf(row)
	rowType := rowRef.Type()

	values = make(map[string]float64, rowRef.NumField()-1) // -1 for timestamp, which has its own variable

	for i := range rowRef.NumField() {
		switch f := rowRef.Field(i); f.Kind() { //nolint:exhaustive
		case reflect.Int64:
			ts = f.Int()
		case reflect.Float64:
			colName := rowType.Field(i).Name
			if deniedColumns[colName] {
				continue
			}

			value := f.Float()
			if math.IsNaN(value) {
				continue
			}

			values[colName] = value
		default:
			log.Warn().Msgf("Unexpected type %s in column %q", f.Kind(), rowType.Field(i).Name)
		}
	}

	return ts, values
}
