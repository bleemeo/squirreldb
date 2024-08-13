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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"runtime"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/rs/zerolog/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"golang.org/x/exp/maps"
)

const readBatchSize = 100

var errCantSeekTimestamp = errors.New("can't seek timestamp")

func importData(opts options) error {
	log.Debug().Str("tenant", opts.tenantHeader).Stringer("write-url", opts.writeURL).Str("input-file", opts.inputFile).
		Time("start", opts.start).Time("end", opts.end).Any("labels", opts.labelPairs).Msg("Import options")

	var m1, m2 runtime.MemStats

	runtime.GC() // TODO: remove
	runtime.ReadMemStats(&m1)

	tRead := time.Now()

	series, firstTS, lastTS, err := readSeries(opts)
	if err != nil {
		return fmt.Errorf("reading series: %w", err)
	}

	if len(series) == 0 {
		return errNoSeriesFound
	}

	tWrite := time.Now()

	err = remoteWriteSeries(series, opts)
	if err != nil {
		return fmt.Errorf("remotly writing series: %w", err)
	}

	runtime.ReadMemStats(&m2) // TODO: remove
	log.Warn().Uint64("total", m2.TotalAlloc-m1.TotalAlloc).Uint64("mallocs", m2.Mallocs-m1.Mallocs).Msg("Memory:")

	log.Info().Msgf(
		"Imported %d serie(s) across a time range of %s from parquet in %s (read: %s, write: %s)",
		len(series),
		formatTimeRange(time.UnixMilli(firstTS), time.UnixMilli(lastTS)),
		time.Since(tRead).Round(100*time.Millisecond).String(),
		tWrite.Sub(tRead).Round(time.Millisecond).String(),
		time.Since(tWrite).Round(time.Millisecond).String(),
	)

	return nil
}

// readSeries loads the metric series from the specified parquet file.
// It returns the series, the first and last timestamps seen, or any error.
func readSeries(opts options) (series []prompb.TimeSeries, firstTS, lastTS int64, err error) {
	fr, err := local.NewLocalFileReader(opts.inputFile)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("open input file: %w", err)
	}

	defer func() {
		err := fr.Close()
		if err != nil {
			log.Err(err).Msg("Failed to close parquet file")
		}
	}()

	pr, err := reader.NewParquetReader(fr, nil, parallelWorkers)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("can't initialize parquet reader: %w", err)
	}

	defer pr.ReadStop()

	labelsPerColName, matchersPerColName := getLabelsFromMetadata(pr.Footer.KeyValueMetadata)
	blacklistedColumns := makeLabelsBlacklist(matchersPerColName, opts.labelPairs)
	seriesByLabels := make(map[string]prompb.TimeSeries, len(labelsPerColName))

	startTS, endTS := opts.start.UnixMilli(), opts.end.UnixMilli()
	if startTS != 0 {
		err = seekToTimestamp(pr, startTS)
		if err != nil {
			return nil, 0, 0, err
		}
	}

READ:
	for {
		rows, err := pr.ReadByNumber(readBatchSize)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("can't read rows: %w", err)
		}

		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			ts, values := decodeRow(row, blacklistedColumns)
			if startTS != 0 && ts < startTS {
				continue
			}

			if endTS != 0 && ts > endTS {
				break READ
			}

			for col, value := range values {
				s, ok := seriesByLabels[col]
				if !ok {
					s = prompb.TimeSeries{Labels: labelsPerColName[col]}
				}

				s.Samples = append(s.Samples, prompb.Sample{Timestamp: ts, Value: value})
				seriesByLabels[col] = s
			}

			if firstTS == 0 {
				firstTS = ts
			}

			lastTS = ts
		}
	}

	return maps.Values(seriesByLabels), firstTS, lastTS, nil
}

// remoteWriteSeries sends the given metric series to the specified SquirrelDB.
func remoteWriteSeries(series []prompb.TimeSeries, opts options) error {
	writeReq := prompb.WriteRequest{
		Timeseries: series,
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
	req.Header.Set("X-SquirrelDB-Tenant", opts.tenantHeader) //nolint:canonicalheader

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

// seekToTimestamp positions the reader cursor to the day containing the specified timestamp,
// or returns an error if the timestamp couldn't be found in the parquet file.
func seekToTimestamp(pr *reader.ParquetReader, targetTimestamp int64) error {
	var rowsToSkip int64

	for _, rg := range pr.Footer.RowGroups {
		stats := rg.Columns[0].MetaData.Statistics // Assuming the first column contains the timestamps

		if !stats.IsSetMaxValue() {
			return fmt.Errorf("%w: row-group max value is not set", errCantSeekTimestamp)
		}

		maxTimestamp := binary.LittleEndian.Uint64(stats.MaxValue)
		if int64(maxTimestamp) >= targetTimestamp {
			// The timestamp we look for is in this row-group (day); skipping up to it.
			return pr.SkipRows(rowsToSkip)
		}

		rowsToSkip += rg.NumRows
	}

	return fmt.Errorf("%w: no timestamp satisfaying specified start time found", errCantSeekTimestamp)
}

func getLabelsFromMetadata(keyValues []*parquet.KeyValue) (map[string][]prompb.Label, map[string][]*labels.Matcher) {
	prompbLabels := make(map[string][]prompb.Label, len(keyValues))
	labelsMatchers := make(map[string][]*labels.Matcher)

	for _, kv := range keyValues {
		matchers, err := parser.ParseMetricSelector("{" + *kv.Value + "}")
		if err != nil {
			log.Err(err).Msgf("Failed to decode labels %#v", *kv.Value)

			return nil, nil
		}

		prompbLbls := make([]prompb.Label, 0, len(matchers))

		for _, label := range matchers {
			prompbLbls = append(prompbLbls, prompb.Label{Name: label.Name, Value: label.Value})
		}

		prompbLabels[kv.Key] = prompbLbls
		labelsMatchers[kv.Key] = matchers
	}

	return prompbLabels, labelsMatchers
}

func makeLabelsBlacklist(matchersPerColName map[string][]*labels.Matcher, labels map[string]string) map[string]bool {
	if len(labels) == 0 {
		return map[string]bool{}
	}

	blacklist := make(map[string]bool)

COLUMNS:
	for col, matchers := range matchersPerColName {
		for _, matcher := range matchers {
			if value, found := labels[matcher.Name]; found {
				if !matcher.Matches(value) {
					blacklist[col] = true

					continue COLUMNS
				}
			}
		}
	}

	return blacklist
}

// decodeRow extract the timestamp and metric values from the given struct.
func decodeRow(row any, blacklistedColumns map[string]bool) (ts int64, values map[string]float64) {
	rowRef := reflect.ValueOf(row)
	rowType := rowRef.Type()

	values = make(map[string]float64, rowRef.NumField()-1)

	for i := range rowRef.NumField() {
		switch f := rowRef.Field(i); f.Kind() { //nolint:exhaustive
		case reflect.Int64:
			ts = f.Int()
		case reflect.Float64:
			colName := rowType.Field(i).Name
			if blacklistedColumns[colName] {
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
