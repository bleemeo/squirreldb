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
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/rs/zerolog/log"
)

func importData(opts options) error {
	t0 := time.Now()

	importedSeries, importedPoints, firstTS, lastTS, timings, err := importSeries(opts)
	if err != nil {
		return fmt.Errorf("importing series: %w", err)
	}

	log.Info().Msgf(
		"Imported %d serie%s and %d point%s across a time range of %s from parquet in %s (read: %s, send: %s, pre-aggreg: %s)", //nolint:lll
		importedSeries, plural(importedSeries),
		importedPoints, plural(importedPoints),
		formatTimeRange(time.UnixMilli(firstTS), time.UnixMilli(lastTS)),
		time.Since(t0).Round(time.Millisecond).String(),
		timings.read.Round(time.Millisecond).String(),
		timings.send.Round(time.Millisecond).String(),
		timings.preAggreg.Round(time.Millisecond).String(),
	)

	return nil
}

type importTimings struct {
	read, send, preAggreg time.Duration
}

type columnDetail struct {
	index int
	name  string
	lbls  []prompb.Label
}

// importSeries loads each metric series from the specified parquet file,
// and imports it to SquirrelDB.
// It returns the first and last timestamps seen, or any error.
func importSeries(opts options) (
	importedSeries, importedPoints int,
	firstTS, lastTS int64,
	timings importTimings,
	err error,
) {
	f, err := os.Open(opts.inputFile)
	if err != nil {
		return 0, 0, 0, 0, importTimings{}, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// f will be automatically closed at the same time as the parquet reader.

	pr, err := file.NewParquetReader(f)
	if err != nil {
		return 0, 0, 0, 0, importTimings{}, fmt.Errorf("failed to create parquet reader: %w", err)
	}

	defer func() {
		closeErr := pr.Close()
		if closeErr != nil && err == nil { // so as not to override the (probably) responsible error
			err = fmt.Errorf("closing writer: %w", closeErr)
		}
	}()

	cols, err := getColumnsFromSchema(opts, pr)
	if err != nil {
		return 0, 0, 0, 0, importTimings{}, err
	}

	tRead := time.Now()

	minTS, maxTS := opts.start.UnixMilli(), opts.end.UnixMilli()

	timestamps, minRG, maxRG, err := readTimestamps(pr, minTS, maxTS)
	if err != nil {
		return 0, 0, 0, 0, importTimings{}, err
	}

	timings.read += time.Since(tRead)

	log.Info().Msgf("Found %d timestamps matching the given time range", len(timestamps))

	for c, col := range cols {
		log.Debug().Msgf("Importing column %d/%d", c+1, len(cols))

		colImportedPoints, colFirstTS, colLastTS, err := importColumn(opts, pr, col, minTS, maxTS, timestamps, minRG, maxRG, &timings) //nolint:lll
		if err != nil {
			return 0, 0, 0, 0, importTimings{},
				fmt.Errorf("importing column %d: %w", c+1, err)
		}

		importedSeries++
		importedPoints += colImportedPoints

		if firstTS == 0 || colFirstTS < firstTS {
			firstTS = colFirstTS
		}

		if colLastTS > lastTS {
			lastTS = colLastTS
		}
	}

	return importedSeries, importedPoints, firstTS, lastTS, timings, nil
}

func getColumnsFromSchema(opts options, pr *file.Reader) ([]columnDetail, error) {
	sch := pr.MetaData().GetSchema()
	cols := make([]columnDetail, 0, len(sch)-2)

COLUMNS:
	for c, col := range sch[2:] {
		if col.GetType().String() != parquet.Types.Double.String() {
			return nil, fmt.Errorf("unexpected type %q in column %s", col.GetType(), col.GetName())
		}

		colName := col.GetName()

		seriesMatchers, err := parser.ParseMetricSelector("{" + colName + "}")
		if err != nil {
			return nil, fmt.Errorf("bad column name %s: %w", colName, err)
		}

		prompbLabels := make([]prompb.Label, 0, len(seriesMatchers))

		for _, label := range seriesMatchers {
			for _, givenMatcher := range opts.labelMatchers {
				if givenMatcher.Name == label.Name {
					if !givenMatcher.Matches(label.Value) {
						continue COLUMNS
					}
				}
			}

			prompbLabels = append(prompbLabels, prompb.Label{Name: label.Name, Value: label.Value})
		}

		cols = append(cols, columnDetail{
			index: c + 1,
			name:  colName,
			lbls:  prompbLabels,
		})
	}

	return cols, nil
}

// readTimestamps loads the timestamps from all row groups.
// We assume they're in ascending order.
func readTimestamps(pr *file.Reader, minTS int64, maxTS int64) (timestamps []int64, minRG, maxRG int, err error) {
	timestamps = make([]int64, 0, pr.NumRows())
	minRG = -1 // -1 = unset

	for rg := range pr.NumRowGroups() {
		rgr := pr.RowGroup(rg)

		colReader, err := rgr.Column(0) // The first column contains the timestamps
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to create column reader: %w", err)
		}

		rgTimestamps := make([]int64, rgr.NumRows())

		rowsRead, tsRead, err := colReader.(*file.Int64ColumnChunkReader).ReadBatch(rgr.NumRows(), rgTimestamps, nil, nil) //nolint:lll, forcetypeassert
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to read timestamps column: %w", err)
		}

		if rowsRead != int64(tsRead) {
			return nil, 0, 0, fmt.Errorf("unexpected number of timestamps rows read VS values read: %d / %d", rowsRead, tsRead)
		}

		if len(rgTimestamps) == 0 || rgTimestamps[len(rgTimestamps)-1] < minTS {
			continue
		}

		if rgTimestamps[0] > maxTS {
			break
		}

		if minRG < 0 {
			minRG = rg
		}

		maxRG = rg // so far

		maxReached := false

		for i, ts := range rgTimestamps {
			if ts < minTS {
				// We keep timestamps from this row-group even when they're inferior to minTS,
				// so as not to have to handle a shift in the values columns.
				continue
			}

			if ts > maxTS {
				rgTimestamps = rgTimestamps[:i]
				maxReached = true

				break
			}
		}

		timestamps = append(timestamps, rgTimestamps...)

		if maxReached {
			break
		}
	}

	return timestamps, minRG, maxRG, nil
}

func importColumn(opts options,
	pr *file.Reader,
	col columnDetail,
	minTS, maxTS int64,
	timestamps []int64,
	minRG, maxRG int,
	timings *importTimings,
) (importedPoints int, firstTS, lastTS int64, err error) {
	samples := make([]prompb.Sample, 0, len(timestamps)) // Max theoretic length
	tsIndex := 0

	tRead := time.Now()

	for rg := range pr.NumRowGroups() {
		if rg < minRG {
			continue
		}

		if rg > maxRG {
			break
		}

		rgr := pr.RowGroup(rg)

		log.Trace().Msgf("  Loading row-group %d/%d", (rg-minRG)+1, (maxRG-minRG)+1)

		colReader, err := rgr.Column(col.index)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("failed to create column reader: %w", err)
		}

		values := make([]float64, rgr.NumRows())
		defLevels := make([]int16, rgr.NumRows())

		_, pointsRead, err := colReader.(*file.Float64ColumnChunkReader).ReadBatch(rgr.NumRows(), values, defLevels, nil) //nolint:lll, forcetypeassert
		if err != nil {
			return 0, 0, 0, fmt.Errorf("failed to read column values: %w", err)
		}

		currentPointIdx := 0

	TIMESTAMPS:
		for i, ts := range timestamps[tsIndex : tsIndex+len(defLevels)] { // only read the timestamps from this row-group
			switch {
			case ts > maxTS:
				break TIMESTAMPS
			case ts < minTS:
				if defLevels[i] == 1 {
					currentPointIdx++ // effectively skip the value
				}

				fallthrough
			case defLevels[i] == 0:
				continue
			}

			val := values[currentPointIdx]
			if !math.IsNaN(val) {
				samples = append(samples, prompb.Sample{Timestamp: ts, Value: val})
			}

			currentPointIdx++
			if currentPointIdx == pointsRead {
				break
			}
		}

		tsIndex += len(defLevels)
	}

	timings.read += time.Since(tRead)

	if len(samples) == 0 {
		return 0, 0, 0, nil
	}

	importedPoints += len(samples)
	firstTS = samples[0].Timestamp
	lastTS = samples[len(samples)-1].Timestamp

	series := prompb.TimeSeries{
		Labels:  col.lbls,
		Samples: samples,
	}

	tSend := time.Now()

	err = remoteWriteSeries(series, opts)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("remotely writing series from column %s: %w", col.name, err)
	}

	timings.send += time.Since(tSend)

	if opts.preAggregURL != nil {
		log.Trace().Msgf("Pre-aggregating column %s ...", col.name)

		tPreAggreg := time.Now()

		err = triggerPreAggregation(opts.preAggregURL.String(), opts.tenantHeader, firstTS, lastTS, col.lbls)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("triggering pre-aggregation on column %s: %w", col.name, err)
		}

		timings.preAggreg += time.Since(tPreAggreg)
	}

	return importedPoints, firstTS, lastTS, nil
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

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("received non-204 response: %d %s", resp.StatusCode, tryParseErrorBody(resp.Body))
	}

	return nil
}

// triggerPreAggregation will send a pre-aggregation request to SquirrelDB for the given series.
func triggerPreAggregation(preAggregURL, tenant string, from, to int64, importedLabels []prompb.Label) error {
	preAggURL, _ := url.Parse(preAggregURL)
	q := preAggURL.Query()

	q.Set("from", time.UnixMilli(from).Format("2006-01-02"))
	q.Set("to", time.UnixMilli(to).Format("2006-01-02"))

	lbls := make([]string, 0, len(importedLabels))

	for _, label := range importedLabels {
		lbls = append(lbls, label.GetName()+"="+label.GetValue())
	}

	q.Set("labels", strings.Join(lbls, ","))

	preAggURL.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, preAggURL.String(), nil) //nolint: noctx
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if tenant != "" {
		req.Header.Set(types.HeaderTenant, tenant)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-200 response: %d %s", resp.StatusCode, tryParseErrorBody(resp.Body))
	}

	return nil
}
