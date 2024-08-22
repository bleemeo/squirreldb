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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

const seriesFetchBatchSizeDays = 1

type batchTimings struct {
	fetch, merge, write time.Duration
}

func exportData(opts options) error {
	log.Debug().Str("tenant", opts.tenantHeader).Stringer("read-url", opts.readURL).Str("output-file", opts.outputFile).
		Time("start", opts.start).Time("end", opts.end).Any("metric-selector", opts.labelMatchers).Msg("Export options")

	var m1, m2 runtime.MemStats

	runtime.GC() // TODO: remove
	runtime.ReadMemStats(&m1)

	tFind := time.Now()

	seriesLabels, err := findSeries(opts)
	if err != nil {
		return fmt.Errorf("find series: %w", err)
	}

	if len(seriesLabels) == 0 {
		return errNoSeriesFound
	}

	log.Debug().Msgf("Found %d serie(s) in %s", len(seriesLabels), time.Since(tFind).Round(time.Millisecond))

	tExport := time.Now()

	totalRows, totalPoints, timings, err := exportSeries(opts, seriesLabels)
	if err != nil {
		return fmt.Errorf("write to parquet file: %w", err)
	}

	runtime.ReadMemStats(&m2) // TODO: remove
	log.Warn().Uint64("total", m2.TotalAlloc-m1.TotalAlloc).Uint64("mallocs", m2.Mallocs-m1.Mallocs).Msg("Memory:")

	log.Info().Msgf(
		"Exported %d serie(s) totaling %d row(s) and %d point(s) across a time range of %s to parquet in %s (find: %s, export: %s (fetch: %s, merge: %s, write: %s))", //nolint:lll
		len(seriesLabels), totalRows, totalPoints,
		formatTimeRange(opts.start, opts.end),
		time.Since(tFind).Round(time.Millisecond).String(),
		tExport.Sub(tFind).Round(time.Millisecond).String(),
		time.Since(tExport).Round(time.Millisecond).String(),
		timings.fetch.Round(time.Millisecond),
		timings.merge.Round(time.Millisecond),
		timings.write.Round(time.Millisecond),
	)

	return nil
}

func findSeries(opts options) ([]map[string]string, error) {
	seriesURL, err := opts.readURL.Parse("/api/v1/series")
	if err != nil {
		return nil, err
	}

	params := url.Values{
		"match[]": {opts.metricSelector},
		"start":   {opts.start.Format(time.RFC3339)},
		"end":     {opts.end.Format(time.RFC3339)},
	}

	req, err := http.NewRequest(http.MethodPost, seriesURL.String(), strings.NewReader(params.Encode())) //nolint: noctx
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(types.HeaderMaxEvaluatedSeries, strconv.FormatUint(uint64(opts.squirrelDBMaxEvalSeries), 10))
	req.Header.Set(types.HeaderMaxEvaluatedPoints, strconv.FormatUint(opts.squirrelDBMaxEvalPoints, 10))

	if opts.tenantHeader != "" {
		req.Header.Set(types.HeaderTenant, opts.tenantHeader)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return nil, fmt.Errorf("received non-200 response: %d\n%s", resp.StatusCode, body)
	}

	var foundSeries struct {
		Data []map[string]string `json:"data"`
	}

	return foundSeries.Data, json.NewDecoder(resp.Body).Decode(&foundSeries)
}

func exportSeries(opts options, seriesLabels []map[string]string) (
	totalRows, totalPoints int64,
	timings batchTimings,
	err error,
) {
	const batchSizeMs = seriesFetchBatchSizeDays * 24 * 60 * 60 * 1000

	fw, err := makeParquetFile(opts.outputFile)
	if err != nil {
		return 0, 0, batchTimings{}, fmt.Errorf("can't initialize parquet file: %w", err)
	}

	defer func() {
		err := fw.Close()
		if err != nil {
			log.Err(err).Msg("Failed to close parquet file")
		}
	}()

	schema, colNames := generateParquetSchema(seriesLabels)

	pw, err := writer.NewParquetWriter(fw, schema, parallelWorkers)
	if err != nil {
		return 0, 0, batchTimings{}, fmt.Errorf("can't initialize parquet writer: %w", err)
	}

	defer func() {
		err := pw.WriteStop()
		if err != nil {
			log.Err(err).Msg("Failed to close parquet writer")
		}
	}()

	/*for i := range pw.SchemaHandler.Infos {
		pw.SchemaHandler.Infos[i].Encoding = parquet.Encoding_PLAIN
	}*/

	startTS, endTS := opts.start.UnixMilli(), opts.end.UnixMilli()
	for batchStartTS := startTS; batchStartTS < endTS; batchStartTS += batchSizeMs {
		batchEndTS := min(batchStartTS+batchSizeMs, endTS)

		tStart, tEnd := time.UnixMilli(batchStartTS), time.UnixMilli(batchEndTS)
		log.Debug().Msgf("Fetching series from %s to %s (%s)", tStart, tEnd, formatTimeRange(tStart, tEnd))

		tFetch := time.Now()

		series, err := fetchSeries(opts, batchStartTS, batchEndTS)
		if err != nil {
			return 0, 0, batchTimings{},
				fmt.Errorf("fetch series [%s -> %s]: %w", time.UnixMilli(batchStartTS), time.UnixMilli(batchEndTS), err)
		}

		timings.fetch += time.Since(tFetch)

		if len(series) == 0 {
			continue
		}

		rowsCount, pointsCount, err := writeTimeSeries(pw, colNames, series, &timings)
		if err != nil {
			return 0, 0, batchTimings{}, fmt.Errorf("write series: %w", err)
		}

		totalRows += int64(rowsCount)
		totalPoints += int64(pointsCount)
	}

	// TODO: re-add metadata
	/*varNameToLabels := make([]*parquet.KeyValue, len(colNames))

	for col, name := range metricColNames {
		labels := labelsTextFromSlice(series[col].GetLabels())
		varNameToLabels[col] = &parquet.KeyValue{
			Key:   name,
			Value: &labels,
		}
	}

	pw.Footer.KeyValueMetadata = varNameToLabels*/

	return totalRows, totalPoints, timings, nil
}

// fetchSeries retrieves the metric series matching the given options within the given time range from a SquirrelDB.
func fetchSeries(opts options, batchStartTS, batchEndTS int64) ([]*prompb.TimeSeries, error) {
	query := &prompb.Query{
		StartTimestampMs: batchStartTS,
		EndTimestampMs:   batchEndTS,
		Matchers:         make([]*prompb.LabelMatcher, 0, len(opts.labelMatchers)),
	}

	for _, matcher := range opts.labelMatchers {
		query.Matchers = append(query.Matchers, &prompb.LabelMatcher{
			Type:  prompb.LabelMatcher_Type(matcher.Type),
			Name:  matcher.Name,
			Value: matcher.Value,
		})
	}

	readRequest := &prompb.ReadRequest{
		Queries:               []*prompb.Query{query},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
	}

	serializedRequest, err := readRequest.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	compressedBody := snappy.Encode(nil, serializedRequest)
	bodyReader := bytes.NewReader(compressedBody)

	req, err := http.NewRequest(http.MethodPost, opts.readURL.String(), bodyReader) //nolint: noctx
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set(types.HeaderMaxEvaluatedSeries, strconv.FormatUint(uint64(opts.squirrelDBMaxEvalSeries), 10))
	req.Header.Set(types.HeaderMaxEvaluatedPoints, strconv.FormatUint(opts.squirrelDBMaxEvalPoints, 10))

	if opts.tenantHeader != "" {
		req.Header.Set(types.HeaderTenant, opts.tenantHeader)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return nil, fmt.Errorf("received non-200 response: %d %s", resp.StatusCode, body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	uncompressedBody, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("decompress response: %w", err)
	}

	readResponse := new(prompb.ReadResponse)
	if err = proto.Unmarshal(uncompressedBody, protoadapt.MessageV2Of(readResponse)); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	results := readResponse.GetResults()
	if len(results) != 1 {
		return nil, fmt.Errorf("expected 1 result, got %d", len(results))
	}

	return results[0].GetTimeseries(), nil
}

func writeTimeSeries(
	pw *writer.ParquetWriter,
	colNames map[string]struct{},
	series []*prompb.TimeSeries,
	timings *batchTimings,
) (rows, points int, err error) {
	timestamps := make(map[int64]struct{}, len(series[0].Samples)) //nolint: lll // using the length of the first series as an order of size
	indexBySeries := make(map[int]int, len(series))
	metricColNames := make([]string, len(series))
	seriesNotRepresented := maps.Clone(colNames)

	tMerge := time.Now()

	for si, s := range series {
		indexBySeries[si] = 0
		colName := common.StringToVariableName(labelsTextFromSlice(s.GetLabels()))
		metricColNames[si] = colName
		// This metric series appears in this batch, so it won't need to have its value (NaN) put by hand.
		delete(seriesNotRepresented, colName)

		for _, sample := range s.Samples {
			timestamps[sample.Timestamp] = struct{}{}
		}
	}

	tss := maps.Keys(timestamps)

	slices.Sort(tss)

	timings.merge += time.Since(tMerge)

	for _, ts := range tss {
		tMerge = time.Now()

		row := make(map[string]any, 1+len(series)) // +1 for timestamp
		row["Timestamp"] = ts

		for si, s := range series {
			seriesIdx := indexBySeries[si]
			colName := metricColNames[si]

			if seriesIdx >= len(s.Samples) {
				row[colName] = math.NaN()

				continue
			}

			if sample := s.Samples[seriesIdx]; sample.Timestamp == ts {
				row[colName] = sample.Value
				indexBySeries[si]++
				points++
			} else {
				row[colName] = math.NaN()
			}
		}

		// For metric series that are absent from this batch,
		// we need to manually give them a value for each timestamp.
		for metric := range seriesNotRepresented {
			row[metric] = /*(*float64)(nil)*/ math.NaN()
		}

		timings.merge += time.Since(tMerge)

		tWrite := time.Now()

		if err := pw.Write(row); err != nil {
			return 0, 0, fmt.Errorf("writing row at ts=%d: %w", ts, err)
		}

		timings.write += time.Since(tWrite)
	}

	tWrite := time.Now()

	err = pw.Flush(true)
	if err != nil {
		return 0, 0, fmt.Errorf("flushing writer: %w", err)
	}

	timings.write += time.Since(tWrite)

	return len(tss), points, nil
}

func makeParquetFile(file string) (source.ParquetFile, error) {
	if file == "-" {
		return writerfile.NewWriterFile(os.Stdout), nil
	}

	return local.NewLocalFileWriter(file)
}

// generateParquetSchema drafts schema elements according to the given metric series.
// It also returns a map with the keys representing all the metric series columns.
func generateParquetSchema(seriesLabels []map[string]string) ([]*parquet.SchemaElement, map[string]struct{}) {
	// TODO: generate a struct instead of a schema (*float64 & encodings)
	totalColumns := int32(1 + len(seriesLabels))            // +1 for the timestamps column
	elems := make([]*parquet.SchemaElement, 1+totalColumns) // +1 for the schema root
	elems[0] = &parquet.SchemaElement{
		Name:           "root",
		NumChildren:    &totalColumns,
		RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
	}
	elems[1] = &parquet.SchemaElement{
		Name:           "timestamp",
		Type:           parquet.TypePtr(parquet.Type_INT64),
		RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED),
		ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
	}

	colNames := make(map[string]struct{}, totalColumns-1) // -1 for the timestamps column

	for i, labels := range seriesLabels {
		elem := parquet.NewSchemaElement()
		elem.Name = labelsTextFromMap(labels)
		elem.Type = parquet.TypePtr(parquet.Type_DOUBLE)
		// Values for all timestamps are required; metrics that don't have one will get a NaN instead.
		elem.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)

		elems[2+i] = elem // +2 for schema root & timestamps col
		colNames[common.StringToVariableName(elem.Name)] = struct{}{}
	}

	return elems, colNames
}

func labelsTextFromSlice(labels []prompb.Label) string {
	if len(labels) == 0 {
		return ""
	}

	sortedLabels := slices.Clone(labels) // Avoid mutating the corresponding series

	slices.SortFunc(sortedLabels, func(a, b prompb.Label) int {
		return strings.Compare(a.Name, b.Name)
	})

	quoteReplacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	strLabels := make([]string, 0, len(sortedLabels))

	for _, pair := range sortedLabels {
		if pair.GetValue() == "" {
			continue
		}

		str := pair.GetName() + "=\"" + quoteReplacer.Replace(pair.GetValue()) + "\""
		strLabels = append(strLabels, str)
	}

	return strings.Join(strLabels, ",")
}

func labelsTextFromMap(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	keys := maps.Keys(labels)

	slices.Sort(keys)

	quoteReplacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	strLabels := make([]string, 0, len(labels))

	for _, key := range keys {
		value := labels[key]
		if value == "" {
			continue
		}

		str := key + "=\"" + quoteReplacer.Replace(value) + "\""
		strLabels = append(strLabels, str)
	}

	return strings.Join(strLabels, ",")
}
