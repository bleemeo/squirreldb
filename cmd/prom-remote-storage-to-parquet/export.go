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
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

type batchTimings struct {
	fetch, write time.Duration
}

func exportData(opts options) error {
	tFind := time.Now()

	seriesLabels, err := findSeries(opts)
	if err != nil {
		return fmt.Errorf("finding series: %w", err)
	}

	if len(seriesLabels) == 0 {
		return errNoSeriesFound
	}

	log.Debug().Msgf("Found %d serie%s in %s", len(seriesLabels), plural(len(seriesLabels)), time.Since(tFind).Round(time.Millisecond)) //nolint:lll

	tExport := time.Now()

	totalSeries, totalRows, totalPoints, timings, err := exportSeries(opts, seriesLabels)
	if err != nil {
		return fmt.Errorf("exporting: %w", err)
	}

	log.Info().Msgf(
		"Exported %d serie%s totaling %d row%s and %d point%s across a time range of %s to parquet in %s (find: %s, export: %s (fetch: %s, write: %s))", //nolint:lll
		totalSeries, plural(totalSeries),
		totalRows, plural(totalRows),
		totalPoints, plural(totalPoints),
		formatTimeRange(opts.start, opts.end),
		time.Since(tFind).Round(time.Millisecond).String(),
		tExport.Sub(tFind).Round(time.Millisecond).String(),
		time.Since(tExport).Round(time.Millisecond).String(),
		timings.fetch.Round(time.Millisecond),
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
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(types.HeaderMaxEvaluatedSeries, strconv.FormatUint(uint64(opts.squirrelDBMaxEvalSeries), 10))
	req.Header.Set(types.HeaderMaxEvaluatedPoints, strconv.FormatUint(opts.squirrelDBMaxEvalPoints, 10))

	if opts.tenantHeader != "" {
		req.Header.Set(types.HeaderTenant, opts.tenantHeader)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-200 response: %d %s", resp.StatusCode, tryParseErrorBody(resp.Body))
	}

	var foundSeries struct {
		Data []map[string]string `json:"data"`
	}

	return foundSeries.Data, json.NewDecoder(resp.Body).Decode(&foundSeries)
}

func exportSeries(opts options, seriesLabels []map[string]string) (
	totalSeries int,
	totalRows, totalPoints int64,
	timings batchTimings,
	err error,
) {
	seriesNames, pw, err := makeParquetWriter(opts.outputFile, seriesLabels, opts.exportCompressionLevel)
	if err != nil {
		return 0, 0, 0, batchTimings{}, err
	}

	defer func() {
		closeErr := pw.Close()
		if closeErr != nil && err == nil { // so as not to override the (probably) responsible error
			err = fmt.Errorf("closing writer: %w", closeErr)
		}
	}()

	batchSizeMs := opts.exportBatchSize.Milliseconds()
	actualSeries := make(map[string]struct{}, len(seriesNames))

	startTS, endTS := opts.start.UnixMilli(), opts.end.UnixMilli()
	for batchStartTS := startTS; batchStartTS < endTS; batchStartTS += batchSizeMs {
		batchEndTS := min(batchStartTS+batchSizeMs, endTS)

		tStart, tEnd := time.UnixMilli(batchStartTS), time.UnixMilli(batchEndTS)
		log.Debug().Msgf("Fetching series from %s to %s (%s)", tStart, tEnd, formatTimeRange(tStart, tEnd))

		tFetch := time.Now()

		series, err := fetchSeries(opts, batchStartTS, batchEndTS)
		if err != nil {
			return 0, 0, 0, batchTimings{},
				fmt.Errorf("fetching series [%s -> %s]: %w", time.UnixMilli(batchStartTS), time.UnixMilli(batchEndTS), err)
		}

		timings.fetch += time.Since(tFetch)

		if len(series) == 0 {
			continue
		}

		samplesBySeriesName := make(map[string][]prompb.Sample, len(series))

		for _, s := range series {
			labelsText := labelsTextFromSlice(s.Labels)
			samplesBySeriesName[labelsText] = s.Samples
			actualSeries[labelsText] = struct{}{}
		}

		rowsCount, pointsCount, err := writeTimeSeries(seriesNames, pw.AppendRowGroup(), samplesBySeriesName, &timings)
		if err != nil {
			return 0, 0, 0, batchTimings{}, fmt.Errorf("writing series: %w", err)
		}

		totalRows += int64(rowsCount)
		totalPoints += int64(pointsCount)
	}

	return len(actualSeries), totalRows, totalPoints, timings, nil
}

func makeParquetWriter(outputFile string, seriesLabels []map[string]string, compressionLevel zstd.EncoderLevel) (
	allSeries []string,
	pw *file.Writer,
	err error,
) {
	allSeries = make([]string, len(seriesLabels))

	fields := make(schema.FieldList, 1+len(seriesLabels))
	fields[0] = schema.MustPrimitive(schema.NewPrimitiveNodeLogical(
		"timestamp",
		parquet.Repetitions.Required,
		schema.NewTimestampLogicalType(true, schema.TimeUnitMillis),
		parquet.Types.Int64,
		0,
		0,
	))

	for s, labels := range seriesLabels {
		col := 1 + s // +1 for timestamps col
		labelsText := labelsTextFromMap(labels)
		allSeries[s] = labelsText
		fields[col] = schema.NewFloat64Node(labelsText, parquet.Repetitions.Optional, int32(col))
	}

	schemaDef, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("making parquet schema: %w", err)
	}

	f, err := os.Create(outputFile)
	if err != nil {
		return nil, nil, err
	}

	// f will be automatically closed when closing the parquet writer.

	pw = file.NewParquetWriter(f, schemaDef, file.WithWriterProps(
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(int(compressionLevel)),
		),
	))

	return allSeries, pw, nil
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
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	compressedBody := snappy.Encode(nil, serializedRequest)
	bodyReader := bytes.NewReader(compressedBody)

	req, err := http.NewRequest(http.MethodPost, opts.readURL.String(), bodyReader) //nolint: noctx
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
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
		return nil, fmt.Errorf("sending request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-200 response: %d %s", resp.StatusCode, tryParseErrorBody(resp.Body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	uncompressedBody, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("decompressing response: %w", err)
	}

	readResponse := new(prompb.ReadResponse)
	if err = proto.Unmarshal(uncompressedBody, protoadapt.MessageV2Of(readResponse)); err != nil {
		return nil, fmt.Errorf("unmarshaling response: %w", err)
	}

	results := readResponse.GetResults()
	if len(results) != 1 {
		return nil, fmt.Errorf("expected 1 result, got %d", len(results))
	}

	return results[0].GetTimeseries(), nil
}

func writeTimeSeries(
	allSeries []string,
	rgw file.SerialRowGroupWriter,
	seriesByName map[string][]prompb.Sample,
	timings *batchTimings,
) (rowsCount, pointsCount int, err error) {
	timestamps := make(map[int64]struct{})

	for _, samples := range seriesByName {
		for _, sample := range samples {
			timestamps[sample.Timestamp] = struct{}{}
		}
	}

	tss := maps.Keys(timestamps)

	slices.Sort(tss)

	tWrite := time.Now()

	// Writing the timestamps
	timestampWriter, err := rgw.NextColumn()
	if err != nil {
		return 0, 0, fmt.Errorf("getting timestamps column writer: %w", err)
	}

	_, err = timestampWriter.(*file.Int64ColumnChunkWriter).WriteBatch(tss, nil, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("writing timestamp: %w", err)
	}

	err = timestampWriter.Close()
	if err != nil {
		return 0, 0, fmt.Errorf("closing timestamp writer: %w", err)
	}

	timings.write += time.Since(tWrite)

	// Writing each series
	for _, seriesName := range allSeries {
		values := make([]float64, len(tss))
		defLevels := make([]int16, len(tss))

		// All columns must be defined; series absent from this batch will be filled with null (definition level = 0).
		if series, found := seriesByName[seriesName]; found {
			currentSampleIdx := 0

			for t, ts := range tss {
				sample := series[currentSampleIdx]
				if sample.Timestamp == ts {
					values[t] = sample.Value
					defLevels[t] = 1 // 1 = defined

					currentSampleIdx++
					if currentSampleIdx >= len(series) {
						break
					}
				}
			}
		}

		tWrite = time.Now()

		valueWriter, err := rgw.NextColumn()
		if err != nil {
			return 0, 0, fmt.Errorf("getting next column writer: %w", err)
		}

		written, err := valueWriter.(*file.Float64ColumnChunkWriter).WriteBatch(values, defLevels, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("writing values: %w", err)
		}

		err = valueWriter.Close()
		if err != nil {
			return 0, 0, fmt.Errorf("closing values writer: %w", err)
		}

		timings.write += time.Since(tWrite)
		pointsCount += int(written)
	}

	tWrite = time.Now()

	err = rgw.Close()
	if err != nil {
		return 0, 0, fmt.Errorf("closing row group writer: %w", err)
	}

	timings.write += time.Since(tWrite)

	return len(tss), pointsCount, nil
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
