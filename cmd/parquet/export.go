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
	"io"
	"math"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

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

func exportData(opts options) error {
	log.Debug().Str("tenant", opts.tenantHeader).Stringer("read-url", opts.readURL).Str("output-file", opts.outputFile).
		Time("start", opts.start).Time("end", opts.end).Any("labels", opts.labelPairs).Msg("Export options")

	var m1, m2 runtime.MemStats

	runtime.GC() // TODO: remove
	runtime.ReadMemStats(&m1)

	tRead := time.Now()

	series, err := fetchSeries(opts)
	if err != nil {
		return fmt.Errorf("fetch series: %w", err)
	}

	if len(series) == 0 {
		return errNoSeriesFound
	}

	tWrite := time.Now()

	err = writeToParquet(opts.outputFile, series)
	if err != nil {
		return fmt.Errorf("write to parquet file: %w", err)
	}

	runtime.ReadMemStats(&m2) // TODO: remove
	log.Warn().Uint64("total", m2.TotalAlloc-m1.TotalAlloc).Uint64("mallocs", m2.Mallocs-m1.Mallocs).Msg("Memory:")

	log.Info().Msgf(
		"Exported %d serie(s) across a time range of %s to parquet in %s (fetch: %s, write: %s)",
		len(series),
		formatTimeRange(opts.start, opts.end),
		time.Since(tRead).Round(100*time.Millisecond).String(),
		tWrite.Sub(tRead).Round(time.Millisecond).String(),
		time.Since(tWrite).Round(time.Millisecond).String(),
	)

	return nil
}

// fetchSeries retrieves the metric series matching the given options from a SquirrelDB.
func fetchSeries(opts options) ([]*prompb.TimeSeries, error) {
	query := &prompb.Query{
		StartTimestampMs: opts.start.UnixMilli(),
		EndTimestampMs:   opts.end.UnixMilli(),
		Matchers:         make([]*prompb.LabelMatcher, 0, len(opts.labelPairs)),
	}

	for name, value := range opts.labelPairs {
		query.Matchers = append(query.Matchers, &prompb.LabelMatcher{
			Type:  prompb.LabelMatcher_EQ,
			Name:  name,
			Value: value,
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
	req.Header.Set("X-SquirrelDB-Tenant", opts.tenantHeader) //nolint:canonicalheader
	req.Header.Set("X-SquirrelDB-Max-Evaluated-Points", "0") //nolint:canonicalheader

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return nil, fmt.Errorf("received non-200 response: %d\n%s", resp.StatusCode, body)
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

// writeToParquet dumps the given metric series to the specified output.
// (outputFile="-" means stdout).
func writeToParquet(outputFile string, series []*prompb.TimeSeries) error {
	fw, err := makeParquetFile(outputFile)
	if err != nil {
		return fmt.Errorf("can't initialize parquet file: %w", err)
	}

	defer func() {
		err := fw.Close()
		if err != nil {
			log.Err(err).Msg("Failed to close parquet file")
		}
	}()

	schema := generateParquetSchema(series)

	pw, err := writer.NewParquetWriter(fw, schema, parallelWorkers)
	if err != nil {
		return fmt.Errorf("can't initialize parquet writer: %w", err)
	}

	defer func() {
		err := pw.WriteStop()
		if err != nil {
			log.Err(err).Msg("Failed to close parquet writer")
		}
	}()

	err = writeTimeSeries(pw, series)
	if err != nil {
		return fmt.Errorf("writing timeseries to parquet: %w", err)
	}

	return nil
}

func writeTimeSeries(pw *writer.ParquetWriter, series []*prompb.TimeSeries) error {
	timestamps := make(map[int64]struct{})
	indexBySeries := make(map[int]int)
	metricColNames := make([]string, len(series))

	for si, s := range series {
		indexBySeries[si] = 0
		metricColNames[si] = common.StringToVariableName(labelsToText(s.GetLabels()))

		for _, sample := range s.Samples {
			timestamps[sample.Timestamp] = struct{}{}
		}
	}

	tss := maps.Keys(timestamps)

	slices.Sort(tss)

	const dayMs = 86_400_000 // 24h in ms

	currentDay := tss[0] / dayMs

	for _, ts := range tss {
		tsDay := ts / dayMs
		if tsDay != currentDay {
			// Flushing at the end of each day, to create one row-group per day.
			err := pw.Flush(true)
			if err != nil {
				return fmt.Errorf("flushing writer: %w", err)
			}

			currentDay = tsDay
		}

		row := make(map[string]any, 1+len(series))
		row["Timestamp"] = ts

	SERIES:
		for si, s := range series {
			for i, sample := range s.Samples[indexBySeries[si]:] {
				if sample.Timestamp == ts {
					row[metricColNames[si]] = sample.Value
					indexBySeries[si] = i + 1

					continue SERIES
				}
			}

			row[metricColNames[si]] = math.NaN()
		}

		if err := pw.Write(row); err != nil {
			return fmt.Errorf("writing row at ts=%d: %w", ts, err)
		}
	}

	varNameToLabels := make([]*parquet.KeyValue, len(metricColNames))

	for col, name := range metricColNames {
		labels := labelsToText(series[col].GetLabels())
		varNameToLabels[col] = &parquet.KeyValue{
			Key:   name,
			Value: &labels,
		}
	}

	pw.Footer.KeyValueMetadata = varNameToLabels

	return nil
}

func makeParquetFile(file string) (source.ParquetFile, error) {
	if file == "-" {
		return writerfile.NewWriterFile(os.Stdout), nil
	}

	return local.NewLocalFileWriter(file)
}

// generateParquetSchema drafts schema elements according to the given metric series.
func generateParquetSchema(series []*prompb.TimeSeries) []*parquet.SchemaElement {
	totalColumns := int32(1 + len(series))                  // +1 for the timestamps column
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

	for i, col := range series {
		elem := parquet.NewSchemaElement()
		elem.Name = labelsToText(col.GetLabels())
		elem.Type = parquet.TypePtr(parquet.Type_DOUBLE)
		// Values for all timestamps are required; metrics that don't have one will get a NaN instead.
		elem.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)

		elems[2+i] = elem // +2 for schema root & timestamps col
	}

	return elems
}

func labelsToText(labels []prompb.Label) string {
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
