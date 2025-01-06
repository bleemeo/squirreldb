// Copyright 2015-2025 Bleemeo
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

// Command used to get information on sstable data content, like number of points.
// From a folder than contains sstables files:
// docker run --rm -ti --log-driver none \
//    -v $(pwd):/srv/squirreldb/data:ro -w /srv/squirreldb/data --name sstabletools cassandra:latest bash
//
// Build the command and copy the command to the container:
// GOOS=linux go build ./cmd/sstable-data-content
// docker cp sstable-data-content sstabletools:/tmp
//
// Then from inside the container:
// ls -l md-288820-big-Data.db
// /opt/cassandra/tools/bin/sstabledump -l md-288820-big-Data.db | /tmp/sstable-data-content
//
// To export as CSV, from outside the container:
// docker exec sstabletools sh -c '/opt/cassandra/tools/bin/sstabledump -l md-288820-big-Data.db | \
// 		/tmp/sstable-data-content -csv' | gzip > POSSIBLY_BIG.csv.gz

package main

import (
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/logger"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = logger.NewTestLogger(false)
	writeCSV := flag.Bool("csv", false, "output CSV with all values to stdout")
	skipHeader := flag.Bool("no-header", false, "don't print CSV header")

	flag.Parse()

	err := run(*writeCSV, !*skipHeader)
	if err != nil {
		log.Err(err).Msg("Failed to import data")
	}
}

// sstableDumpLine is the json-line from  sstabledump -l for an squirreldb.data table.
type sstableDumpLine struct {
	Partition sstableDumpPartition `json:"partition"`
	Rows      []sstableDumpRow     `json:"rows"`
}

type sstableDumpPartition struct {
	Key      sstableDumpKey `json:"key"`
	Position int            `json:"position"`
}

type sstableDumpKey struct {
	MetricID int64 `json:"-"`
	BaseTS   int64 `json:"-"`
}

func (f *sstableDumpKey) UnmarshalJSON(b []byte) error {
	work := make([]string, 0, 2)

	if err := json.Unmarshal(b, &work); err != nil {
		return err
	}

	if len(work) != 2 {
		return fmt.Errorf("wrong number of element for sstableDumpRowCluster, had %d want 2", len(work))
	}

	var err error

	f.MetricID, err = strconv.ParseInt(work[0], 10, 64)
	if err != nil {
		return err
	}

	f.BaseTS, err = strconv.ParseInt(work[1], 10, 64)
	if err != nil {
		return err
	}

	return nil
}

type sstableDumpRow struct {
	Type         string                     `json:"type"`
	Position     int                        `json:"position"`
	Clustering   sstableDumpRowCluster      `json:"clustering"`
	LivenessInfo sstableDumpRowLivenessInfo `json:"liveness_info"`
	Cells        []sstableDumpRowCell       `json:"cells"`
}

type sstableDumpRowCluster struct {
	offsetMS   int
	insertTime string // it's an UUID
}

func (f *sstableDumpRowCluster) UnmarshalJSON(b []byte) error {
	work := make([]interface{}, 0, 2)

	if err := json.Unmarshal(b, &work); err != nil {
		return err
	}

	if len(work) != 2 {
		return fmt.Errorf("wrong number of element for sstableDumpRowCluster, had %d want 2", len(work))
	}

	offset, ok := work[0].(float64)
	if !ok {
		return fmt.Errorf("first element of sstableDumpRowCluster must be an float64, had %T (%v)", work[0], work[0])
	}

	insertTime, ok := work[1].(string)
	if !ok {
		return fmt.Errorf("second element of sstableDumpRowCluster must be an string, had %T (%v)", work[1], work[1])
	}

	f.offsetMS = int(offset)
	f.insertTime = insertTime

	return nil
}

type sstableDumpRowLivenessInfo struct {
	Timestamp string `json:"tstamp"`
	TTL       int    `json:"ttl"`
	ExpireAt  string `json:"expires_at"`
	Expired   bool   `json:"expired"`
}

type sstableDumpRowCell struct {
	Name         string                         `json:"name"`
	Value        string                         `json:"value"`
	DeletionInfo sstableDumpRowCellDeletionInfo `json:"deletion_info"`
}

type sstableDumpRowCellDeletionInfo struct {
	LocalDeleteTime string `json:"local_delete_time"`
}

func run(writeCSV bool, writeHeader bool) error {
	decoder := json.NewDecoder(os.Stdin)

	var line sstableDumpLine

	linesCount := 0
	rowsCount := 0
	cellsCount := 0
	tombstonesCount := 0
	pointsPoint := 0
	uniqueMetric := make(map[int64]bool)

	var writer *csv.Writer

	if writeCSV {
		writer = csv.NewWriter(os.Stdout)

		if writeHeader {
			if err := writer.Write([]string{
				"metricID",
				"timestamp",
				"value",
			}); err != nil {
				return err
			}
		}
	}

	var (
		minTS time.Time
		maxTS time.Time
	)

	xorChunkPool := chunkenc.NewPool()

	for {
		err := decoder.Decode(&line)

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		linesCount++

		uniqueMetric[line.Partition.Key.MetricID] = true

		for _, row := range line.Rows {
			rowsCount++

			for _, cell := range row.Cells {
				cellsCount++

				if cell.Name != "values" {
					return fmt.Errorf("line %d, unexpected cell %#v", linesCount, cell)
				}

				if len(cell.Value) == 0 && len(cell.DeletionInfo.LocalDeleteTime) > 0 {
					tombstonesCount++

					continue
				}

				if len(cell.Value) <= 2 {
					return fmt.Errorf("line %d, cell values too short: %#v", linesCount, cell)
				}

				values, err := hex.DecodeString(cell.Value[2:])
				if err != nil {
					return err
				}

				if values[0] != 0 {
					return fmt.Errorf("line %d, unsupported data version: %v", linesCount, values[0])
				}

				chunk, err := xorChunkPool.Get(chunkenc.EncXOR, values[1:])
				if err != nil {
					return err
				}

				it := chunk.Iterator(nil)
				for it.Next() != chunkenc.ValNone {
					pointTS := time.UnixMilli(it.AtT())
					if maxTS.IsZero() || pointTS.After(maxTS) {
						maxTS = pointTS
					}

					if minTS.IsZero() || pointTS.Before(minTS) {
						minTS = pointTS
					}

					pointsPoint++

					if writer != nil {
						ts, value := it.At()

						if err := writer.Write([]string{
							strconv.FormatInt(line.Partition.Key.MetricID, 10),
							fmt.Sprintf("%.3f", float64(ts)/1000.),
							fmt.Sprint(value),
						}); err != nil {
							return err
						}
					}
				}

				if err := xorChunkPool.Put(chunk); err != nil {
					return err
				}
			}
		}
	}

	lines := []string{
		fmt.Sprintf(
			"read %d lines %d rows, %d cells, %d tombstones, %d points",
			linesCount,
			rowsCount,
			cellsCount,
			tombstonesCount,
			pointsPoint,
		),
		fmt.Sprintf("See %d unique metrics", len(uniqueMetric)),
		fmt.Sprintf("minTS=%s, maxTS=%s, delta=%v", minTS, maxTS, humainTime(maxTS.Sub(minTS))),
	}

	fmt.Fprintln(os.Stderr, strings.Join(lines, "\n"))

	if writer != nil {
		writer.Flush()

		if err := writer.Error(); err != nil {
			return err
		}
	}

	return nil
}

func humainTime(duration time.Duration) string {
	duration = duration.Round(time.Hour)

	daysCount := int64(duration / (24 * time.Hour))

	duration -= time.Duration(daysCount) * (24 * time.Hour)

	return fmt.Sprintf("%d days, %.0f hours", daysCount, duration.Hours())
}
