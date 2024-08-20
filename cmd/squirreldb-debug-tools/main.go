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

//nolint:forbidigo
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/tsdb"
	"github.com/gocql/gocql"
)

func quotedToBinary(s string) ([]byte, error) {
	buffer := bytes.NewReader([]byte(s))
	result := make([]byte, 0, len(s)/2)

	for {
		b, err := buffer.ReadByte()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		if b == '\\' { //nolint:nestif
			escapeChar, err := buffer.ReadByte()
			if err != nil {
				return nil, err
			}

			switch escapeChar {
			case 't':
				result = append(result, '\t')
			case 'r':
				result = append(result, '\r')
			case 'n':
				result = append(result, '\n')
			case '"':
				result = append(result, '"')
			case 'x':
				digit1, err := buffer.ReadByte()
				if err != nil {
					return nil, err
				}

				digit2, err := buffer.ReadByte()
				if err != nil {
					return nil, err
				}

				v, err := strconv.ParseInt(string([]byte{digit1, digit2}), 16, 0)
				if err != nil {
					return nil, err
				}

				result = append(result, byte(v))
			default:
				return nil, fmt.Errorf("unknown escape char, got %q", escapeChar)
			}
		} else if b != '"' {
			result = append(result, b)
		}
	}

	return result, nil
}

func main() {
	idToRedisKey := flag.Int("idToRedis", 0, "convert a Metric ID to redis keys")
	queryCassandraID := flag.Int("queryCassandraId", 0, "query Cassandra raw data (see -cassandraBaseTime)")
	cassandraBaseTime := flag.String("cassandraBaseTime", time.Now().Format(time.DateOnly), "Query around given time, rounder to 5 days partition (format 2006-01-02)") //nolint:lll
	redisData := flag.Bool(
		"redisData",
		false,
		"Used to decode the result of 'redis-cli get squirreldb-metric-???' to known the content",
	)
	cassandraData := flag.Bool(
		"cassandraData",
		false,
		"Used to decode the result of 'cqlsh ...' to known the content",
	)

	flag.Parse()

	if *idToRedisKey != 0 {
		idStr := strconv.FormatInt(int64(*idToRedisKey), 36)

		fmt.Println("The following Redis key should exists for this metric ID:")
		fmt.Printf("redis-cli get squirreldb-metric-%s | go run ./cmd/squirreldb_debug_tools/ -redisData\n", idStr)

		return
	}

	if *queryCassandraID != 0 {
		const rawPartitionSize = 5 * 24 * time.Hour

		baseTime, err := time.Parse(time.DateOnly, *cassandraBaseTime)
		if err != nil {
			fmt.Printf("can't parse date: %v\n", err)

			return
		}

		baseTS := baseTime.UnixMilli() - (baseTime.UnixMilli() % rawPartitionSize.Milliseconds())

		fmt.Println("Run the following:")
		fmt.Printf("cqlsh -e 'SELECT base_ts, offset_ms, insert_time, values FROM squirreldb.data WHERE metric_id = %d AND base_ts = %d' | go run ./cmd/squirreldb_debug_tools/ -cassandraData\n", *queryCassandraID, baseTS) //nolint:lll

		return
	}

	if *redisData {
		stdinScanner := bufio.NewScanner(os.Stdin)
		stdinScanner.Scan()

		if err := stdinScanner.Err(); err != nil {
			fmt.Println("unabled to read a line")

			return
		}

		redisData := stdinScanner.Text()

		type serializedPoints struct {
			Timestamp  int64
			Value      float64
			TimeToLive int64
		}

		const serializedSize = 24

		binDate, err := quotedToBinary(redisData)
		if err != nil {
			fmt.Printf("unable to decode quoted string: %v\n", err)

			return
		}

		buffer := bytes.NewReader(binDate)
		pointCount := len(binDate) / serializedSize
		dataSerialized := make([]serializedPoints, pointCount)

		if err := binary.Read(buffer, binary.BigEndian, &dataSerialized); err != nil {
			fmt.Printf("unable to decode deserialize points: %v", err)

			return
		}

		for _, point := range dataSerialized {
			fmt.Printf(" * %s %f (ttl=%d)\n", time.UnixMilli(point.Timestamp), point.Value, point.TimeToLive)
		}

		return
	}

	if *cassandraData { //nolint:nestif
		stdinScanner := bufio.NewScanner(os.Stdin)
		for stdinScanner.Scan() {
			line := stdinScanner.Text()

			if !strings.Contains(line, "|") {
				continue
			}

			if strings.Contains(line, "base_ts") {
				continue
			}

			if strings.Contains(line, "---") {
				continue
			}

			part := strings.Split(line, "|")

			if len(part) != 4 {
				fmt.Printf("expected 4 part in line, got %v\n", part)

				return
			}

			baseTS, err := strconv.ParseInt(strings.TrimSpace(part[0]), 10, 0)
			if err != nil {
				fmt.Printf("unable to parse int: %v\n", err)

				return
			}

			offsetMs, err := strconv.ParseInt(strings.TrimSpace(part[1]), 10, 0)
			if err != nil {
				fmt.Printf("unable to parse int: %v\n", err)

				return
			}

			insertTimesUUID, err := gocql.ParseUUID(strings.TrimSpace(part[2]))
			if err != nil {
				fmt.Printf("unable to parse timeuuid: %v\n", err)

				return
			}

			values, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(part[3]), "0x"))
			if err != nil {
				fmt.Printf("unable to parse hex string: %v\n", err)

				return
			}

			points, err := tsdb.InternalDecodePoints(values)
			if err != nil {
				fmt.Printf("unable to parse hex string: %v\n", err)

				return
			}

			baseTime := time.UnixMilli(baseTS)
			offsetTime := time.UnixMilli(baseTS + offsetMs)

			fmt.Printf(
				"-- Row with base time = %s, offset time = %s and insert time = %s\n",
				baseTime.Format(time.RFC3339),
				offsetTime.Format(time.RFC3339),
				insertTimesUUID.Time().Format(time.RFC3339),
			)

			for _, pts := range points {
				fmt.Printf("  * %s %v\n", time.UnixMilli(pts.Timestamp).Format(time.RFC3339Nano), pts.Value)
			}
		}

		if err := stdinScanner.Err(); err != nil && !errors.Is(err, io.EOF) {
			fmt.Println("unabled to read a line")

			return
		}

		return
	}

	flag.Usage()
	fmt.Println("")
	fmt.Println("Get a metric ID, likely using curl http://localhost:9201/debug/index_dump_by_labels -G -d query='cpu_used{instance=\"1234\"}'") //nolint:lll
	fmt.Println("The run this debug script with -idToRedis or -queryCassandraId")
}
