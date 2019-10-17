package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/gocql/gocql"
	"io"
	"squirreldb/compare"
	"squirreldb/types"
	"strings"
)

// Read returns metrics according to the request
func (c *Cassandra) Read(request types.MetricRequest) (types.Metrics, error) {
	aggregated := request.Step >= c.options.AggregateResolution
	var rowSize, partitionSize int64

	if aggregated {
		rowSize = c.options.AggregateSize
		partitionSize = c.options.AggregatePartitionSize
	} else {
		rowSize = c.options.BatchSize
		partitionSize = c.options.RawPartitionSize
	}

	fromBaseTimestamp := request.FromTimestamp - (request.FromTimestamp % partitionSize)
	toBaseTimestamp := request.ToTimestamp - (request.ToTimestamp % partitionSize)

	metrics := make(types.Metrics)

	for baseTimestamp := fromBaseTimestamp; baseTimestamp <= toBaseTimestamp; baseTimestamp += partitionSize {
		fromOffsetTimestamp := compare.Int64Max(request.FromTimestamp-baseTimestamp-rowSize, 0)
		toOffsetTimestamp := compare.Int64Min(request.ToTimestamp-baseTimestamp, partitionSize)

		for _, uuid := range request.UUIDs {
			var iterator *gocql.Iter
			var points types.MetricPoints
			var err error

			if aggregated {
				iterator = c.readDatabase(aggregatedDataTable, gocql.UUID(uuid.UUID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
				points, err = readAggregatedData(iterator, request)
			} else {
				iterator = c.readDatabase(dataTable, gocql.UUID(uuid.UUID), baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
				points, err = readData(iterator, request)
			}

			if err != nil {
				return nil, err
			}

			if err := iterator.Close(); err != nil {
				return nil, err
			}

			metrics[uuid] = append(metrics[uuid], points...)
		}
	}

	return metrics, nil
}

// Returns an iterator from the specified table according to the parameters
func (c *Cassandra) readDatabase(table string, uuid gocql.UUID, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iteratorReplacer := strings.NewReplacer("$TABLE", table)
	iterator := c.session.Query(iteratorReplacer.Replace(`
		SELECT base_ts, offset_ts, values FROM $TABLE
		WHERE metric_uuid = ? AND base_ts = ? AND offset_ts >= ? AND offset_ts <= ?
	`), uuid, baseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}

// Returns metrics
func readData(iterator *gocql.Iter, request types.MetricRequest) (types.MetricPoints, error) {
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	var points types.MetricPoints

	for iterator.Scan(&baseTimestamp, &offsetTimestamp, &values) {
		buffer := bytes.NewReader(values)

	forLoop:
		for {
			var pointData struct {
				Timestamp uint16
				Value     float64
			}

			err := binary.Read(buffer, binary.BigEndian, &pointData)

			switch err {
			case nil:
				timestamp := baseTimestamp + offsetTimestamp + int64(pointData.Timestamp)

				if (timestamp >= request.FromTimestamp) && (timestamp <= request.ToTimestamp) {
					point := types.MetricPoint{
						Timestamp: timestamp,
						Value:     pointData.Value,
					}

					points = append(points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return types.MetricPoints{}, err
			}
		}
	}

	return points, nil
}

// Returns aggregated metrics
func readAggregatedData(iterator *gocql.Iter, request types.MetricRequest) (types.MetricPoints, error) {
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	var points types.MetricPoints

	for iterator.Scan(&baseTimestamp, &offsetTimestamp, &values) {
		buffer := bytes.NewReader(values)

	forLoop:
		for {
			var pointData struct {
				Timestamp uint16
				Min       float64
				Max       float64
				Average   float64
				Count     float64
			}

			err := binary.Read(buffer, binary.BigEndian, &pointData)

			switch err {
			case nil:
				timestamp := baseTimestamp + offsetTimestamp + int64(pointData.Timestamp)

				if (timestamp >= request.FromTimestamp) && (timestamp <= request.ToTimestamp) {
					point := types.MetricPoint{
						Timestamp: timestamp,
					}

					switch request.Function {
					case "min":
						point.Value = pointData.Min
					case "max":
						point.Value = pointData.Max
					case "avg":
						point.Value = pointData.Average
					case "count":
						point.Value = pointData.Count
					default:
						point.Value = pointData.Average
					}

					points = append(points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return types.MetricPoints{}, err
			}
		}
	}

	return points, nil
}
