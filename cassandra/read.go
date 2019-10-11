package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/gocql/gocql"
	"io"
	"squirreldb/config"
	"squirreldb/types"
)

// Read returns metrics according to the request
func (c *Cassandra) Read(request types.MetricRequest) (types.Metrics, error) {
	var fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64

	aggregateStep := config.C.Int64("cassandra.aggregate.step")
	aggregated := request.Step >= aggregateStep

	if aggregated {
		aggregateSize := config.C.Int64("cassandra.aggregate.size")
		partitionSize := config.C.Int64("cassandra.partition_size.aggregated")
		fromBaseTimestamp = request.FromTimestamp - (request.FromTimestamp % partitionSize)
		toBaseTimestamp = request.ToTimestamp - (request.ToTimestamp % partitionSize)
		fromOffsetTimestamp = request.FromTimestamp - fromBaseTimestamp - aggregateSize
		toOffsetTimestamp = request.ToTimestamp - toBaseTimestamp
	} else {
		batchSize := config.C.Int64("batch.size")
		partitionSize := config.C.Int64("cassandra.partition_size.raw")
		fromBaseTimestamp = request.FromTimestamp - (request.FromTimestamp % partitionSize)
		toBaseTimestamp = request.ToTimestamp - (request.ToTimestamp % partitionSize)
		fromOffsetTimestamp = request.FromTimestamp - fromBaseTimestamp - batchSize
		toOffsetTimestamp = request.ToTimestamp - toBaseTimestamp
	}

	var iterator *gocql.Iter
	metrics := make(types.Metrics)

	for _, uuid := range request.UUIDs {
		var err error

		if aggregated {
			iterator = c.readSQL(aggregatedDataTable, gocql.UUID(uuid.UUID), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
			metrics[uuid], err = readAggregatedData(iterator, request)
		} else {
			iterator = c.readSQL(dataTable, gocql.UUID(uuid.UUID), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
			metrics[uuid], err = readData(iterator, request)
		}

		if err != nil {
			return nil, err
		}
	}

	return metrics, nil
}

// Returns an iterator from the specified table according to the parameters
func (c *Cassandra) readSQL(table string, uuid gocql.UUID, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iterator := c.session.Query(
		"SELECT base_ts, offset_ts, values FROM "+table+" "+
			"WHERE metric_uuid = ? AND base_ts IN (?, ?) AND offset_ts >= ? AND offset_ts <= ?",
		uuid, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

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
