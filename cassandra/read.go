package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/gocql/gocql"
	"io"
	"squirreldb/types"
)

func (c *Cassandra) Read(request types.MetricRequest) (map[types.MetricUUID]types.MetricData, error) {
	var fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64

	aggregated := false

	if aggregated {
		aggregateSize := int64(86400)
		partitionSize := int64(6912000)
		fromBaseTimestamp = request.FromTimestamp - (request.FromTimestamp % partitionSize)
		toBaseTimestamp = request.ToTimestamp - (request.ToTimestamp % partitionSize)
		fromOffsetTimestamp = request.FromTimestamp - fromBaseTimestamp - aggregateSize
		toOffsetTimestamp = request.ToTimestamp - toBaseTimestamp
	} else {
		batchSize := int64(300)
		partitionSize := int64(432000)
		fromBaseTimestamp = request.FromTimestamp - (request.FromTimestamp % partitionSize)
		toBaseTimestamp = request.ToTimestamp - (request.ToTimestamp % partitionSize)
		fromOffsetTimestamp = request.FromTimestamp - fromBaseTimestamp - batchSize
		toOffsetTimestamp = request.ToTimestamp - toBaseTimestamp
	}

	var iterator *gocql.Iter
	metrics := make(map[types.MetricUUID]types.MetricData)

	for _, uuid := range request.UUIDs {
		var err error

		if aggregated {
			iterator = c.readSQL(aggregatedDataTable, gocql.UUID(uuid.UUID), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		} else {
			iterator = c.readSQL(dataTable, gocql.UUID(uuid.UUID), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
			metrics[uuid], err = readData(request, iterator)
		}

		if err != nil {
			return nil, err
		}
	}

	return metrics, nil
}

func (c *Cassandra) readSQL(table string, uuid gocql.UUID, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iterator := c.session.Query(
		"SELECT base_ts, offset_ts, values FROM "+table+" "+
			"WHERE metric_uuid = ? AND base_ts IN (?, ?) AND offset_ts >= ? AND offset_ts <= ?",
		uuid, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}

func readData(request types.MetricRequest, iterator *gocql.Iter) (types.MetricData, error) {
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	var data types.MetricData

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

					data.Points = append(data.Points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return types.MetricData{}, err
			}
		}
	}

	return data, nil
}
