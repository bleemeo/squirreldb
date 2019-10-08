package cassandra

import (
	"bytes"
	"encoding/binary"
	"github.com/cenkalti/backoff"
	"github.com/gocql/gocql"
	"io"
	"squirreldb/config"
	"squirreldb/types"
	"time"
)

// Returns the list of points meeting the conditions of the request
func (c *Cassandra) Read(mRequest types.MetricRequest) ([]types.MetricPoints, error) {
	var fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64

	_, aggregated := mRequest.Labels["__aggregated__"]

	if aggregated {
		aggregateSize := config.C.Int64("aggregate.size")
		partitionSize := config.C.Int64("cassandra.partition_size.aggregated")
		fromBaseTimestamp = mRequest.FromTime.Unix() - (mRequest.FromTime.Unix() % partitionSize)
		toBaseTimestamp = mRequest.ToTime.Unix() - (mRequest.ToTime.Unix() % partitionSize)
		fromOffsetTimestamp = mRequest.FromTime.Unix() - fromBaseTimestamp - aggregateSize
		toOffsetTimestamp = mRequest.ToTime.Unix() - toBaseTimestamp
	} else {
		batchSize := config.C.Int64("batch.size")
		partitionSize := config.C.Int64("cassandra.partition_size.classic")
		fromBaseTimestamp = mRequest.FromTime.Unix() - (mRequest.FromTime.Unix() % partitionSize)
		toBaseTimestamp = mRequest.ToTime.Unix() - (mRequest.ToTime.Unix() % partitionSize)
		fromOffsetTimestamp = mRequest.FromTime.Unix() - fromBaseTimestamp - batchSize
		toOffsetTimestamp = mRequest.ToTime.Unix() - toBaseTimestamp
	}

	var iterator *gocql.Iter
	var mUUID types.MetricUUID
	var results map[string]types.MetricPoints
	var err error

	_ = backoff.Retry(func() error {
		var err error
		mUUID, err = mRequest.UUID()

		if err != nil {
			logger.Println("Read: Can't generate UUID (", err, ")")
		}

		return err
	}, exponentialBackOff)

	if aggregated {
		iterator = c.readSQL(aggregatedDataTable, gocql.UUID(mUUID.UUID), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		results, err = readAggregatedPointData(mRequest, iterator)
	} else {
		iterator = c.readSQL(dataTable, gocql.UUID(mUUID.UUID), fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp)
		results, err = readPointData(mRequest, iterator)
	}

	if err != nil {
		return nil, err
	}

	if err := iterator.Close(); err != nil {
		return nil, err
	}

	var msPoints []types.MetricPoints

	for _, mPoint := range results {
		msPoints = append(msPoints, mPoint)
	}

	return msPoints, nil
}

// readPointData
func readPointData(mRequest types.MetricRequest, iterator *gocql.Iter) (map[string]types.MetricPoints, error) {
	var metricUUID string
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	results := make(map[string]types.MetricPoints)

	for iterator.Scan(&metricUUID, &baseTimestamp, &offsetTimestamp, &values) {
		item, exists := results[metricUUID]

		if !exists {
			item = types.MetricPoints{
				Metric: mRequest.Metric,
			}
		}

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
				pointTime := time.Unix(baseTimestamp+offsetTimestamp+int64(pointData.Timestamp), 0)

				if (mRequest.Step == 0 || ((int64(pointData.Timestamp) % mRequest.Step) == 0)) &&
					!pointTime.Before(mRequest.FromTime) && !pointTime.After(mRequest.ToTime) {

					point := types.Point{
						Time:  pointTime,
						Value: pointData.Value,
					}

					item.Points = append(item.Points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return nil, err
			}
		}

		results[metricUUID] = item
	}

	return results, nil
}

// readAggregatedPointData
func readAggregatedPointData(mRequest types.MetricRequest, iterator *gocql.Iter) (map[string]types.MetricPoints, error) {
	var metricUUID string
	var baseTimestamp, offsetTimestamp int64
	var values []byte
	results := make(map[string]types.MetricPoints)

	for iterator.Scan(&metricUUID, &baseTimestamp, &offsetTimestamp, &values) {
		item, exists := results[metricUUID]

		if !exists {
			item = types.MetricPoints{
				Metric: mRequest.Metric,
			}
		}

		buffer := bytes.NewReader(values)

	forLoop:
		for {
			var aggregatedPointData struct {
				Timestamp uint16
				Min       float64
				Max       float64
				Average   float64
				Count     uint16
			}

			err := binary.Read(buffer, binary.BigEndian, &aggregatedPointData)

			switch err {
			case nil:
				pointTime := time.Unix(baseTimestamp+offsetTimestamp+int64(aggregatedPointData.Timestamp), 0)

				if (mRequest.Step == 0 || ((int64(aggregatedPointData.Timestamp) % mRequest.Step) == 0)) &&
					!pointTime.Before(mRequest.FromTime) && !pointTime.After(mRequest.ToTime) {

					point := types.Point{
						Time: pointTime,
					}

					switch mRequest.Labels["__aggregated_function__"] {
					case "min":
						point.Value = aggregatedPointData.Min
					case "max":
						point.Value = aggregatedPointData.Max
					case "average":
						point.Value = aggregatedPointData.Average
					case "count":
						point.Value = float64(aggregatedPointData.Count)
					default:
						point.Value = aggregatedPointData.Average
					}

					item.Points = append(item.Points, point)
				}
			case io.EOF:
				break forLoop
			default:
				return nil, err
			}
		}

		results[metricUUID] = item
	}

	return results, nil
}

// Returns an iterator from the specified table according to the parameters
func (c *Cassandra) readSQL(table string, uuid gocql.UUID, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp int64) *gocql.Iter {
	iterator := c.session.Query(
		"SELECT metric_uuid, base_ts, offset_ts, values FROM "+table+" "+
			"WHERE metric_uuid = ? AND base_ts IN (?, ?) AND offset_ts >= ? AND offset_ts <= ?",
		uuid, fromBaseTimestamp, toBaseTimestamp, fromOffsetTimestamp, toOffsetTimestamp).Iter()

	return iterator
}
