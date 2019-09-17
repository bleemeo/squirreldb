package cassandra

import (
	"fmt"
	"hamsterdb/types"
)

func (cassandra *Cassandra) Write(msPoints []types.MetricPoints) error {
	for _, mPoints := range msPoints {
		labels := mPoints.CanonicalLabels()
		var timestamp int64
		var values string
		var valuesByte []byte

		// Search the smallest timestamp
		for i, point := range mPoints.Points {
			if i == 0 {
				timestamp = point.Time.Unix()
			} else if pointTimestamp := point.Time.Unix(); pointTimestamp < timestamp {
				timestamp = pointTimestamp
			}
		}

		// Canonicalize points
		for i, point := range mPoints.Points {
			if i == 0 {
				values = fmt.Sprintf("%d=%f", point.Time.Unix()-timestamp, point.Value)
			} else {
				values = fmt.Sprintf("%s,%d=%f", values, point.Time.Unix()-timestamp, point.Value)
			}
		}

		valuesByte = []byte(values)

		// Insert new entry in metrics table
		insertQuery := cassandra.session.Query(
			"INSERT INTO "+metricsTable+" (labels, timestamp, values)"+
				"VALUES (?, ?, ?)",
			labels, timestamp, valuesByte)

		if err := insertQuery.Exec(); err != nil {
			return err
		}
	}

	return nil
}
