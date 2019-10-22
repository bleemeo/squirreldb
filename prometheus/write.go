package prometheus

import (
	"github.com/cenkalti/backoff"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io/ioutil"
	"net/http"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

type WritePoints struct {
	indexer types.MetricIndexer
	writer  types.MetricWriter
}

// Serve the write handler
// Decodes the request and transforms it into Metrics
// Sends the Metrics to storage
func (w *WritePoints) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	speedWriteRequest := debug.NewSpeed() // TODO: Speed

	speedWriteRequest.Start()

	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger.Println("WritePoints: Error: Can't read the body (", err, ")")
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)
		return
	}

	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		logger.Println("WritePoints: Error: Can't decode the body (", err, ")")
		http.Error(writer, "Can't decode the HTTP body", http.StatusBadRequest)
		return
	}

	var writeRequest prompb.WriteRequest

	if err := proto.Unmarshal(decoded, &writeRequest); err != nil {
		logger.Println("WritePoints: Error: Can't unmarshal the decoded body (", err, ")")
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)
		return
	}

	metrics := make(types.Metrics)

	for _, series := range writeRequest.Timeseries {
		labels := pbLabelsToLabels(series.Labels)
		uuid := w.indexer.UUID(labels)

		metrics[uuid] = toMetricPoints(series)
	}

	_ = backoff.Retry(func() error {
		err := w.writer.Write(metrics)

		if err != nil {
			logger.Println("WritePoints: Can't write in storage (", err, ")")
		}

		return err
	}, retry.NewBackOff(30*time.Second))

	speedWriteRequest.Stop(1)
	// speedWriteRequest.Print("prometheus", "Process", "WriteRequest")
}

// Convert Prometheus Labels to MetricLabels
func pbLabelsToLabels(pbLabels []*prompb.Label) types.MetricLabels {
	labels := make(types.MetricLabels, 0, len(pbLabels))

	for _, pbLabel := range pbLabels {
		label := types.MetricLabel{
			Name:  pbLabel.Name,
			Value: pbLabel.Value,
		}

		labels = append(labels, label)
	}

	return labels
}

// Generate MetricPoints
func toMetricPoints(series *prompb.TimeSeries) types.MetricPoints {
	var points types.MetricPoints

	for _, sample := range series.Samples {
		point := types.MetricPoint{
			Timestamp: sample.Timestamp / 1000,
			Value:     sample.Value,
		}

		points = append(points, point)
	}

	return points
}
