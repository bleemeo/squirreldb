package prometheus

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io/ioutil"
	"net/http"
	"squirreldb/retry"
	"squirreldb/types"
	"strconv"
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
	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger.Printf("Error: Can't read the body (%v)", err)
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)

		return
	}

	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		logger.Printf("Error: Can't decode the body (%v)", err)
		http.Error(writer, "Can't decode the HTTP body", http.StatusBadRequest)

		return
	}

	var writeRequest prompb.WriteRequest

	if err := proto.Unmarshal(decoded, &writeRequest); err != nil {
		logger.Printf("Error: Can't unmarshal the decoded body (%v)", err)
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)

		return
	}

	metrics := make(types.Metrics)

	for _, series := range writeRequest.Timeseries {
		labels := pbLabelsToLabels(series.Labels)
		timeToLive := timeToLive(labels)
		uuid := w.indexer.UUID(labels)

		metrics[uuid] = toMetricData(series, timeToLive)
	}

	retry.Print(func() error {
		return w.writer.Write(metrics)
	}, retry.NewBackOff(30*time.Second), logger,
		"Error: Can't write in storage",
		"Resolved: Write in storage")
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

func timeToLive(labels types.MetricLabels) int64 {
	timeToLiveString, exists := labels.Value("__bleemeo_ttl__")
	timeToLive := int64(0)

	if exists {
		timeToLive, _ = strconv.ParseInt(timeToLiveString, 10, 64)
	}

	return timeToLive
}

// Generate MetricPoints
func toMetricData(series *prompb.TimeSeries, timeToLive int64) types.MetricData {
	points := types.MetricPoints{}

	for _, sample := range series.Samples {
		point := types.MetricPoint{
			Timestamp: sample.Timestamp / 1000,
			Value:     sample.Value,
		}

		points = append(points, point)
	}

	metricData := types.MetricData{
		Points:     points,
		TimeToLive: timeToLive,
	}

	return metricData
}
