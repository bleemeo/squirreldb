package prometheus

import (
	"github.com/prometheus/prometheus/prompb"
	"net/http"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

type WriteMetrics struct {
	indexer types.MetricIndexer
	writer  types.MetricWriter
}

// ServeHTTP handles writing requests
func (w *WriteMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()

	var writeRequest prompb.WriteRequest

	err := decodeRequest(request, &writeRequest)

	if err != nil {
		logger.Printf("Error: Can't decode the write request (%v)", err)
		http.Error(writer, "Can't decode the write request", http.StatusBadRequest)

		return
	}

	metrics := metricsFromTimeseries(writeRequest.Timeseries, w.indexer.UUID)

	retry.Print(func() error {
		return w.writer.Write(metrics)
	}, retry.NewExponentialBackOff(30*time.Second), logger,
		"Error: Can't write metrics with the writer",
		"Resolved: Read metrics with the writer")

	requestSecondsWrite.Observe(time.Since(start).Seconds())
}

// Returns a MetricLabel list generated from a Label list
func labelsFromPromLabels(promLabels []*prompb.Label) []types.MetricLabel {
	if len(promLabels) == 0 {
		return nil
	}

	labels := make([]types.MetricLabel, 0, len(promLabels))

	for _, promLabel := range promLabels {
		label := types.MetricLabel{
			Name:  promLabel.Name,
			Value: promLabel.Value,
		}

		labels = append(labels, label)
	}

	return labels
}

// Returns a MetricPoint list generated from a Sample list
func pointsFromPromSamples(promSamples []prompb.Sample) []types.MetricPoint {
	if len(promSamples) == 0 {
		return nil
	}

	points := make([]types.MetricPoint, 0, len(promSamples))

	for _, promSample := range promSamples {
		point := types.MetricPoint{
			Timestamp: promSample.Timestamp / 1000,
			Value:     promSample.Value,
		}

		points = append(points, point)
	}

	return points
}

// Returns a MetricUUID and a MetricData generated from a TimeSeries
func metricFromPromSeries(promSeries *prompb.TimeSeries, fun func(labels []types.MetricLabel) types.MetricUUID) (types.MetricUUID, types.MetricData) {
	labels := labelsFromPromLabels(promSeries.Labels)
	uuid := fun(labels)
	timeToLive := int64(0)
	points := pointsFromPromSamples(promSeries.Samples)
	data := types.MetricData{
		Points:     points,
		TimeToLive: timeToLive,
	}

	return uuid, data
}

// Returns a metric list generated from a TimeSeries list
func metricsFromTimeseries(promTimeseries []*prompb.TimeSeries, fun func(labels []types.MetricLabel) types.MetricUUID) map[types.MetricUUID]types.MetricData {
	if len(promTimeseries) == 0 {
		return nil
	}

	metrics := make(map[types.MetricUUID]types.MetricData, len(promTimeseries))

	for _, promSeries := range promTimeseries {
		uuid, data := metricFromPromSeries(promSeries, fun)

		metrics[uuid] = data
	}

	return metrics
}
