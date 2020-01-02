package remotestorage

import (
	"github.com/prometheus/prometheus/prompb"

	"net/http"
	"squirreldb/compare"
	"squirreldb/retry"
	"squirreldb/types"
	"strconv"
	"time"
)

type WriteMetrics struct {
	indexer types.Indexer
	writer  types.MetricWriter
}

const (
	timeToLiveLabelName = "__ttl__"
)

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

	metrics := metricsFromTimeseries(writeRequest.Timeseries, func(labels []types.MetricLabel) (int64, types.MetricUUID) {
		timeToLive, err := timeToLiveFromLabels(labels)

		if err != nil {
			logger.Printf("Warning: Can't get time to live from labels (%v), using default", err)
		}

		var uuid types.MetricUUID

		retry.Print(func() error {
			var err error
			uuid, err = w.indexer.UUID(labels)

			return err
		}, retry.NewExponentialBackOff(retryMaxDelay), logger,
			"Error: Can't get UUID from the index",
			"Resolved: Get UUID from the index")

		return timeToLive, uuid
	})

	w.writer.Write(metrics)

	requestsSecondsWrite.Observe(time.Since(start).Seconds())
}

// Returns and delete time to live from a MetricLabel list
func timeToLiveFromLabels(labels []types.MetricLabel) (int64, error) {
	value, exists := types.GetLabelsValue(labels, timeToLiveLabelName)

	var timeToLive int64

	if exists {
		var err error
		timeToLive, err = strconv.ParseInt(value, 10, 64)

		if err != nil {
			return 0, err
		}

		types.DeleteLabelsValue(&labels, timeToLiveLabelName)
	}

	return timeToLive, nil
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
func metricFromPromSeries(promSeries *prompb.TimeSeries, fun func(labels []types.MetricLabel) (int64, types.MetricUUID)) (types.MetricUUID, types.MetricData) {
	labels := labelsFromPromLabels(promSeries.Labels)
	timeToLive, uuid := fun(labels)
	points := pointsFromPromSamples(promSeries.Samples)
	data := types.MetricData{
		Points:     points,
		TimeToLive: timeToLive,
	}

	return uuid, data
}

// Returns a metric list generated from a TimeSeries list
func metricsFromTimeseries(promTimeseries []*prompb.TimeSeries, fun func(labels []types.MetricLabel) (int64, types.MetricUUID)) map[types.MetricUUID]types.MetricData {
	if len(promTimeseries) == 0 {
		return nil
	}

	totalPoints := 0

	metrics := make(map[types.MetricUUID]types.MetricData, len(promTimeseries))

	for _, promSeries := range promTimeseries {
		uuid, data := metricFromPromSeries(promSeries, fun)
		currentData := metrics[uuid]

		currentData.Points = append(currentData.Points, data.Points...)
		currentData.TimeToLive = compare.MaxInt64(currentData.TimeToLive, data.TimeToLive)

		metrics[uuid] = currentData
		totalPoints += len(data.Points)
	}

	requestsPointsTotalWrite.Add(float64(totalPoints))

	return metrics
}
