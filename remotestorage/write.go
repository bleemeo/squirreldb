package remotestorage

import (
	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"

	"net/http"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

type WriteMetrics struct {
	index  types.Index
	writer types.MetricWriter
}

// ServeHTTP handles writing requests
func (w *WriteMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()

	defer func() {
		requestsSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	var writeRequest prompb.WriteRequest

	err := decodeRequest(request, &writeRequest)

	if err != nil {
		logger.Printf("Error: Can't decode the write request (%v)", err)
		http.Error(writer, "Can't decode the write request", http.StatusBadRequest)
		requestsErrorWrite.Inc()

		return
	}

	metrics, err := metricsFromTimeseries(writeRequest.Timeseries, w.index)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		requestsErrorWrite.Inc()

		return
	}

	if err := w.writer.Write(metrics); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		requestsErrorWrite.Inc()

		return
	}
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

// Returns a UUID and a MetricData generated from a TimeSeries
func metricFromPromSeries(promSeries *prompb.TimeSeries, index types.Index) (gouuid.UUID, types.MetricData, error) {
	labels := labelsFromPromLabels(promSeries.Labels)

	uuid, timeToLive, err := index.LookupUUID(labels)
	if err != nil {
		return uuid, types.MetricData{}, err
	}

	points := pointsFromPromSamples(promSeries.Samples)
	data := types.MetricData{
		Points:     points,
		TimeToLive: timeToLive,
	}

	return uuid, data, nil
}

// Returns a metric list generated from a TimeSeries list
func metricsFromTimeseries(promTimeseries []*prompb.TimeSeries, index types.Index) (map[gouuid.UUID]types.MetricData, error) {
	if len(promTimeseries) == 0 {
		return nil, nil
	}

	totalPoints := 0

	metrics := make(map[gouuid.UUID]types.MetricData, len(promTimeseries))

	for _, promSeries := range promTimeseries {
		uuid, data, err := metricFromPromSeries(promSeries, index)
		if err != nil {
			return nil, err
		}

		currentData := metrics[uuid]

		currentData.Points = append(currentData.Points, data.Points...)
		currentData.TimeToLive = compare.MaxInt64(currentData.TimeToLive, data.TimeToLive)

		metrics[uuid] = currentData
		totalPoints += len(data.Points)
	}

	requestsPointsTotalWrite.Add(float64(totalPoints))

	return metrics, nil
}
