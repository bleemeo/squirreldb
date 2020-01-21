package remotestorage

import (
	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"

	"net/http"
	"squirreldb/types"
	"time"
)

type WriteMetrics struct {
	index    types.Index
	writer   types.MetricWriter
	reqCtxCh chan *requestContext
}

func (w *WriteMetrics) getRequestContext() *requestContext {
	select {
	case reqCtx := <-w.reqCtxCh:
		return reqCtx
	default:
		return &requestContext{
			pb: &prompb.WriteRequest{},
		}
	}
}

func (w *WriteMetrics) putRequestContext(reqCtx *requestContext) {
	select {
	case w.reqCtxCh <- reqCtx:
	default:
	}
}

// ServeHTTP handles writing requests
func (w *WriteMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()

	defer func() {
		requestsSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	reqCtx := w.getRequestContext()
	defer w.putRequestContext(reqCtx)
	err := decodeRequest(request.Body, reqCtx)

	if err != nil {
		logger.Printf("Error: Can't decode the write request (%v)", err)
		http.Error(writer, "Can't decode the write request", http.StatusBadRequest)
		requestsErrorWrite.Inc()

		return
	}

	writeRequest := reqCtx.pb.(*prompb.WriteRequest)

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
func metricFromPromSeries(promSeries *prompb.TimeSeries, index types.Index) (types.MetricData, error) {
	labels := labelsFromPromLabels(promSeries.Labels)

	uuid, timeToLive, err := index.LookupUUID(labels)
	if err != nil {
		return types.MetricData{}, err
	}

	points := pointsFromPromSamples(promSeries.Samples)
	data := types.MetricData{
		UUID:       uuid,
		Points:     points,
		TimeToLive: timeToLive,
	}

	return data, nil
}

// Returns a metric list generated from a TimeSeries list
func metricsFromTimeseries(promTimeseries []*prompb.TimeSeries, index types.Index) ([]types.MetricData, error) {
	if len(promTimeseries) == 0 {
		return nil, nil
	}

	uuidToIndex := make(map[gouuid.UUID]int, len(promTimeseries))

	totalPoints := 0
	metrics := make([]types.MetricData, len(promTimeseries))
	i := 0

	for _, promSeries := range promTimeseries {
		data, err := metricFromPromSeries(promSeries, index)
		if err != nil {
			return nil, err
		}

		if idx, found := uuidToIndex[data.UUID]; found {
			metrics[idx].Points = append(metrics[idx].Points, data.Points...)
			if metrics[idx].TimeToLive < data.TimeToLive {
				metrics[idx].TimeToLive = data.TimeToLive
			}
		} else {
			metrics[i] = data
			uuidToIndex[data.UUID] = i

			i++
		}

		totalPoints += len(data.Points)
	}

	metrics = metrics[:i]

	requestsPointsTotalWrite.Add(float64(totalPoints))

	return metrics, nil
}
