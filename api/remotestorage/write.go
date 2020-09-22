package remotestorage

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"

	"net/http"
	"squirreldb/types"
	"time"
)

type writeMetrics struct {
	index    types.Index
	writer   types.MetricWriter
	reqCtxCh chan *requestContext
}

func (w *writeMetrics) getRequestContext(ctx context.Context) *requestContext {
	select {
	case reqCtx := <-w.reqCtxCh:
		return reqCtx
	case <-ctx.Done():
		return nil
	}
}

func (w *writeMetrics) putRequestContext(reqCtx *requestContext) {
	w.reqCtxCh <- reqCtx
}

// ServeHTTP handles writing requests.
func (w *writeMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()
	ctx := request.Context()

	defer func() {
		requestsSecondsWrite.Observe(time.Since(start).Seconds())
	}()

	reqCtx := w.getRequestContext(ctx)
	if ctx.Err() != nil || reqCtx == nil {
		http.Error(writer, "Request cancelled", http.StatusBadRequest)
		requestsErrorWrite.Inc()

		return
	}

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
		logger.Printf("Unable to convert to internal metric: %v", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		requestsErrorWrite.Inc()

		return
	}

	if err := w.writer.Write(ctx, metrics); err != nil {
		logger.Printf("Unable to write metric: %v", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		requestsErrorWrite.Inc()

		return
	}
}

// Returns a MetricPoint list generated from a Sample list.
func pointsFromPromSamples(promSamples []prompb.Sample) []types.MetricPoint {
	if len(promSamples) == 0 {
		return nil
	}

	points := make([]types.MetricPoint, len(promSamples))

	for i, promSample := range promSamples {
		points[i].Timestamp = promSample.Timestamp
		points[i].Value = promSample.Value
	}

	return points
}

// Returns a metric list generated from a TimeSeries list.
func metricsFromTimeseries(promTimeseries []prompb.TimeSeries, index types.Index) ([]types.MetricData, error) {
	if len(promTimeseries) == 0 {
		return nil, nil
	}

	idToIndex := make(map[types.MetricID]int, len(promTimeseries))

	totalPoints := 0
	metrics := make([]types.MetricData, 0, len(promTimeseries))

	labelsList := make([]labels.Labels, len(promTimeseries))

	for i, promSeries := range promTimeseries {
		labelsList[i] = labelProtosToLabels(promSeries.Labels)
	}

	ids, ttls, err := index.LookupIDs(labelsList)

	if err != nil {
		return nil, fmt.Errorf("metric ID lookup failed: %v", err)
	}

	for i, promSeries := range promTimeseries {
		points := pointsFromPromSamples(promSeries.Samples)
		data := types.MetricData{
			ID:         ids[i],
			Points:     points,
			TimeToLive: ttls[i],
		}

		if idx, found := idToIndex[data.ID]; found {
			metrics[idx].Points = append(metrics[idx].Points, data.Points...)
			if metrics[idx].TimeToLive < data.TimeToLive {
				metrics[idx].TimeToLive = data.TimeToLive
			}
		} else {
			metrics = append(metrics, data)
			idToIndex[data.ID] = len(metrics) - 1
		}

		totalPoints += len(data.Points)
	}

	requestsPointsTotalWrite.Add(float64(totalPoints))

	return metrics, nil
}
