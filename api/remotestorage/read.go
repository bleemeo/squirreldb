package remotestorage

import (
	"context"
	"fmt"
	"net/http"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

type readMetrics struct {
	index   types.Index
	reader  types.MetricReader
	metrics *metrics
}

// ServeHTTP handles read requests.
func (r *readMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()
	ctx := request.Context()

	defer func() {
		r.metrics.RequestsSeconds.WithLabelValues("read").Observe(time.Since(start).Seconds())
	}()

	var readRequest prompb.ReadRequest

	reqCtx := requestContext{
		pb: &readRequest,
	}

	if err := decodeRequest(request.Body, &reqCtx); err != nil {
		logger.Printf("Error: Can't decode the read request (%v)", err)
		http.Error(writer, "Can't decode the read request", http.StatusBadRequest)
		r.metrics.RequestsError.WithLabelValues("read").Inc()

		return
	}

	requests, id2labels, err := requestsFromPromReadRequest(ctx, &readRequest, r.index)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		r.metrics.RequestsError.WithLabelValues("read").Inc()

		return
	}

	promQueryResults := make([]*prompb.QueryResult, 0, len(requests))

	for i, request := range requests {
		metricIter, err := r.reader.ReadIter(ctx, request)
		if err != nil {
			logger.Printf("Error: Can't retrieve metric data for %v: %v", readRequest.Queries[i].Matchers, err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			r.metrics.RequestsError.WithLabelValues("read").Inc()

			return
		}

		timeseries, totalPoints, err := promTimeseriesFromMetrics(metricIter, id2labels, len(request.IDs))
		if err != nil {
			logger.Printf("Error: Can't format metric data for %v: %v", readRequest.Queries[i].Matchers, err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			r.metrics.RequestsError.WithLabelValues("read").Inc()

			return
		}

		r.metrics.RequestsPoints.WithLabelValues("read").Add(float64(totalPoints))

		promQueryResult := &prompb.QueryResult{
			Timeseries: timeseries,
		}

		promQueryResults = append(promQueryResults, promQueryResult)
	}

	readResponse := prompb.ReadResponse{
		Results: promQueryResults,
	}

	encodedResponse, err := encodeResponse(&readResponse)
	if err != nil {
		logger.Printf("Error: Can't encode the read response (%v)", err)
		http.Error(writer, "Can't encode the read response", http.StatusBadRequest)
		r.metrics.RequestsError.WithLabelValues("read").Inc()

		return
	}

	_, err = writer.Write(encodedResponse)

	if err != nil {
		logger.Printf("Error: Can't write the read response (%v)", err)
		r.metrics.RequestsError.WithLabelValues("read").Inc()

		return
	}
}

// Returns a MetricRequest generated from a Query.
func requestFromPromQuery(ctx context.Context, promQuery *prompb.Query, index types.Index, id2labels map[types.MetricID]labels.Labels) (map[types.MetricID]labels.Labels, types.MetricRequest, error) {
	matchers, err := fromLabelMatchers(promQuery.Matchers)
	if err != nil {
		return nil, types.MetricRequest{}, fmt.Errorf("read matchers failed: %w", err)
	}

	start := time.Unix(promQuery.StartTimestampMs/1000, promQuery.StartTimestampMs%1000)
	end := time.Unix(promQuery.EndTimestampMs/1000, promQuery.EndTimestampMs%1000)

	metrics, err := index.Search(ctx, start, end, matchers)
	if err != nil {
		return nil, types.MetricRequest{}, fmt.Errorf("search failed: %w", err)
	}

	ids := make([]types.MetricID, 0, metrics.Count())

	if len(id2labels) == 0 {
		id2labels = make(map[types.MetricID]labels.Labels, metrics.Count())
	}

	for metrics.Next() {
		m := metrics.At()
		ids = append(ids, m.ID)
		id2labels[m.ID] = m.Labels
	}

	request := types.MetricRequest{
		IDs:           ids,
		FromTimestamp: promQuery.StartTimestampMs,
		ToTimestamp:   promQuery.EndTimestampMs,
	}

	if promQuery.Hints != nil {
		request.StepMs = promQuery.Hints.StepMs
		request.Function = promQuery.Hints.Func
	}

	return id2labels, request, nil
}

// Returns a MetricRequest list generated from a ReadRequest.
func requestsFromPromReadRequest(ctx context.Context, promReadRequest *prompb.ReadRequest, index types.Index) ([]types.MetricRequest, map[types.MetricID]labels.Labels, error) {
	if len(promReadRequest.Queries) == 0 {
		return nil, nil, nil
	}

	id2labels := make(map[types.MetricID]labels.Labels)
	requests := make([]types.MetricRequest, 0, len(promReadRequest.Queries))

	for _, query := range promReadRequest.Queries {
		var (
			request types.MetricRequest
			err     error
		)

		id2labels, request, err = requestFromPromQuery(ctx, query, index, id2labels)
		if err != nil {
			return requests, id2labels, err
		}

		requests = append(requests, request)
	}

	return requests, id2labels, nil
}

// Returns a Sample list generated from a MetricPoint list.
func promSamplesFromPoints(points []types.MetricPoint) []prompb.Sample {
	if len(points) == 0 {
		return nil
	}

	promSamples := make([]prompb.Sample, 0, len(points))

	for _, point := range points {
		promSample := prompb.Sample{
			Value:     point.Value,
			Timestamp: point.Timestamp,
		}

		promSamples = append(promSamples, promSample)
	}

	return promSamples
}

// Returns a pointer of a TimeSeries generated from a ID and a MetricData.
func promSeriesFromMetric(id types.MetricID, data types.MetricData, id2labels map[types.MetricID]labels.Labels) (*prompb.TimeSeries, error) {
	labels, ok := id2labels[id]
	if !ok {
		return nil, fmt.Errorf("metric with ID %d not found", id)
	}

	promSample := promSamplesFromPoints(data.Points)

	promQueryResult := &prompb.TimeSeries{
		Labels:  labelsToLabelsProto(labels, nil),
		Samples: promSample,
	}

	return promQueryResult, nil
}

// Returns a TimeSeries pointer list generated from a metric list.
func promTimeseriesFromMetrics(metrics types.MetricDataSet, id2labels map[types.MetricID]labels.Labels, sizeHint int) ([]*prompb.TimeSeries, int, error) {
	totalPoints := 0

	promTimeseries := make([]*prompb.TimeSeries, 0, sizeHint)

	for metrics.Next() {
		data := metrics.At()

		promSeries, err := promSeriesFromMetric(data.ID, data, id2labels)
		if err != nil {
			return nil, totalPoints, err
		}

		promTimeseries = append(promTimeseries, promSeries)

		totalPoints += len(data.Points)
	}

	return promTimeseries, totalPoints, metrics.Err()
}
