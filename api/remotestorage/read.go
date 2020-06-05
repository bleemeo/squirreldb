package remotestorage

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"

	"net/http"
	"squirreldb/types"
	"time"
)

type readMetrics struct {
	index  types.Index
	reader types.MetricReader
}

// ServeHTTP handles read requests.
func (r *readMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()

	defer func() {
		requestsSecondsRead.Observe(time.Since(start).Seconds())
	}()

	var readRequest prompb.ReadRequest

	reqCtx := requestContext{
		pb: &readRequest,
	}
	err := decodeRequest(request.Body, &reqCtx)

	if err != nil {
		logger.Printf("Error: Can't decode the read request (%v)", err)
		http.Error(writer, "Can't decode the read request", http.StatusBadRequest)
		requestsErrorRead.Inc()

		return
	}

	requests, err := requestsFromPromReadRequest(&readRequest, r.index)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		requestsErrorRead.Inc()

		return
	}

	promQueryResults := make([]*prompb.QueryResult, 0, len(requests))

	for i, request := range requests {
		metricIter, err := r.reader.ReadIter(request)
		if err != nil {
			logger.Printf("Error: Can't retrieve metric data for %v: %v", readRequest.Queries[i].Matchers, err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			requestsErrorRead.Inc()

			return
		}

		timeseries, err := promTimeseriesFromMetrics(metricIter, r.index, len(request.IDs))
		if err != nil {
			logger.Printf("Error: Can't format metric data for %v: %v", readRequest.Queries[i].Matchers, err)
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			requestsErrorRead.Inc()

			return
		}

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
		requestsErrorRead.Inc()

		return
	}

	_, err = writer.Write(encodedResponse)

	if err != nil {
		logger.Printf("Error: Can't write the read response (%v)", err)
		requestsErrorRead.Inc()

		return
	}
}

// Returns a MetricRequest generated from a Query.
func requestFromPromQuery(promQuery *prompb.Query, index types.Index) (types.MetricRequest, error) {
	matchers, err := remote.FromLabelMatchers(promQuery.Matchers)
	if err != nil {
		return types.MetricRequest{}, err
	}

	ids, err := index.Search(matchers)

	if err != nil {
		return types.MetricRequest{}, err
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

	return request, nil
}

// Returns a MetricRequest list generated from a ReadRequest.
func requestsFromPromReadRequest(promReadRequest *prompb.ReadRequest, index types.Index) ([]types.MetricRequest, error) {
	if len(promReadRequest.Queries) == 0 {
		return nil, nil
	}

	requests := make([]types.MetricRequest, 0, len(promReadRequest.Queries))

	for _, query := range promReadRequest.Queries {
		request, err := requestFromPromQuery(query, index)
		if err != nil {
			return requests, err
		}

		requests = append(requests, request)
	}

	return requests, nil
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
func promSeriesFromMetric(id types.MetricID, data types.MetricData, index types.Index) (*prompb.TimeSeries, error) {
	labels, err := index.LookupLabels(id)
	if err != nil {
		return nil, err
	}

	promSample := promSamplesFromPoints(data.Points)

	promQueryResult := &prompb.TimeSeries{
		Labels:  labelsToLabelsProto(labels, nil),
		Samples: promSample,
	}

	return promQueryResult, nil
}

// Returns a TimeSeries pointer list generated from a metric list.
func promTimeseriesFromMetrics(metrics types.MetricDataSet, index types.Index, sizeHint int) ([]*prompb.TimeSeries, error) {
	totalPoints := 0

	promTimeseries := make([]*prompb.TimeSeries, 0, sizeHint)

	for metrics.Next() {
		data := metrics.At()

		promSeries, err := promSeriesFromMetric(data.ID, data, index)
		if err != nil {
			return nil, err
		}

		promTimeseries = append(promTimeseries, promSeries)

		totalPoints += len(data.Points)
	}

	requestsPointsTotalRead.Add(float64(totalPoints))

	return promTimeseries, metrics.Err()
}
