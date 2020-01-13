package remotestorage

import (
	"github.com/prometheus/prometheus/prompb"

	"net/http"
	"squirreldb/types"
	"time"
)

type ReadMetrics struct {
	index  types.Index
	reader types.MetricReader
}

// ServeHTTP handles read requests
func (r *ReadMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()

	defer func() {
		requestsSecondsRead.Observe(time.Since(start).Seconds())
	}()

	var readRequest prompb.ReadRequest

	err := decodeRequest(request, &readRequest)

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

	for _, request := range requests {
		metrics, err := r.reader.Read(request)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			requestsErrorRead.Inc()

			return
		}

		timeseries, err := promTimeseriesFromMetrics(metrics, r.index)
		if err != nil {
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
		http.Error(writer, "Can't write the read response", http.StatusBadRequest)
		requestsErrorRead.Inc()

		return
	}
}

// Returns a MetricLabelMatcher list- generated from LabelMatcher
func matchersFromPromMatchers(promMatchers []*prompb.LabelMatcher) []types.MetricLabelMatcher {
	if len(promMatchers) == 0 {
		return nil
	}

	matchers := make([]types.MetricLabelMatcher, 0, len(promMatchers))

	for _, promMatcher := range promMatchers {
		matcher := types.MetricLabelMatcher{
			MetricLabel: types.MetricLabel{
				Name:  promMatcher.Name,
				Value: promMatcher.Value,
			},
			Type: uint8(promMatcher.Type),
		}

		matchers = append(matchers, matcher)
	}

	return matchers
}

// Returns a MetricRequest generated from a Query
func requestFromPromQuery(promQuery *prompb.Query, index types.Index) (types.MetricRequest, error) {
	matchers := matchersFromPromMatchers(promQuery.Matchers)
	uuids, err := index.Search(matchers)

	if err != nil {
		return types.MetricRequest{}, err
	}

	request := types.MetricRequest{
		UUIDs:         uuids,
		FromTimestamp: promQuery.StartTimestampMs / 1000,
		ToTimestamp:   promQuery.EndTimestampMs / 1000,
	}

	if promQuery.Hints != nil {
		request.Step = promQuery.Hints.StepMs / 1000
		request.Function = promQuery.Hints.Func
	}

	return request, nil
}

// Returns a MetricRequest list generated from a ReadRequest
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

// Returns a Label list generated from a MetricLabel list
func promLabelsFromLabels(labels []types.MetricLabel) []*prompb.Label {
	if len(labels) == 0 {
		return nil
	}

	promLabels := make([]*prompb.Label, 0, len(labels))

	for _, label := range labels {
		promLabel := &prompb.Label{
			Name:  label.Name,
			Value: label.Value,
		}

		promLabels = append(promLabels, promLabel)
	}

	return promLabels
}

// Returns a Sample list generated from a MetricPoint list
func promSamplesFromPoints(points []types.MetricPoint) []prompb.Sample {
	if len(points) == 0 {
		return nil
	}

	promSamples := make([]prompb.Sample, 0, len(points))

	for _, point := range points {
		promSample := prompb.Sample{
			Value:     point.Value,
			Timestamp: point.Timestamp * 1000,
		}

		promSamples = append(promSamples, promSample)
	}

	return promSamples
}

// Returns a pointer of a TimeSeries generated from a MetricUUID and a MetricData
func promSeriesFromMetric(uuid types.MetricUUID, data types.MetricData, index types.Index) (*prompb.TimeSeries, error) {
	labels, err := index.LookupLabels(uuid)
	if err != nil {
		return nil, err
	}

	promLabels := promLabelsFromLabels(labels)
	promSample := promSamplesFromPoints(data.Points)

	promQueryResult := &prompb.TimeSeries{
		Labels:  promLabels,
		Samples: promSample,
	}

	return promQueryResult, nil
}

// Returns a TimeSeries pointer list generated from a metric list
func promTimeseriesFromMetrics(metrics map[types.MetricUUID]types.MetricData, index types.Index) ([]*prompb.TimeSeries, error) {
	if len(metrics) == 0 {
		return nil, nil
	}

	totalPoints := 0

	promTimeseries := make([]*prompb.TimeSeries, 0, len(metrics))

	for uuid, data := range metrics {
		promSeries, err := promSeriesFromMetric(uuid, data, index)
		if err != nil {
			return nil, err
		}

		promTimeseries = append(promTimeseries, promSeries)

		totalPoints += len(data.Points)
	}

	requestsPointsTotalRead.Add(float64(totalPoints))

	return promTimeseries, nil
}
