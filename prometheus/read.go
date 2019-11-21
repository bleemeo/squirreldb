package prometheus

import (
	"github.com/prometheus/prometheus/prompb"
	"net/http"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

type ReadMetrics struct {
	indexer types.MetricIndexer
	reader  types.MetricReader
}

// ServeHTTP handles read requests
func (r *ReadMetrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()

	var readRequest prompb.ReadRequest

	err := decodeRequest(request, &readRequest)

	if err != nil {
		logger.Printf("Error: Can't decode the read request (%v)", err)
		http.Error(writer, "Can't decode the read request", http.StatusBadRequest)

		return
	}

	requests := requestsFromPromReadRequest(&readRequest, r.indexer.UUIDs)

	var promQueryResults []*prompb.QueryResult

	for _, request := range requests {
		var metrics map[types.MetricUUID]types.MetricData

		retry.Print(func() error {
			var err error
			metrics, err = r.reader.Read(request)

			return err
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't read metrics with the reader",
			"Resolved: Read metrics with the reader")

		promQueryResult := &prompb.QueryResult{
			Timeseries: promTimeseriesFromMetrics(metrics, r.indexer.Labels),
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

		return
	}

	_, err = writer.Write(encodedResponse)

	if err != nil {
		logger.Printf("Error: Can't write the read response (%v)", err)
		http.Error(writer, "Can't write the read response", http.StatusBadRequest)

		return
	}

	requestSecondsRead.Observe(time.Since(start).Seconds())
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
func requestFromPromQuery(promQuery *prompb.Query, fun func(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID) types.MetricRequest {
	matchers := matchersFromPromMatchers(promQuery.Matchers)
	uuids := fun(matchers, false)

	request := types.MetricRequest{
		UUIDs:         uuids,
		FromTimestamp: promQuery.StartTimestampMs / 1000,
		ToTimestamp:   promQuery.EndTimestampMs / 1000,
	}

	if promQuery.Hints != nil {
		request.Step = promQuery.Hints.StepMs / 1000
		request.Function = promQuery.Hints.Func
	}

	return request
}

// Returns a MetricRequest list generated from a ReadRequest
func requestsFromPromReadRequest(promReadRequest *prompb.ReadRequest, fun func(matchers []types.MetricLabelMatcher, all bool) []types.MetricUUID) []types.MetricRequest {
	if len(promReadRequest.Queries) == 0 {
		return nil
	}

	requests := make([]types.MetricRequest, 0, len(promReadRequest.Queries))

	for _, query := range promReadRequest.Queries {
		request := requestFromPromQuery(query, fun)

		requests = append(requests, request)
	}

	return requests
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
func promSeriesFromMetric(uuid types.MetricUUID, data types.MetricData, fun func(uuid types.MetricUUID) []types.MetricLabel) *prompb.TimeSeries {
	labels := fun(uuid)
	promLabels := promLabelsFromLabels(labels)
	promSample := promSamplesFromPoints(data.Points)

	promQueryResult := &prompb.TimeSeries{
		Labels:  promLabels,
		Samples: promSample,
	}

	return promQueryResult
}

// Returns a TimeSeries pointer list generated from a metric list
func promTimeseriesFromMetrics(metrics map[types.MetricUUID]types.MetricData, fun func(uuid types.MetricUUID) []types.MetricLabel) []*prompb.TimeSeries {
	if len(metrics) == 0 {
		return nil
	}

	promTimeseries := make([]*prompb.TimeSeries, 0, len(metrics))

	for uuid, data := range metrics {
		promSeries := promSeriesFromMetric(uuid, data, fun)

		promTimeseries = append(promTimeseries, promSeries)
	}

	return promTimeseries
}
