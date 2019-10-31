package prometheus

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io/ioutil"
	"net/http"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

type ReadPoints struct {
	indexer types.MetricIndexer
	reader  types.MetricReader
}

// Serve the read handler
// Decodes the request and transforms it into MetricRequests
// Retrieves metrics via MetricRequests
// Generates and returns a response containing the requested data
func (r *ReadPoints) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger.Println("ReadPoints: Error: Can't read the body (", err, ")")
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)

		return
	}

	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		logger.Println("ReadPoints: Error: Can't decode the body (", err, ")")
		http.Error(writer, "Can't decode the HTTP body", http.StatusBadRequest)

		return
	}

	var readRequest prompb.ReadRequest

	if err := proto.Unmarshal(decoded, &readRequest); err != nil {
		logger.Println("ReadPoints: Error: Can't unmarshal the decoded body (", err, ")")
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)

		return
	}

	var readResponse prompb.ReadResponse

	for _, query := range readRequest.Queries {
		labels := pbMatchersToLabels(query.Matchers)
		matchers := r.indexer.UUIDs(labels)
		var uuids types.MetricUUIDs

		for uuid := range matchers {
			uuids = append(uuids, uuid)
		}

		request := toMetricRequest(uuids, query)
		var metrics types.Metrics

		retry.Do(func() error {
			var err error
			metrics, err = r.reader.Read(request)

			return err
		}, "prometheus", "ReadPoints",
			"Can't read in storage",
			"Resolved: Read in storage",
			retry.NewBackOff(30*time.Second))

		queryResult := toQueryResult(matchers, metrics)

		readResponse.Results = append(readResponse.Results, &queryResult)
	}

	marshal, err := readResponse.Marshal()

	if err != nil {
		logger.Println("ReadPoints: Error: Can't marshal the read response (", err, ")")
		http.Error(writer, "Can't marshal the read response", http.StatusBadRequest)

		return
	}

	encoded := snappy.Encode(nil, marshal)

	_, err = writer.Write(encoded)

	if err != nil {
		logger.Println("ReadPoints: Error: Can't write the read response (", err, ")")
		http.Error(writer, "Can't marshal the read response", http.StatusBadRequest)

		return
	}
}

// Convert Prometheus LabelMatchers to MetricLabels
func pbMatchersToLabels(pbLabels []*prompb.LabelMatcher) types.MetricLabels {
	labels := make(types.MetricLabels, 0, len(pbLabels))

	for _, pbLabel := range pbLabels {
		label := types.MetricLabel{
			Name:  pbLabel.Name,
			Value: pbLabel.Value,
			Type:  uint8(pbLabel.Type),
		}

		labels = append(labels, label)
	}

	return labels
}

// Convert MetricLabels to Prometheus Labels
func labelsToPbLabels(labels types.MetricLabels) []*prompb.Label {
	pbLabels := make([]*prompb.Label, 0, len(labels))

	for _, label := range labels {
		pbLabel := prompb.Label{
			Name:  label.Name,
			Value: label.Value,
		}

		pbLabels = append(pbLabels, &pbLabel)
	}

	return pbLabels
}

// Generate MetricRequest
func toMetricRequest(uuids types.MetricUUIDs, query *prompb.Query) types.MetricRequest {
	request := types.MetricRequest{
		UUIDs:         uuids,
		FromTimestamp: query.StartTimestampMs / 1000,
		ToTimestamp:   query.EndTimestampMs / 1000,
	}

	if query.Hints != nil {
		request.Step = query.Hints.StepMs / 1000
		request.Function = query.Hints.Func
	}

	return request
}

// Generate Prometheus QueryResult
func toQueryResult(matchers map[types.MetricUUID]types.MetricLabels, metrics types.Metrics) prompb.QueryResult {
	var queryResult prompb.QueryResult

	for uuid, metricData := range metrics {
		labels := matchers[uuid]
		pbLabels := labelsToPbLabels(labels)

		series := prompb.TimeSeries{
			Labels: pbLabels,
		}

		for _, point := range metricData.Points {
			sample := prompb.Sample{
				Value:     point.Value,
				Timestamp: point.Timestamp * 1000,
			}

			series.Samples = append(series.Samples, sample)
		}

		queryResult.Timeseries = append(queryResult.Timeseries, &series)
	}

	return queryResult
}
