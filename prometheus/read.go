package prometheus

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io/ioutil"
	"net/http"
	"squirreldb/types"
	"time"
)

type ReadPoints struct {
	reader types.MetricReader
}

// Convert Prometheus Query to MetricRequest
func toMetricRequest(query *prompb.Query) types.MetricRequest {
	labels := make(map[string]string)

	for _, labelMatcher := range query.Matchers {
		labels[labelMatcher.Name] = labelMatcher.Value
	}

	mRequest := types.MetricRequest{
		Metric:   types.Metric{Labels: labels},
		FromTime: time.Unix(query.StartTimestampMs/1000, 0),
		ToTime:   time.Unix(query.EndTimestampMs/1000, 0),
	}

	if query.Hints != nil {
		mRequest.Step = query.Hints.StepMs / 1000
	}

	return mRequest
}

// Convert MetricPoints list to Prometheus TimeSeries list
func toTimeseries(msPoints []types.MetricPoints) []*prompb.TimeSeries {
	var timeseries []*prompb.TimeSeries

	for _, mPoints := range msPoints {
		var labels []*prompb.Label
		var samples []prompb.Sample

		for key, value := range mPoints.Labels {
			label := prompb.Label{
				Name:  key,
				Value: value,
			}

			labels = append(labels, &label)
		}

		for _, point := range mPoints.Points {
			sample := prompb.Sample{
				Value:     point.Value,
				Timestamp: point.Time.Unix() * 1000,
			}

			samples = append(samples, sample)
		}

		item := prompb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		}

		timeseries = append(timeseries, &item)
	}

	return timeseries
}

// Serve the read handler
// Decodes the request and transforms it into MetricRequests. Retrieves points via MetricRequests.
// Generates and returns a response containing the requested data
func (r *ReadPoints) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger.Printf("ReadPoints: Error: Can't read the body (%v)", err)
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)
		return
	}

	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		logger.Printf("ReadPoints: Error: Can't decode the body (%v)", err)
		http.Error(writer, "Can't decode the HTTP body", http.StatusBadRequest)
		return
	}

	var readRequest prompb.ReadRequest

	if err := proto.Unmarshal(decoded, &readRequest); err != nil {
		logger.Printf("ReadPoints: Error: Can't unmarshal the decoded body (%v)", err)
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)
		return
	}

	var readResponse prompb.ReadResponse

	for _, query := range readRequest.Queries {
		mRequest := toMetricRequest(query)
		// TODO: Handle error
		msPoints, _ := r.reader.Read(mRequest)
		series := toTimeseries(msPoints)
		queryResult := prompb.QueryResult{Timeseries: series}

		readResponse.Results = append(readResponse.Results, &queryResult)
	}

	marshal, err := readResponse.Marshal()

	if err != nil {
		logger.Printf("ReadPoints: Error: Can't marshal the read response (%v)", err)
		http.Error(writer, "Can't marshal the read response", http.StatusBadRequest)
		return
	}

	encoded := snappy.Encode(nil, marshal)

	_, err = writer.Write(encoded)

	if err != nil {
		logger.Printf("ReadPoints: Error: Can't write the read response (%v)", err)
		http.Error(writer, "Can't marshal the read response", http.StatusBadRequest)
		return
	}
}
