package prometheus

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"hamsterdb/types"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type ReadPoints struct {
	reader types.Reader
}

func toMetricRequest(labelMatchers []*prompb.LabelMatcher, hints *prompb.ReadHints) types.MetricRequest {
	labels := make(map[string]string)

	// Convert Prometheus LabelMatcher array into a label map
	for _, labelMatcher := range labelMatchers {
		labels[labelMatcher.Name] = labelMatcher.Value
	}

	// Create MetricRequest
	mRequest := types.MetricRequest{
		Metric:   types.Metric{Labels: labels},
		FromTime: time.Unix(hints.StartMs/1000, 0),
		ToTime:   time.Unix(hints.EndMs/1000, 0),
		Step:     hints.StepMs / 1000,
	}

	return mRequest
}

func toTimeseries(msPoints []types.MetricPoints) []*prompb.TimeSeries {
	var timeseries []*prompb.TimeSeries

	for _, mPoints := range msPoints {
		var labels []*prompb.Label
		var samples []prompb.Sample

		// Convert label map into a Prometheus Label array
		for key, value := range mPoints.Labels {
			label := prompb.Label{
				Name:  key,
				Value: value,
			}

			labels = append(labels, &label)
		}

		// Convert Point array into a Prometheus Sample array
		for _, point := range mPoints.Points {
			sample := prompb.Sample{
				Value:     point.Value,
				Timestamp: point.Time.Unix() * 1000,
			}

			samples = append(samples, sample)
		}

		// Create TimeSeries item
		item := prompb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		}

		timeseries = append(timeseries, &item)
	}

	return timeseries
}

func (handler *ReadPoints) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Read body
	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		log.Printf("[prometheus] ReadPoints: Error: Can't read the body (%v)", err)
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)
		return
	}

	// Decode body
	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		log.Printf("[prometheus] ReadPoints: Error: Can't decode the body (%v)", err)
		http.Error(writer, "Can't decodeRequest the HTTP body", http.StatusBadRequest)
		return
	}

	// Unmarshal decoded body
	var readRequest prompb.ReadRequest

	if err := proto.Unmarshal(decoded, &readRequest); err != nil {
		log.Printf("[prometheus] ReadPoints: Error: Can't unmarshal the decoded body (%v)", err)
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)
		return
	}

	var readResponse prompb.ReadResponse

	// Marshal response
	marshal, err := readResponse.Marshal()

	if err != nil {
		log.Printf("[prometheus] ReadPoints: Error: Can't marshal the read response (%v)", err)
		http.Error(writer, "Can't marshal the read response", http.StatusBadRequest)
		return
	}

	// Encode marshaled response
	encoded := snappy.Encode(nil, marshal)

	// Write response
	_, err = writer.Write(encoded)

	if err != nil {
		log.Printf("[prometheus] ReadPoints: Error: Can't write the read response (%v)", err)
		http.Error(writer, "Can't marshal the read response", http.StatusBadRequest)
		return
	}
}
