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

type WritePoints struct {
	writer types.Writer
}

func toMetricPoints(series *prompb.TimeSeries) types.MetricPoints {
	labels := make(map[string]string)
	var points []types.Point

	// Convert Prometheus Label array into a label map
	for _, label := range series.Labels {
		labels[label.Name] = label.Value
	}
	// Convert Prometheus Sample array into a Point array
	for _, sample := range series.Samples {
		point := types.Point{
			Time:  time.Unix(sample.Timestamp/1000, 0),
			Value: sample.Value,
		}

		points = append(points, point)
	}

	// Create MetricPoints
	mPoints := types.MetricPoints{
		Metric: types.Metric{Labels: labels},
		Points: points,
	}

	return mPoints
}

func (handler *WritePoints) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Read body
	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		log.Printf("[prometheus] WritePoints: Error: Can't read the body (%v)", err)
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)
		return
	}

	// Decode body
	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		log.Printf("[prometheus] WritePoints: Error: Can't decode the body (%v)", err)
		http.Error(writer, "Can't decode the HTTP body", http.StatusBadRequest)
		return
	}

	// Unmarshal decoded body
	var writeRequest prompb.WriteRequest

	if err := proto.Unmarshal(decoded, &writeRequest); err != nil {
		log.Printf("[prometheus] WritePoints: Error: Can't unmarshal the decoded body (%v)", err)
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)
		return
	}

	// Create MetricPoints array
	var msPoints []types.MetricPoints

	for _, series := range writeRequest.Timeseries {
		mPoints := toMetricPoints(series)

		msPoints = append(msPoints, mPoints)
	}

	// Write MetricPoints array
	if err := handler.writer.Write(msPoints); err != nil {
		log.Printf("[prometheus] WritePoints: Error: Can't write MetricPoints array (%v)", err)
		http.Error(writer, "Can't write MetricPoints array", http.StatusBadRequest)
		return
	}
}
