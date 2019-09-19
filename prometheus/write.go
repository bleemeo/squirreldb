package prometheus

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"hamsterdb/types"
	"io/ioutil"
	"net/http"
	"time"
)

type WritePoints struct {
	writer types.MetricWriter
}

func toMetricPoints(series *prompb.TimeSeries) types.MetricPoints {
	labels := make(map[string]string)
	var points []types.Point

	for _, label := range series.Labels {
		labels[label.Name] = label.Value
	}

	for _, sample := range series.Samples {
		point := types.Point{
			Time:  time.Unix(sample.Timestamp/1000, 0),
			Value: sample.Value,
		}

		points = append(points, point)
	}

	mPoints := types.MetricPoints{
		Metric: types.Metric{Labels: labels},
		Points: points,
	}

	return mPoints
}

func (w *WritePoints) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger.Printf("WritePoints: Error: Can't read the body (%v)", err)
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)
		return
	}

	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		logger.Printf("WritePoints: Error: Can't decode the body (%v)", err)
		http.Error(writer, "Can't decode the HTTP body", http.StatusBadRequest)
		return
	}

	var writeRequest prompb.WriteRequest

	if err := proto.Unmarshal(decoded, &writeRequest); err != nil {
		logger.Printf("WritePoints: Error: Can't unmarshal the decoded body (%v)", err)
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)
		return
	}

	var msPoints []types.MetricPoints

	for _, series := range writeRequest.Timeseries {
		mPoints := toMetricPoints(series)

		msPoints = append(msPoints, mPoints)
	}

	if err := w.writer.Write(msPoints); err != nil {
		logger.Printf("WritePoints: Error: Can't write MetricPoints array (%v)", err)
		http.Error(writer, "Can't write MetricPoints array", http.StatusBadRequest)
		return
	}
}
