package remotestorage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"squirreldb/cassandra/tsdb"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/prompb"
)

type errBadRequest struct {
	err error
}

func (e errBadRequest) Error() string {
	return fmt.Sprintf("bad request: %v", e.err)
}

var errTypeAssertion = errors.New("type assertion failed")

type writeMetrics struct {
	index    types.Index
	writer   types.MetricWriter
	reqCtxCh chan *requestContext
	metrics  *metrics
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

	err := w.do(ctx, request)

	w.metrics.RequestsSeconds.WithLabelValues("write").Observe(time.Since(start).Seconds())

	if err != nil {
		w.metrics.RequestsError.WithLabelValues("write").Inc()

		statusCode := http.StatusInternalServerError

		switch {
		case errors.Is(err, ctx.Err()):
			statusCode = http.StatusRequestTimeout
		case errors.As(err, &errBadRequest{}):
			statusCode = http.StatusBadRequest
		}

		if statusCode != http.StatusRequestTimeout {
			logger.Printf("Error: %v", err)
		}

		http.Error(writer, err.Error(), statusCode)

		return
	}
}

func (w *writeMetrics) do(ctx context.Context, request *http.Request) error {
	reqCtx := w.getRequestContext(ctx)
	if ctx.Err() != nil || reqCtx == nil {
		return fmt.Errorf("request cancelled: %w", ctx.Err())
	}

	defer w.putRequestContext(reqCtx)

	err := decodeRequest(request.Body, reqCtx)
	if err != nil {
		return errBadRequest{err: fmt.Errorf("can't decode request: %w", err)}
	}

	writeRequest, ok := reqCtx.pb.(*prompb.WriteRequest)
	if !ok {
		return errTypeAssertion
	}

	metrics, totalPoints, err := metricsFromTimeseries(ctx, writeRequest.Timeseries, w.index)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("unable to convert metrics: %w", err)
	}

	w.metrics.RequestsPoints.WithLabelValues("write").Add(float64(totalPoints))

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := w.writer.Write(ctx, metrics); err != nil && ctx.Err() == nil {
		return fmt.Errorf("unable to write metrics: %w", err)
	}

	return nil
}

// Returns a MetricPoint list generated from a Sample list.
func pointsFromPromSamples(promSamples []prompb.Sample) []types.MetricPoint {
	if len(promSamples) == 0 {
		return nil
	}

	points := make([]types.MetricPoint, len(promSamples))

	for i, promSample := range promSamples {
		points[i].Timestamp = promSample.Timestamp
		if math.IsNaN(promSample.Value) && !value.IsStaleNaN(promSample.Value) {
			// Ensure canonical NaN value (but still allow StaleNaN).
			points[i].Value = math.Float64frombits(value.NormalNaN)
		} else {
			points[i].Value = promSample.Value
		}
	}

	return points
}

// Returns a metric list generated from a TimeSeries list.
func metricsFromTimeseries(ctx context.Context, promTimeseries []prompb.TimeSeries, index types.Index) ([]types.MetricData, int, error) {
	if len(promTimeseries) == 0 {
		return nil, 0, nil
	}

	idToIndex := make(map[types.MetricID]int, len(promTimeseries))

	totalPoints := 0
	metrics := make([]types.MetricData, 0, len(promTimeseries))

	requests := make([]types.LookupRequest, 0, len(promTimeseries))

	for _, promSeries := range promTimeseries {
		if len(promSeries.Samples) == 0 {
			continue
		}

		min := int64(math.MaxInt64)
		max := int64(math.MinInt64)

		for _, s := range promSeries.Samples {
			if min > s.Timestamp {
				min = s.Timestamp
			}

			if max < s.Timestamp {
				max = s.Timestamp
			}
		}

		if min < time.Now().Add(-tsdb.MaxPastDelay).Unix()*1000 {
			logger.Printf("warning: points with timestamp %v will be ignored by pre-aggregation", time.Unix(min/1000, 0))
		}

		requests = append(requests, types.LookupRequest{
			Labels: labelProtosToLabels(promSeries.Labels),
			End:    time.Unix(max/1000, max%1000),
			Start:  time.Unix(min/1000, min%1000),
		})
	}

	ids, ttls, err := index.LookupIDs(ctx, requests)
	if err != nil {
		return nil, totalPoints, fmt.Errorf("metric ID lookup failed: %w", err)
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

	return metrics, totalPoints, nil
}
