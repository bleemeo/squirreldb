package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/api/promql"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

func TestFakeRemoteWriter(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		pointFutureness  time.Duration
		backdateOffset   time.Duration
		expectStatusCode int
	}{
		{
			pointFutureness:  time.Minute,
			backdateOffset:   time.Hour,
			expectStatusCode: http.StatusNoContent,
		},
		{
			pointFutureness:  time.Hour,
			backdateOffset:   0,
			expectStatusCode: http.StatusBadRequest,
		},
		{
			pointFutureness:  time.Hour,
			backdateOffset:   2 * time.Hour,
			expectStatusCode: http.StatusNoContent,
		},
	}

	zlog := zerolog.New(zerolog.NewTestWriter(t))
	index := dummy.NewIndex([]types.MetricLabel{})
	tsdb := &dummy.MemoryTSDB{}
	store := promql.NewStore(
		zlog,
		index,
		tsdb,
		"__account_id",
		false,
		0,
		0,
		prometheus.NewRegistry(),
	)

	for _, tc := range testCases {
		name := fmt.Sprintf("Point in %s and offset of %s", tc.pointFutureness, tc.backdateOffset)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			appendable := new(dummyAppendable)
			api := NewPrometheus(store, appendable, 1, prometheus.NewRegistry(), false, zlog)
			router := route.New()

			patchRemoteWriteHandler(api, tc.backdateOffset)
			api.Register(router)

			pointTS := time.Now().Round(time.Second).Add(tc.pointFutureness)

			req, err := http.NewRequest(http.MethodPost, "/write", makeWriteReqBody(t, pointTS)) //nolint: noctx
			if err != nil {
				t.Fatal("Failed to make request:", err)
			}

			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, req)

			if recorder.Code != tc.expectStatusCode {
				t.Fatalf("Expected status code %d, got %d", tc.expectStatusCode, recorder.Code)
			}

			if recorder.Code < 300 {
				if len(appendable.points) != 1 {
					t.Fatalf("Expected 1 point, got %d", len(appendable.points))
				}

				gotTime := time.UnixMilli(appendable.points[0].Timestamp)
				if !gotTime.Equal(pointTS) {
					diff := math.Abs(gotTime.Sub(pointTS).Seconds()) * float64(time.Second)
					t.Fatalf("The point was not written at the good timestamp: want %s, got %s (diff of %s)",
						pointTS.Format(time.DateTime), gotTime.Format(time.DateTime), time.Duration(diff),
					)
				}
			}
		})
	}
}

func makeWriteReqBody(t *testing.T, pointTS time.Time) io.Reader {
	t.Helper()

	series := prompb.TimeSeries{
		Labels: []prompb.Label{
			{
				Name:  "__bleemeo_account__",
				Value: "some-uuid",
			},
			{
				Name:  "__name__",
				Value: "cpu_used",
			},
		},
		Samples: []prompb.Sample{
			{
				Value:     7,
				Timestamp: pointTS.UnixMilli(),
			},
		},
	}
	writeReq := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{series},
	}

	body, err := writeReq.Marshal()
	if err != nil {
		t.Fatal("Failed to marshal write request:", err)
	}

	compressedBody := snappy.Encode(nil, body)

	return bytes.NewReader(compressedBody)
}

type dummyAppendable struct {
	points []prompb.Sample
}

func (dable *dummyAppendable) Appender(ctx context.Context) storage.Appender {
	var offsetMs int64

	if offset := ctx.Value(types.BackdateContextKey{}); offset != nil {
		offsetMs = offset.(int64) //nolint: forcetypeassert
	}

	return &dummyAppender{
		offsetMs: offsetMs,
		points:   &dable.points,
	}
}

type dummyAppender struct {
	offsetMs int64
	points   *[]prompb.Sample
}

func (der *dummyAppender) Append(
	ref storage.SeriesRef,
	_ labels.Labels,
	t int64, v float64,
) (storage.SeriesRef, error) {
	*der.points = append(*der.points, prompb.Sample{
		Value:     v,
		Timestamp: t + der.offsetMs,
	})

	return ref, nil
}

func (der *dummyAppender) Commit() error {
	return nil
}

func (der *dummyAppender) Rollback() error {
	return nil
}

func (der *dummyAppender) AppendExemplar(
	storage.SeriesRef,
	labels.Labels,
	exemplar.Exemplar,
) (storage.SeriesRef, error) {
	panic("implement me")
}

func (der *dummyAppender) AppendHistogram(
	storage.SeriesRef,
	labels.Labels,
	int64,
	*histogram.Histogram,
	*histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	panic("implement me")
}

func (der *dummyAppender) UpdateMetadata(
	storage.SeriesRef,
	labels.Labels,
	metadata.Metadata,
) (storage.SeriesRef, error) {
	panic("implement me")
}

func (der *dummyAppender) AppendCTZeroSample(
	storage.SeriesRef,
	labels.Labels,
	int64,
	int64,
) (storage.SeriesRef, error) {
	panic("implement me")
}

func TestBackdatePoints(t *testing.T) {
	t.Parallel()

	pointTS := time.Now().Round(time.Second).Add(time.Hour)
	offsetMs := (24 * time.Hour).Milliseconds()

	testFn := func(t *testing.T, ctx context.Context, updatedPointTS int64) {
		t.Helper()

		appliedOffset := ctx.Value(types.BackdateContextKey{})
		if appliedOffset == nil || appliedOffset.(int64) != offsetMs { //nolint: forcetypeassert
			t.Fatalf("Expected applied offset to be %d, got %v", offsetMs, appliedOffset)
		}

		if updatedPointTS != pointTS.UnixMilli()-offsetMs {
			t.Fatalf("Expected timestamp to be %d, got %d", pointTS.UnixMilli()-offsetMs, updatedPointTS)
		}
	}

	t.Run("V1", func(t *testing.T) {
		t.Parallel()

		series := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  "__bleemeo_account__",
						Value: "some-uuid",
					},
					{
						Name:  "__name__",
						Value: "cpu_used",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     7,
						Timestamp: pointTS.UnixMilli(),
					},
				},
			},
		}

		ctx := backdateSeries(context.Background(), series, offsetMs)

		testFn(t, ctx, series[0].Samples[0].Timestamp)
	})

	t.Run("V2", func(t *testing.T) {
		t.Parallel()

		series := []writev2.TimeSeries{
			{
				// Labels in v2 are weird
				Samples: []writev2.Sample{
					{
						Value:     7,
						Timestamp: pointTS.UnixMilli(),
					},
				},
			},
		}

		ctx := backdateSeries(context.Background(), series, offsetMs)

		testFn(t, ctx, series[0].Samples[0].Timestamp)
	})
}
