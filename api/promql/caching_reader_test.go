package promql

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"squirreldb/dummy"
	"squirreldb/logger"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// createTSDB create index & TSDB for caching tests.
// The following metrics are created:
// * myname{item="1"}: 4 points every 10 seconds between 2023-05-10T12:00:00Z to 2023-05-10T12:00:30Z
// * myname{item="2"}: 4 points every 10 seconds between 2023-05-10T12:00:20Z to 2023-05-10T12:00:50Z
// * myname{item="3"}: 4 points every 1  hour    between 2023-05-10T11:00:10Z to 2023-05-10T13:00:10Z
// Metrics use respectively the ID, metricID1, metricID2, metricID3.
func createTSDB() (*dummy.Index, *dummy.MemoryTSDB) {
	metric1 := types.MetricLabel{
		Labels: labels.FromMap(map[string]string{
			"__name__": "myname",
			"item":     "1",
		}),
		ID: metricID1,
	}
	metric2 := types.MetricLabel{
		Labels: labels.FromMap(map[string]string{
			"__name__": "myname",
			"item":     "2",
		}),
		ID: metricID2,
	}
	metric3 := types.MetricLabel{
		Labels: labels.FromMap(map[string]string{
			"__name__": "myname",
			"item":     "3",
		}),
		ID: metricID3,
	}

	index := dummy.NewIndex([]types.MetricLabel{metric1, metric2, metric3})
	tsdb := &dummy.MemoryTSDB{
		Data: map[types.MetricID]types.MetricData{
			metricID1: {
				ID: metricID1,
				Points: []types.MetricPoint{
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
						Value:     1,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 10, 0, time.UTC).UnixMilli(),
						Value:     2,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 20, 0, time.UTC).UnixMilli(),
						Value:     3,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 30, 0, time.UTC).UnixMilli(),
						Value:     4,
					},
				},
				TimeToLive: 86400,
			},
			metricID2: {
				ID: metricID2,
				Points: []types.MetricPoint{
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 20, 0, time.UTC).UnixMilli(),
						Value:     10,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 30, 0, time.UTC).UnixMilli(),
						Value:     20,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 40, 0, time.UTC).UnixMilli(),
						Value:     30,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 50, 0, time.UTC).UnixMilli(),
						Value:     40,
					},
				},
				TimeToLive: 86400,
			},
			metricID3: {
				ID: metricID3,
				Points: []types.MetricPoint{
					{
						Timestamp: time.Date(2023, 5, 10, 11, 0, 10, 0, time.UTC).UnixMilli(),
						Value:     10,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 12, 0, 10, 0, time.UTC).UnixMilli(),
						Value:     20,
					},
					{
						Timestamp: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC).UnixMilli(),
						Value:     30,
					},
				},
				TimeToLive: 86400,
			},
		},
	}

	return index, tsdb
}

func Test_cachingReader_ReadIter(t *testing.T) { //nolint:maintidx
	_, realStore := createTSDB()

	type readRequest struct {
		name             string
		req              types.MetricRequest
		numberOfNextCall int // 0 means unlimited, -1 means no call.
		pointsRead       int
	}

	tests := []struct {
		name     string
		reader   types.MetricReader
		requests []readRequest
	}{
		{
			name: "simple",
			reader: &mockStoreFixedResponse{
				response: []types.MetricData{
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Now().UnixMilli(),
								Value:     42.0,
							},
						},
						ID:         metricID1,
						TimeToLive: 789,
					},
				},
			},
			requests: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       1,
					numberOfNextCall: 0,
				},
			},
		},
		{
			name: "simple-multiple-points",
			reader: &mockStoreFixedResponse{
				response: []types.MetricData{
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 25, 30, time.UTC).UnixMilli(),
								Value:     42.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 35, 30, time.UTC).UnixMilli(),
								Value:     43.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 45, 30, time.UTC).UnixMilli(),
								Value:     44.0,
							},
						},
						ID:         metricID1,
						TimeToLive: 789,
					},
				},
			},
			requests: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       3,
					numberOfNextCall: 0,
				},
			},
		},
		{
			name: "simple-multiple-metric",
			reader: &mockStoreFixedResponse{
				response: []types.MetricData{
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 25, 30, time.UTC).UnixMilli(),
								Value:     42.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 35, 30, time.UTC).UnixMilli(),
								Value:     43.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 45, 30, time.UTC).UnixMilli(),
								Value:     44.0,
							},
						},
						ID:         metricID1,
						TimeToLive: 789,
					},
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
								Value:     1.0,
							},
						},
						ID:         metricID2,
						TimeToLive: 3600,
					},
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 25, 30, time.UTC).UnixMilli(),
								Value:     -1.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 21, 25, 30, time.UTC).UnixMilli(),
								Value:     3.14,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 22, 45, 30, time.UTC).UnixMilli(),
								Value:     44.0,
							},
						},
						ID:         metricID3,
						TimeToLive: 86400,
					},
				},
			},
			requests: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2, metricID3},
					},
					pointsRead:       7,
					numberOfNextCall: 0,
				},
			},
		},
		{
			name: "cache-multiple-points",
			reader: &mockStoreFixedResponse{
				response: []types.MetricData{
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 25, 30, time.UTC).UnixMilli(),
								Value:     42.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 35, 30, time.UTC).UnixMilli(),
								Value:     43.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 45, 30, time.UTC).UnixMilli(),
								Value:     44.0,
							},
						},
						ID:         metricID1,
						TimeToLive: 789,
					},
				},
			},
			requests: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       3,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       0,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       0,
					numberOfNextCall: 0,
				},
			},
		},
		{
			name: "cache-nil-points",
			reader: &mockStoreFixedResponse{
				response: []types.MetricData{
					{
						Points:     nil, // this shouldn't happen because Cassandra TSDB don't return empty result
						ID:         metricID1,
						TimeToLive: 789,
					},
				},
			},
			requests: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       0,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       0,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1},
					},
					pointsRead:       0,
					numberOfNextCall: 0,
				},
			},
		},
		{
			name: "uncachable-multiple-metric",
			reader: &mockStoreFixedResponse{
				response: []types.MetricData{
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 25, 30, time.UTC).UnixMilli(),
								Value:     42.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 35, 30, time.UTC).UnixMilli(),
								Value:     43.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 45, 30, time.UTC).UnixMilli(),
								Value:     44.0,
							},
						},
						ID:         metricID1,
						TimeToLive: 789,
					},
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
								Value:     1.0,
							},
						},
						ID:         metricID2,
						TimeToLive: 3600,
					},
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 25, 30, time.UTC).UnixMilli(),
								Value:     -1.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 21, 25, 30, time.UTC).UnixMilli(),
								Value:     3.14,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 22, 45, 30, time.UTC).UnixMilli(),
								Value:     44.0,
							},
						},
						ID:         metricID3,
						TimeToLive: 86400,
					},
				},
			},
			requests: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2, metricID3},
					},
					pointsRead:       7,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2, metricID3},
					},
					pointsRead:       7,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2, metricID3},
					},
					pointsRead:       7,
					numberOfNextCall: 0,
				},
			},
		},
		{
			name: "cachable-multiple-metric",
			reader: &mockStoreFixedResponse{
				response: []types.MetricData{
					{
						Points: []types.MetricPoint{
							{
								Timestamp: time.Date(2023, 5, 10, 15, 20, 25, 30, time.UTC).UnixMilli(),
								Value:     -1.0,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 21, 25, 30, time.UTC).UnixMilli(),
								Value:     3.14,
							},
							{
								Timestamp: time.Date(2023, 5, 10, 15, 22, 45, 30, time.UTC).UnixMilli(),
								Value:     44.0,
							},
						},
						ID:         metricID3,
						TimeToLive: 86400,
					},
				},
			},
			requests: []readRequest{
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2, metricID3},
					},
					pointsRead:       3,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2, metricID3},
					},
					pointsRead:       0,
					numberOfNextCall: 0,
				},
				{
					req: types.MetricRequest{
						IDs: []types.MetricID{metricID1, metricID2, metricID3},
					},
					pointsRead:       0,
					numberOfNextCall: 0,
				},
			},
		},
		{
			name:   "cache-real-store",
			reader: realStore,
			requests: []readRequest{
				{
					name: "read-all",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1, metricID2, metricID3},
						FromTimestamp: time.Date(2023, 5, 10, 11, 0, 10, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       11,
				},
				{
					name: "read-all-uncachable",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1, metricID2, metricID3},
						FromTimestamp: time.Date(2023, 5, 10, 11, 0, 10, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       11,
				},
				{
					name: "read-metric1",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1},
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 0, 30, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       4,
				},
				{
					name: "read-metric1-cachable",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1},
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 0, 30, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       0,
				},
				{
					name: "read-metric3",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID3},
						FromTimestamp: time.Date(2023, 5, 10, 11, 0, 10, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       3,
				},
				{
					name: "read-metric1-nocache",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1},
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 0, 30, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       4,
				},
				{
					name: "read-metric2-nocache",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID2},
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 0, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 0, 30, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       2,
				},
				{
					name: "read-single-response",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1, metricID2},
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 31, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 1, 0, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       2,
				},
				{
					name: "read-single-response-cache",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1, metricID2},
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 31, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 1, 0, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       0,
				},
				{
					name: "read-single-response-cache2",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1, metricID2},
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 31, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 1, 0, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       0,
				},
				{
					name: "read-single-response-cache",
					req: types.MetricRequest{
						IDs:           []types.MetricID{metricID1, metricID2},
						Function:      "function", // function is ignored with raw data
						FromTimestamp: time.Date(2023, 5, 10, 12, 0, 31, 0, time.UTC).UnixMilli(),
						ToTimestamp:   time.Date(2023, 5, 10, 12, 1, 0, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       0,
				},
				{
					name: "read-single-response-nocache",
					req: types.MetricRequest{
						IDs:                []types.MetricID{metricID1, metricID2},
						Function:           "function",
						ForcePreAggregated: true,
						FromTimestamp:      time.Date(2023, 5, 10, 12, 0, 31, 0, time.UTC).UnixMilli(),
						ToTimestamp:        time.Date(2023, 5, 10, 12, 1, 0, 0, time.UTC).UnixMilli(),
					},
					numberOfNextCall: 0,
					pointsRead:       2,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			countingReader := &limitingReader{reader: tt.reader}
			cacheReader := &cachingReader{
				reader: countingReader,
			}

			for reqIdx, req := range tt.requests {
				reqName := req.name
				if reqName == "" {
					reqName = fmt.Sprintf("#%d", reqIdx)
				}

				wantIter, err := tt.reader.ReadIter(context.Background(), req.req)
				if err != nil {
					t.Fatal(err)
				}

				var (
					want []types.MetricData
					got  []types.MetricData
				)

				if req.numberOfNextCall != -1 {
					want, err = types.MetricIterToList(wantIter, req.numberOfNextCall)
					if err != nil {
						t.Fatal(err)
					}
				}

				countBefore := countingReader.PointsRead()

				gotIter, err := cacheReader.ReadIter(context.Background(), req.req)
				if err != nil {
					t.Errorf("req %s: %v", reqName, err)

					break
				}

				if req.numberOfNextCall != -1 {
					got, err = types.MetricIterToList(gotIter, req.numberOfNextCall)
					if err != nil {
						t.Errorf("req %s: %v", reqName, err)
					}
				}

				countAfter := countingReader.PointsRead()

				pointRead := int(math.Round((countAfter - countBefore)))
				if pointRead != req.pointsRead {
					t.Errorf("req %s: points read = %d, want %d", reqName, pointRead, req.pointsRead)
				}

				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("req %s: mismatch in ReadIter() (-want +got)\n%s", reqName, diff)
				}
			}
		})
	}
}

func seriesSize(t *testing.T, workingSeries storage.Series) (int, error) {
	t.Helper()

	it := workingSeries.Iterator(nil)
	count := 0

	for it.Next() != chunkenc.ValNone {
		it.At()
		count++
	}

	if err := it.Err(); err != nil {
		return count, err
	}

	return count, nil
}

func generatePoint(fromTime time.Time, toTime time.Time, step time.Duration) []types.MetricPoint {
	v := 1.0
	points := make([]types.MetricPoint, 0)

	for ts := fromTime; ts.Before(toTime); ts = ts.Add(step) {
		v++
		points = append(points, types.MetricPoint{
			Timestamp: ts.UnixMilli(),
			Value:     v,
		})
	}

	return points
}

func Benchmark_cachingReader(b *testing.B) {
	const (
		extraMetric1 = 50001
		extraMetric2 = 50002
	)

	maxTime := time.Date(2023, 5, 10, 13, 1, 0, 0, time.UTC)
	minTime := maxTime.Add(-24 * time.Hour)

	tests := []struct {
		name           string
		skipCache      bool
		recreateReader bool
		reqs           []types.MetricRequest
	}{
		{
			name:      "small-metric-no-cache",
			skipCache: true,
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					IDs:           []types.MetricID{metricID2},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   maxTime.UnixMilli(),
				},
			},
		},
		{
			name:      "small-metric-cache",
			skipCache: false,
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					IDs:           []types.MetricID{metricID2},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   maxTime.UnixMilli(),
				},
			},
		},
		{
			name:      "large-metric-no-cache",
			skipCache: true,
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					IDs:           []types.MetricID{extraMetric1},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   maxTime.UnixMilli(),
				},
			},
		},
		{
			name: "large-metric-cache",
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					IDs:           []types.MetricID{extraMetric1},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   maxTime.UnixMilli(),
				},
			},
		},
		{
			name:           "large-metric-cache-recreate",
			recreateReader: true,
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					IDs:           []types.MetricID{extraMetric1},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   maxTime.UnixMilli(),
				},
			},
		},
		{
			name:      "multiple-metric-no-cache",
			skipCache: true,
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					IDs:           []types.MetricID{metricID2, extraMetric1},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   maxTime.UnixMilli(),
				},
			},
		},
		{
			name: "multiple-metric-uncachable",
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					IDs:           []types.MetricID{metricID2, extraMetric1},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   maxTime.UnixMilli(),
				},
			},
		},
		{
			name:      "multiple-metric-one-response-no-cache",
			skipCache: true,
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					StepMs:        300000,
					IDs:           []types.MetricID{metricID2, extraMetric1},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   minTime.Add(time.Hour).UnixMilli(),
				},
			},
		},
		{
			name: "multiple-metric-one-response-cache",
			reqs: []types.MetricRequest{
				{
					Function:      "avg_over_time",
					StepMs:        300000,
					IDs:           []types.MetricID{metricID2, extraMetric1},
					FromTimestamp: minTime.UnixMilli(),
					ToTimestamp:   minTime.Add(time.Hour).UnixMilli(),
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		b.Run(tt.name, func(b *testing.B) {
			_, realStore := createTSDB()

			_ = realStore.Write(context.Background(), []types.MetricData{
				{
					Points:     generatePoint(maxTime.Add(-24*time.Hour), maxTime, time.Minute),
					ID:         extraMetric1,
					TimeToLive: 86400 * 90,
				},
				{
					Points:     generatePoint(maxTime.Add(-24*time.Hour), maxTime, time.Minute),
					ID:         extraMetric2,
					TimeToLive: 86400 * 90,
				},
			})

			var (
				actualReader      types.MetricReader
				cachedTotalPoints float64
			)

			reader := &cachingReader{
				reader: realStore,
			}

			actualReader = realStore

			if !tt.skipCache {
				actualReader = reader
			}

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				if tt.recreateReader {
					cachedTotalPoints += reader.cachedPointsCount
					reader = &cachingReader{
						reader: realStore,
					}
					actualReader = reader
				}

				for _, req := range tt.reqs {
					iter, err := actualReader.ReadIter(context.Background(), req)
					if err != nil {
						b.Error(err)

						break
					}

					_, err = types.MetricIterToList(iter, 0)
					if err != nil {
						b.Error(err)
					}
				}
			}
			cachedTotalPoints += reader.cachedPointsCount

			b.ReportMetric(cachedTotalPoints/float64(b.N), "cache-pts/op")
		})
	}
}

// Test_cachingReader_Querier test the cachingReader through the Querier interface.
// It also test few each case with various order between Select() & Next()/At().
func Test_cachingReader_Querier(t *testing.T) { //nolint:maintidx
	type actionType int

	const (
		actionCallSelect actionType = iota
		actionClose
		actionCallNextOnSerieSet
		actionCallErrOnSerieSet
		actionInterateSerieSetAndSerie
	)

	type action struct {
		description string
		selectIdx   int
		action      actionType

		// actionCallSelect
		selectHints   *storage.SelectHints
		selectMatcher []*labels.Matcher

		// actionNext
		wantNextResult  bool
		wantPointsCount int // number of point in current Serie returned by At()

		// all actions
		pointsRead int // number of points read from the TSDB for each actions
	}

	index, tsdb := createTSDB()
	zlog := logger.NewTestLogger(true)

	tests := []struct {
		name    string
		minTime time.Time
		maxTime time.Time
		actions []action
	}{
		{
			name:    "single-select",
			minTime: time.Date(2023, 5, 10, 11, 0, 0, 0, time.UTC),
			maxTime: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC),
			actions: []action{
				{
					description: "open select",
					selectIdx:   0,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
						{Type: labels.MatchEqual, Name: "item", Value: "1"},
					},
				},
				{
					description:     "read everything",
					selectIdx:       0,
					action:          actionInterateSerieSetAndSerie,
					wantPointsCount: 4,
					pointsRead:      4,
				},
			},
		},
		{
			name:    "single-select-all-metrics",
			minTime: time.Date(2023, 5, 10, 11, 0, 0, 0, time.UTC),
			maxTime: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC),
			actions: []action{
				{
					description: "open select",
					selectIdx:   0,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description:     "read everything",
					selectIdx:       0,
					action:          actionInterateSerieSetAndSerie,
					wantPointsCount: 11,
					pointsRead:      11,
				},
			},
		},
		{
			name:    "two-same-select",
			minTime: time.Date(2023, 5, 10, 11, 0, 0, 0, time.UTC),
			maxTime: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC),
			actions: []action{
				{
					description: "open 1st select",
					selectIdx:   1,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
						{Type: labels.MatchEqual, Name: "item", Value: "1"},
					},
				},
				{
					description: "open 2nd select",
					selectIdx:   2,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
						{Type: labels.MatchEqual, Name: "item", Value: "1"},
					},
				},
				{
					description:     "read everything on 1st select",
					selectIdx:       1,
					action:          actionInterateSerieSetAndSerie,
					wantPointsCount: 4,
					pointsRead:      4,
				},

				{
					description:     "re-read everything on 2nd select",
					selectIdx:       2,
					action:          actionInterateSerieSetAndSerie,
					wantPointsCount: 4,
					pointsRead:      0,
				},
			},
		},
		{
			name:    "three-select-multi-metrics-cachable",
			minTime: time.Date(2023, 5, 10, 11, 0, 0, 0, time.UTC),
			maxTime: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC),
			actions: []action{
				{
					description: "open 1st select",
					selectIdx:   1,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description: "open 2nd select",
					selectIdx:   2,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description:     "Next() on 1st",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 2nd",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 4,
				},
				{
					description: "open 3rd select",
					selectIdx:   3,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description:     "Next() on 3rd",
					selectIdx:       3,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 4,
				},
				// Metric2
				{
					description:     "Next() on 1st (metric2)",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 2nd (metric2)",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 3rd (metric2)",
					selectIdx:       3,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 4,
				},
				// Metric3
				{
					description:     "Next() on 1st (metric3)",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      3,
					wantPointsCount: 3,
				},
				{
					description:     "Next() on 3rd (metric3)",
					selectIdx:       3,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 3,
				},
				{
					description:     "Next() on 2nd (metric3)",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 3,
				},
				// EOF
				{
					description:    "Next() on 1st (eof)",
					selectIdx:      1,
					action:         actionCallNextOnSerieSet,
					wantNextResult: false,
					pointsRead:     0,
				},
				{
					description:    "Next() on 2nd (eof)",
					selectIdx:      2,
					action:         actionCallNextOnSerieSet,
					wantNextResult: false,
					pointsRead:     0,
				},
				{
					description:    "Next() on 3rd (eof)",
					selectIdx:      3,
					action:         actionCallNextOnSerieSet,
					wantNextResult: false,
					pointsRead:     0,
				},
			},
		},
		{
			name:    "three-select-multi-metrics-different-advancement",
			minTime: time.Date(2023, 5, 10, 11, 0, 0, 0, time.UTC),
			maxTime: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC),
			actions: []action{
				{
					description: "open 1st select",
					selectIdx:   1,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description: "open 2nd select",
					selectIdx:   2,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description:     "Next() on 1st",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 2nd",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 4,
				},
				{
					description: "open 3rd select",
					selectIdx:   3,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description:     "Next() on 3rd",
					selectIdx:       3,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 4,
				},
				// Exhaust 1st select
				{
					description:     "Next() on 1st (metric2)",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 1st (metric3)",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      3,
					wantPointsCount: 3,
				},
				{
					description:    "Next() on 1st (eof)",
					selectIdx:      1,
					action:         actionCallNextOnSerieSet,
					wantNextResult: false,
					pointsRead:     0,
				},
				// Resume 2nd & 3rd
				{
					description:     "Next() on 2nd (metric2)",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 3rd (metric2)",
					selectIdx:       3,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4, // due to current implementation this isn't cachable.
					wantPointsCount: 4,
				},
				// Metric3
				{
					description:     "Next() on 2nd (metric3)",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      3,
					wantPointsCount: 3,
				},
				{
					description:     "Next() on 3rd (metric3)",
					selectIdx:       3,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      3,
					wantPointsCount: 3,
				},
				// EOF
				{
					description:    "Next() on 2nd (eof)",
					selectIdx:      2,
					action:         actionCallNextOnSerieSet,
					wantNextResult: false,
					pointsRead:     0,
				},
				{
					description:    "Next() on 3rd (eof)",
					selectIdx:      3,
					action:         actionCallNextOnSerieSet,
					wantNextResult: false,
					pointsRead:     0,
				},
			},
		},
		{
			name:    "two-select-multi-metrics-uncachable",
			minTime: time.Date(2023, 5, 10, 11, 0, 0, 0, time.UTC),
			maxTime: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC),
			actions: []action{
				{
					description: "open select",
					selectIdx:   1,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description: "open select",
					selectIdx:   2,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description:     "Next() on 1st",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 1st (metric2)",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
				{
					description:     "Next() on 2nd (uncachable, 1st select advanced too much)",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      4,
					wantPointsCount: 4,
				},
			},
		},
		{
			name:    "two-select-single-metric-present",
			minTime: time.Date(2023, 5, 10, 12, 1, 0, 0, time.UTC),
			maxTime: time.Date(2023, 5, 10, 13, 0, 10, 0, time.UTC),
			actions: []action{
				{
					description: "open select",
					selectIdx:   1,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description: "open select",
					selectIdx:   2,
					action:      actionCallSelect,
					selectMatcher: []*labels.Matcher{
						{Type: labels.MatchEqual, Name: "__name__", Value: "myname"},
					},
				},
				{
					description:     "Next() on 1st",
					selectIdx:       1,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      1,
					wantPointsCount: 1,
				},
				{
					description:    "Next() on 1st",
					selectIdx:      1,
					action:         actionCallNextOnSerieSet,
					wantNextResult: false,
					pointsRead:     0,
				},
				{
					description:     "Next() on 2nd",
					selectIdx:       2,
					action:          actionCallNextOnSerieSet,
					wantNextResult:  true,
					pointsRead:      0,
					wantPointsCount: 1,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			countingReader := &limitingReader{reader: tsdb}

			unCountedStore := NewStore(
				zlog,
				index,
				tsdb,
				"__account_id",
				false,
				0,
				0,
				prometheus.NewRegistry(),
			)

			store := NewStore(
				zlog,
				index,
				countingReader,
				"__account_id",
				false,
				0,
				0,
				prometheus.NewRegistry(),
			)

			reqCtx := types.WrapContext(context.Background(), httptest.NewRequest(http.MethodGet, "/", nil))
			querierIntf, err := store.Querier(reqCtx, tt.minTime.UnixMilli(), tt.maxTime.UnixMilli())
			if err != nil {
				t.Fatal(err)
			}

			openSelect := make(map[int]storage.SeriesSet)
			closes := make([]func() error, 0)
			validatorSelect := make(map[int]storage.SeriesSet)

			for _, action := range tt.actions {
				ptsBefore := countingReader.PointsRead()

				switch action.action {
				case actionCallSelect:
					result := querierIntf.Select(true, action.selectHints, action.selectMatcher...)
					openSelect[action.selectIdx] = result

					// Open another Querier, because cache is never shared between two Querier (it is only between Select()
					// in the same querier) we use this other Querier as validator.
					validator, err := unCountedStore.Querier(reqCtx, tt.minTime.UnixMilli(), tt.maxTime.UnixMilli())
					if err != nil {
						t.Fatal(err)
					}

					closes = append(closes, validator.Close)

					validatorSelect[action.selectIdx] = validator.Select(true, action.selectHints, action.selectMatcher...)
				case actionClose:
					err := querierIntf.Close()
					if err != nil {
						t.Errorf("%s: %v", action.description, err)
					}

					for _, f := range closes {
						if err := f(); err != nil {
							t.Errorf("%s: %v", action.description, err)
						}
					}
				case actionCallNextOnSerieSet:
					seriesSet := openSelect[action.selectIdx]
					if seriesSet == nil {
						t.Fatalf("%s: no open select at idx %d", action.description, action.selectIdx)
					}

					result := seriesSet.Next()
					if result != action.wantNextResult {
						t.Errorf("%s: Next() = %v, want %v", action.description, result, action.wantNextResult)
					}

					validator := validatorSelect[action.selectIdx]
					validResult := validator.Next()

					if result != validResult {
						t.Errorf("%s: Next() = %v, reference want %v", action.description, result, validResult)
					}

					if result {
						currentSeries := seriesSet.At()
						count, err := seriesSize(t, currentSeries)

						if count != action.wantPointsCount {
							t.Errorf("%s: point in the serie = %d, want %d", action.description, count, action.wantPointsCount)
						}

						if err != nil {
							t.Errorf("%s: %v", action.description, err)
						}

						wantSeries := validator.At()

						if err := diffSeries(t, wantSeries, currentSeries); err != nil {
							t.Errorf("%s: %v", action.description, err)
						}
					}
				case actionCallErrOnSerieSet:
					seriesSet := openSelect[action.selectIdx]
					if seriesSet == nil {
						t.Fatalf("%s: no open select at idx %d", action.description, action.selectIdx)
					}

					if err := seriesSet.Err(); err != nil {
						t.Errorf("%s: %v", action.description, err)
					}
				case actionInterateSerieSetAndSerie:
					seriesSet := openSelect[action.selectIdx]
					if seriesSet == nil {
						t.Fatalf("%s: no open select at idx %d", action.description, action.selectIdx)
					}

					totalCount := 0

					for seriesSet.Next() {
						series := seriesSet.At()

						count, err := seriesSize(t, series)
						totalCount += count

						if err != nil {
							t.Errorf("%s: %v", action.description, err)

							break
						}
					}

					if err := seriesSet.Err(); err != nil {
						t.Errorf("%s: %v", action.description, err)
					}

					if totalCount != action.wantPointsCount {
						t.Errorf("%s: point in the series = %d, want %d", action.description, totalCount, action.wantPointsCount)
					}
				default:
					t.Fatalf("%s: action not implemented", action.description)
				}

				ptsAfter := countingReader.PointsRead()

				if int(ptsAfter-ptsBefore) != action.pointsRead {
					t.Errorf("%s: read %d points, want %d", action.description, int(ptsAfter-ptsBefore), action.pointsRead)
				}
			}

			err = querierIntf.Close()
			if err != nil {
				t.Error(err)
			}
		})
	}
}

func Test_cachingReaderFromEngine(t *testing.T) {
	index, tsdb := createTSDB()

	type request struct {
		name       string
		query      string
		isInstant  bool
		start      time.Time
		end        time.Time
		step       time.Duration
		pointsRead int
	}

	tests := []request{
		{
			name:       "instant",
			isInstant:  true,
			query:      `myname{item="1"}`,
			start:      time.Date(2023, 5, 10, 12, 0, 10, 0, time.UTC),
			pointsRead: 2, // PromQL engine have 5 minutes look-back
		},
		{
			name:       "no-cache-between-promql",
			isInstant:  true,
			query:      `myname{item="1"}`,
			start:      time.Date(2023, 5, 10, 12, 0, 10, 0, time.UTC),
			pointsRead: 2, // PromQL engine have 5 minutes look-back
		},
		{
			name:       "use-cache",
			isInstant:  true,
			query:      `myname{item="1"} + myname{item="1"} + myname{item="1"}`,
			start:      time.Date(2023, 5, 10, 12, 0, 10, 0, time.UTC),
			pointsRead: 2, // PromQL engine have 5 minutes look-back
		},
		{
			name:       "max_over_time",
			isInstant:  true,
			query:      `max_over_time(myname{item="2"}[1h])`,
			start:      time.Date(2023, 5, 10, 13, 0, 0, 0, time.UTC),
			pointsRead: 4,
		},
		{
			name:       "max_over_time cache",
			isInstant:  true,
			query:      `max_over_time(myname{item="2"}[1h]) + max_over_time(myname{item="2"}[1h])`,
			start:      time.Date(2023, 5, 10, 13, 0, 0, 0, time.UTC),
			pointsRead: 4,
		},
		{
			name:       "Mix max/avg_over_time with pre-aggregate: uncachable",
			isInstant:  true,
			query:      `max_over_time(myname{item="2"}[1h:5m]) + avg_over_time(myname{item="2"}[1h:5m])`,
			start:      time.Date(2023, 5, 10, 13, 0, 0, 0, time.UTC),
			pointsRead: 8,
		},
		{
			name:       "Mix max/avg_over_time with raw data: cachable",
			isInstant:  true,
			query:      `max_over_time(myname{item="2"}[1h:10s]) + avg_over_time(myname{item="2"}[1h:10s])`,
			start:      time.Date(2023, 5, 10, 13, 0, 0, 0, time.UTC),
			pointsRead: 4,
		},
		{
			name:       "count_over_time with filter, cachable",
			isInstant:  true,
			query:      `count_over_time((myname{item="2"} > 0)[1h:60s]) / count_over_time(myname{item="2"}[1h:60s])`,
			start:      time.Date(2023, 5, 10, 13, 0, 0, 0, time.UTC),
			pointsRead: 4,
		},
	}

	for _, useThanos := range []bool{false, true} {
		useThanos := useThanos

		name := "default"
		if useThanos {
			name = "thanos"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			zlog := logger.NewTestLogger(true)

			engine := NewEngine(
				zlog,
				useThanos,
				prometheus.NewRegistry(),
			)

			countingReader := &limitingReader{reader: tsdb}

			store := NewStore(
				zlog,
				index,
				countingReader,
				"__account_id",
				false,
				0,
				0,
				prometheus.NewRegistry(),
			)

			for _, req := range tests {
				/*if useThanos || req.name != "count_over_time with filter, cachable" {
					continue
				}*/
				countBefore := countingReader.PointsRead()

				reqCtx := types.WrapContext(context.Background(), httptest.NewRequest(http.MethodGet, "/", nil))

				if req.isInstant { //nolint:nestif
					query, err := engine.NewInstantQuery(store, nil, req.query, req.start)
					if err != nil {
						t.Error(err)

						continue
					}

					result := query.Exec(reqCtx)

					query.Close()

					if result == nil {
						t.Error("result is nil")
					} else if result.Err != nil {
						t.Error(result.Err)
					}
				} else {
					query, err := engine.NewRangeQuery(store, nil, req.query, req.start, req.end, req.step)
					if err != nil {
						t.Error(err)

						continue
					}

					result := query.Exec(reqCtx)

					query.Close()

					if result == nil {
						t.Error("result is nil")
					} else if result.Err != nil {
						t.Error(result.Err)
					}
				}

				countAfter := countingReader.PointsRead()

				pointRead := int(math.Round((countAfter - countBefore)))
				if pointRead != req.pointsRead {
					t.Errorf("req %s: points read = %d, want %d", req.name, pointRead, req.pointsRead)
				}
			}
		})
	}
}

func diffSeries(t *testing.T, want storage.Series, got storage.Series) error {
	t.Helper()

	var (
		wantList []model.SamplePair
		gotList  []model.SamplePair
	)

	it := want.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		ts, value := it.At()
		wantList = append(wantList, model.SamplePair{
			Timestamp: model.TimeFromUnixNano(ts * 1e6),
			Value:     model.SampleValue(value),
		})
	}

	if err := it.Err(); err != nil {
		return err
	}

	it = got.Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		ts, value := it.At()
		gotList = append(gotList, model.SamplePair{
			Timestamp: model.TimeFromUnixNano(ts * 1e6),
			Value:     model.SampleValue(value),
		})
	}

	if err := it.Err(); err != nil {
		return err
	}

	if diff := cmp.Diff(wantList, gotList); diff != "" {
		return fmt.Errorf("series mismatch: (-want +got)\n%s", diff)
	}

	return nil
}
