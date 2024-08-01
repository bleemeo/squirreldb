package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/api/remotestorage"
	"github.com/bleemeo/squirreldb/cassandra/mutable"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/types"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

type promQlData struct {
	ResultType string            `json:"resultType"` //nolint: tagliatelle
	Result     []json.RawMessage `json:"result"`
}

type promQlResponse struct {
	Status string     `json:"status"`
	Data   promQlData `json:"data"`
}

type promQlLabels struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

func getPromQLResponse(t *testing.T, resp *http.Response) promQlResponse {
	t.Helper()

	var r promQlResponse

	err := json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		t.Fatal(err)
	}

	return r
}

func getPromQLabels(t *testing.T, resp *http.Response) promQlLabels {
	t.Helper()

	var r promQlLabels

	err := json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		t.Fatal(err)
	}

	return r
}

func TestAPIRoute(t *testing.T) { //nolint:maintidx
	t.Parallel()

	const tenantLabelName = "__account_id"

	t0 := time.Now().Add(-time.Minute)

	data := []struct {
		ID     types.MetricID
		Points []types.MetricPoint
		Labels labels.Labels
	}{
		{
			ID: 1,
			Labels: labels.FromMap(map[string]string{
				tenantLabelName: "1234",
				"__name__":      "cpu_used",
			}),
			Points: []types.MetricPoint{
				{Timestamp: t0.Unix() * 1000, Value: 11},
				{Timestamp: t0.Add(10*time.Second).Unix() * 1000, Value: 21},
			},
		},
		{
			ID: 2,
			Labels: labels.FromMap(map[string]string{
				tenantLabelName: "1234",
				"__name__":      "disk_used",
				"mountpoint":    "/home",
			}),
			Points: []types.MetricPoint{
				{Timestamp: t0.Unix() * 1000, Value: 12},
				{Timestamp: t0.Add(10*time.Second).Unix() * 1000, Value: 22},
			},
		},
		{
			ID: 3,
			Labels: labels.FromMap(map[string]string{
				tenantLabelName: "1235",
				"__name__":      "disk_used",
				"mountpoint":    "/home",
			}),
			Points: []types.MetricPoint{
				{Timestamp: t0.Unix() * 1000, Value: 13},
				{Timestamp: t0.Add(10*time.Second).Unix() * 1000, Value: 23},
			},
		},
		{
			ID: 4,
			Labels: labels.FromMap(map[string]string{
				tenantLabelName: "1236",
				"__name__":      "uptime",
				"instance":      "server:8015",
			}),
			Points: []types.MetricPoint{
				{Timestamp: t0.Unix() * 1000, Value: 14},
				{Timestamp: t0.Add(10*time.Second).Unix() * 1000, Value: 24},
			},
		},
	}

	idxData := make([]types.MetricLabel, len(data))

	for i, d := range data {
		idxData[i] = types.MetricLabel{
			ID:     d.ID,
			Labels: d.Labels,
		}
	}

	storeData := make(map[types.MetricID]types.MetricData)
	for _, d := range data {
		storeData[d.ID] = types.MetricData{
			ID:         d.ID,
			Points:     d.Points,
			TimeToLive: 86400,
		}
	}

	store := &dummy.MemoryTSDB{Data: storeData}

	// The new request factory is used to make produce new different *http.Request for tests.
	// Since we run tests cases twice, this ensure that *http.Request mutation can live between
	// the two tests.
	newReqFactory := func(method string, url string, body []byte, headers map[string]string) func() *http.Request {
		return func() *http.Request {
			var reader io.Reader

			if body != nil {
				reader = bytes.NewReader(body)
			}

			req := httptest.NewRequest(method, url, reader)

			for k, v := range headers {
				req.Header.Add(k, v)
			}

			return req
		}
	}

	urlWithParam := func(input string, params map[string]string) string {
		u, err := url.Parse(input)
		if err != nil {
			panic(err)
		}

		qs := u.Query()

		for k, v := range params {
			qs.Add(k, v)
		}

		u.RawQuery = qs.Encode()

		return u.String()
	}

	cases := []struct {
		name                string
		requireTenantHeader bool
		makeRequest         func() *http.Request
		validateResponse    func(t *testing.T, resp *http.Response)
	}{
		{
			name:                "readiness",
			requireTenantHeader: false,
			makeRequest:         newReqFactory("GET", "http://localhost:9201/ready", nil, nil),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}
			},
		},
		{
			name:                "readiness2",
			requireTenantHeader: false,
			makeRequest:         newReqFactory("GET", "/ready", nil, nil),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}
			},
		},
		{
			name:                "promql-query",
			requireTenantHeader: false,
			makeRequest: newReqFactory(
				"GET", urlWithParam("/api/v1/query", map[string]string{"query": "disk_used"}), nil, nil,
			),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}

				r := getPromQLResponse(t, resp)
				if len(r.Data.Result) != 2 {
					t.Errorf("len(Data.Result) = %d, want 2", len(r.Data.Result))
				}
			},
		},
		{
			name:                "promql-query-forced-matcher",
			requireTenantHeader: false,
			makeRequest: newReqFactory(
				"GET",
				urlWithParam("/api/v1/query", map[string]string{"query": "disk_used"}),
				nil,
				map[string]string{types.HeaderForcedMatcher: "__account_id=1234"},
			),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}

				r := getPromQLResponse(t, resp)
				if len(r.Data.Result) != 1 {
					t.Errorf("len(Data.Result) = %d, want 1", len(r.Data.Result))
				}
			},
		},
		{
			name:                "labels_values",
			requireTenantHeader: false,
			makeRequest:         newReqFactory("GET", "/api/v1/label/__name__/values", nil, nil),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}

				r := getPromQLabels(t, resp)
				if len(r.Data) != 3 {
					t.Errorf("len(Data) = %d, want 3", len(r.Data))
				}
			},
		},
		{
			name:                "promql-query-forced-matcher-2",
			requireTenantHeader: false,
			makeRequest: newReqFactory(
				"GET",
				"/api/v1/label/__name__/values",
				nil,
				map[string]string{types.HeaderForcedMatcher: "__account_id=1236"},
			),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}

				r := getPromQLabels(t, resp)
				if len(r.Data) != 1 {
					t.Errorf("len(Data) = %d, want 1", len(r.Data))
				}
			},
		},
		{
			name:                "promql-query-missing-tenant-header",
			requireTenantHeader: true,
			makeRequest: newReqFactory(
				"GET",
				urlWithParam("/api/v1/query", map[string]string{"query": "disk_used"}),
				nil,
				nil,
			),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusUnprocessableEntity {
					t.Errorf("StatusCode = %d, want 422", resp.StatusCode)
				}
			},
		},
		{
			name:                "promql-query-with-tenant-header",
			requireTenantHeader: true,
			makeRequest: newReqFactory(
				"GET",
				urlWithParam("/api/v1/query", map[string]string{"query": "disk_used"}),
				nil,
				map[string]string{types.HeaderTenant: "1235"}),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}

				r := getPromQLResponse(t, resp)
				if len(r.Data.Result) != 1 {
					t.Errorf("len(Data.Result) = %d, want 1", len(r.Data.Result))
				}
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := API{
				Index:               dummy.NewIndex(idxData),
				Reader:              store,
				Writer:              store,
				RequireTenantHeader: tt.requireTenantHeader,
				TenantLabelName:     tenantLabelName,
			}
			api.init()

			// First validate that all check fail when not ready.
			req := tt.makeRequest()
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			resp := w.Result()

			defer resp.Body.Close()

			want := http.StatusServiceUnavailable

			if resp.StatusCode != want {
				t.Errorf("StatusCode = %d, want %d", resp.StatusCode, want)
			}

			// Check that all check pass when the api is ready.
			api.Ready()

			req = tt.makeRequest()
			w = httptest.NewRecorder()
			api.ServeHTTP(w, req)
			resp = w.Result()

			defer resp.Body.Close()

			tt.validateResponse(t, resp)
		})
	}
}

func TestWriteHandler(t *testing.T) {
	t.Parallel()

	const (
		tenantLabelName = "__account_id"
		tenantValue     = "1234"
	)

	tests := []struct {
		name                 string
		labels               []prompb.Label
		skipTenantHeader     bool
		expectStatus         int
		expectedMetricsCount int
		absentMatchers       []*labels.Matcher
	}{
		// Test that when we try to write an invalid label or metric name we get an HTTP 400 status code.
		// We rely on Prometheus returning a 500 error by default that we convert to a 400, but this behaviour
		// could be changed in the future.
		// See ServeHTTP function in https://github.com/prometheus/prometheus/blob/main/storage/remote/write_handler.go.
		{
			name: "invalid-metric-name",
			labels: []prompb.Label{
				{Name: tenantLabelName, Value: tenantValue},
				{Name: "__name__", Value: "na-me"},
			},
			expectStatus:         http.StatusOK,
			expectedMetricsCount: 0,
		},
		{
			name: "invalid-label-name",
			labels: []prompb.Label{
				{Name: tenantLabelName, Value: tenantValue},
				{Name: "la-bel", Value: "val"},
			},
			expectStatus:         http.StatusOK,
			expectedMetricsCount: 0,
		},
		{
			name: "missing-tenant-header",
			labels: []prompb.Label{
				{Name: "label", Value: "value"},
			},
			expectStatus:         http.StatusBadRequest,
			expectedMetricsCount: 0,
			skipTenantHeader:     true,
		},
		// Mutable labels should be removed when writing.
		{
			name: "invalid-mutable-label",
			labels: []prompb.Label{
				{Name: tenantLabelName, Value: tenantValue},
				{Name: "group", Value: "my_group"},
			},
			expectStatus:         http.StatusOK,
			expectedMetricsCount: 1,
			absentMatchers: []*labels.Matcher{
				{Name: "group", Value: "my_group"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := dummy.NewMutableLabelStore(dummy.MutableLabels{
				AssociatedNames: map[string]map[string]string{
					tenantValue: {
						"group": "instance",
					},
				},
			})

			provider := mutable.NewProvider(context.Background(),
				prometheus.NewRegistry(),
				&dummy.LocalCluster{},
				store,
				logger.NewTestLogger(true),
			)
			labelProcessor := mutable.NewLabelProcessor(provider, tenantLabelName)

			dummyWriter := new(dummy.MemoryTSDB)
			dummyIndex := &dummy.Index{
				StoreMetricIDInMemory: true,
			}
			reg := prometheus.NewRegistry()
			appendable := remotestorage.New(
				dummyWriter,
				dummyIndex,
				1,
				tenantLabelName,
				labelProcessor,
				true,
				reg,
			)
			writeHandler := remote.NewWriteHandler(log.NewLogfmtLogger(os.Stderr), reg, appendable)

			now := time.Now()
			wr := &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: test.labels,
						Samples: []prompb.Sample{
							{
								Value:     10,
								Timestamp: now.UnixMilli(),
							},
						},
					},
				},
			}

			data, err := proto.Marshal(wr)
			if err != nil {
				panic(err)
			}

			encoded := snappy.Encode(nil, data)
			body := bytes.NewReader(encoded)

			req, err := http.NewRequest(http.MethodPost, "http://localhost:9201/write", body) //nolint: noctx
			if err != nil {
				t.Fatal(err)
			}

			req = req.WithContext(types.WrapContext(context.Background(), req))

			if !test.skipTenantHeader {
				req.Header.Add(types.HeaderTenant, tenantValue)
			}

			recorder := httptest.NewRecorder()
			irw := &interceptor{OrigWriter: recorder}
			writeHandler.ServeHTTP(irw, req)

			resp := recorder.Result()
			defer resp.Body.Close()

			if resp.StatusCode != test.expectStatus {
				t.Fatalf("wanted status %d, got %v", test.expectStatus, resp.StatusCode)
			}

			if len(dummyWriter.Data) != test.expectedMetricsCount {
				t.Fatalf("wanted %d metrics written, got %d", test.expectedMetricsCount, len(dummyWriter.Data))
			}

			if len(test.absentMatchers) > 0 {
				metrics, err := dummyIndex.Search(context.Background(), now, now, test.absentMatchers)
				if err != nil {
					t.Fatal(err)
				}

				if metrics.Count() > 0 {
					t.Fatalf("%d metrics matched %s", metrics.Count(), test.absentMatchers)
				}
			}
		})
	}
}

func TestPromQLInstantQuery(t *testing.T) { //nolint:maintidx
	t0 := time.Date(2023, 10, 2, 13, 46, 18, 0, time.UTC)

	tests := []struct {
		name        string
		promql      string
		storeData   []metricLabelsPoints
		time        time.Time
		wantSamples []*model.Sample
	}{
		{
			name:   "simple",
			promql: "cpu_used",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "cpu_used",
					}),
					points: []types.MetricPoint{
						{Timestamp: t0.UnixMilli(), Value: 12},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{"__name__": "cpu_used"},
					Value:     12,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "multiple",
			promql: "cpu_used",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "cpu_used",
						"instance": "server1:8015",
					}),
					points: []types.MetricPoint{
						{Timestamp: (t0.Add(-time.Minute)).UnixMilli(), Value: 8},
						{Timestamp: t0.UnixMilli(), Value: 10},
					},
				},
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "cpu_used",
						"instance": "server2:8015",
					}),
					points: []types.MetricPoint{
						{Timestamp: (t0.Add(-time.Minute)).UnixMilli(), Value: 9},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{"__name__": "cpu_used", "instance": "server1:8015"},
					Value:     10,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
				{
					Metric:    model.Metric{"__name__": "cpu_used", "instance": "server2:8015"},
					Value:     9,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_multiple",
			promql: "sum(cpu_used)",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "cpu_used",
						"instance": "server1:8015",
					}),
					points: []types.MetricPoint{
						{Timestamp: (t0.Add(-time.Minute)).UnixMilli(), Value: 8},
						{Timestamp: t0.UnixMilli(), Value: 10},
					},
				},
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "cpu_used",
						"instance": "server2:8015",
					}),
					points: []types.MetricPoint{
						{Timestamp: (t0.Add(-time.Minute)).UnixMilli(), Value: 9},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{},
					Value:     19,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_time",
			promql: "rate(request_total[1m])",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "request_total",
					}),
					points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: (t0.Add(-50 * time.Second)).UnixMilli(), Value: 0},
						{Timestamp: (t0.Add(-40 * time.Second)).UnixMilli(), Value: 200},
						{Timestamp: (t0.Add(-30 * time.Second)).UnixMilli(), Value: 400},
						{Timestamp: (t0.Add(-20 * time.Second)).UnixMilli(), Value: 600},
						{Timestamp: (t0.Add(-10 * time.Second)).UnixMilli(), Value: 800},
						{Timestamp: t0.UnixMilli(), Value: 1000},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{},
					Value:     16.66666666,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_time_few_data_on_big_range",
			promql: "rate(request_total[2m])",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "request_total",
					}),
					points: []types.MetricPoint{
						{Timestamp: (t0.Add(-70 * time.Second)).UnixMilli(), Value: 500},
						{Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 510},
						{Timestamp: (t0.Add(-50 * time.Second)).UnixMilli(), Value: 520},
						{Timestamp: (t0.Add(-40 * time.Second)).UnixMilli(), Value: 530},
						{Timestamp: (t0.Add(-30 * time.Second)).UnixMilli(), Value: 540},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{},
					// Prometheus does some extrapolation:
					// * make as if the value continued to grow at the same rate near first/last timestamp.
					//   But up to half the resolution (unless close enough to query range boundary).
					//   In this example it assumes that value continued to change between t0-75s and t0-25s.
					// * outside this extrapolated range, it assumes value didn't changed
					// * do the rate over the whole query range of 120s
					// So in our case:
					// * the measured rate is 1.0 (increase of 40 over 40 seconds)
					// * it extrapolates the value of 495 at t0-75s and 545 at t0-25s
					// This give a increase of 50 over a period of 120s -> 0.41666
					Value:     0.41666,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_time_not_rounded",
			promql: "rate(request_total[1m])",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "request_total",
					}),
					points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: (t0.Add(-50 * time.Second).Add(123 * time.Millisecond)).UnixMilli(), Value: 0},
						{Timestamp: (t0.Add(-40 * time.Second).Add(123 * time.Millisecond)).UnixMilli(), Value: 200},
						{Timestamp: (t0.Add(-30 * time.Second).Add(124 * time.Millisecond)).UnixMilli(), Value: 400},
						{Timestamp: (t0.Add(-20 * time.Second).Add(122 * time.Millisecond)).UnixMilli(), Value: 600},
						{Timestamp: (t0.Add(-10 * time.Second).Add(123 * time.Millisecond)).UnixMilli(), Value: 800},
						{Timestamp: t0.Add(123 * time.Millisecond).UnixMilli(), Value: 1000},
					},
				},
			},
			time: t0.Add(257 * time.Millisecond),
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{},
					Value:     16.711, // I'm ensure if that should be a bug or not. The target value is 1000 / 60s -> 16.666
					Timestamp: model.TimeFromUnixNano(t0.Add(257 * time.Millisecond).UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_time_less_stable_value",
			promql: "rate(request_total[1m])",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "request_total",
					}),
					points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: (t0.Add(-50 * time.Second)).UnixMilli(), Value: 0},
						{Timestamp: (t0.Add(-40 * time.Second)).UnixMilli(), Value: 207},
						{Timestamp: (t0.Add(-30 * time.Second)).UnixMilli(), Value: 410},
						{Timestamp: (t0.Add(-20 * time.Second)).UnixMilli(), Value: 580},
						{Timestamp: (t0.Add(-10 * time.Second)).UnixMilli(), Value: 800},
						{Timestamp: t0.UnixMilli(), Value: 1000},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{},
					Value:     16.6666,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_time_missing_last_point",
			promql: "rate(request_total[1m])",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "request_total",
					}),
					points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: (t0.Add(-50 * time.Second)).UnixMilli(), Value: 0},
						{Timestamp: (t0.Add(-40 * time.Second)).UnixMilli(), Value: 207},
						{Timestamp: (t0.Add(-30 * time.Second)).UnixMilli(), Value: 410},
						{Timestamp: (t0.Add(-20 * time.Second)).UnixMilli(), Value: 580},
						{Timestamp: (t0.Add(-10 * time.Second)).UnixMilli(), Value: 800},
						// {Timestamp: t0.UnixMilli(), Value: 1000},  <-- missing point due to ingestion latency
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{},
					Value:     16.666,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_time_actual_bug",
			promql: "rate(squirreldb_requests_points_total[1m])",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "squirreldb_requests_points_total",
					}),
					points: []types.MetricPoint{
						{Timestamp: 1696257630000, Value: 2.524386e+06},
						{Timestamp: 1696257640000, Value: 2.525197e+06},
						{Timestamp: 1696257650000, Value: 2.525945e+06},
						{Timestamp: 1696257660000, Value: 2.526637e+06},
						{Timestamp: 1696257670000, Value: 2.527391e+06},
					},
				},
			},
			time: time.UnixMilli(1696257685488),
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{},
					// This is were we hit the limitation of PromQL extrapolation. We want the value of 75.125,
					// but because last point is too far away from query range end time (it's
					// 15.488 seconds before end of query range, when the limit is 11 seconds).
					Value:     61.99315,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
		{
			// If the same query as aggregate_time_actual_bug is re-done a bit later when
			// last point had time to be ingested, the result will be the expected one.
			name:   "aggregate_time_actual_bug_re_query",
			promql: "rate(squirreldb_requests_points_total[1m])",
			storeData: []metricLabelsPoints{
				{
					labels: labels.FromMap(map[string]string{
						"__name__": "squirreldb_requests_points_total",
					}),
					points: []types.MetricPoint{
						{Timestamp: 1696257630000, Value: 2.524386e+06},
						{Timestamp: 1696257640000, Value: 2.525197e+06},
						{Timestamp: 1696257650000, Value: 2.525945e+06},
						{Timestamp: 1696257660000, Value: 2.526637e+06},
						{Timestamp: 1696257670000, Value: 2.527391e+06},
						{Timestamp: 1696257680000, Value: 2.528114e+06},
					},
				},
			},
			time: time.UnixMilli(1696257685488),
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{},
					Value:     74.56,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
	}

	for _, tt := range tests {
		if tt.name != "aggregate_time_actual_bug" && false {
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idx, store := makeIdxAndStore(tt.storeData)
			squirrelDBAPI := &API{
				Logger:                  logger.NewTestLogger(true),
				Index:                   idx,
				Reader:                  store,
				MetricRegistry:          prometheus.NewRegistry(),
				Writer:                  dummy.DiscardTSDB{},
				MaxRequestBodySizeBytes: 1024,
			}
			squirrelDBAPI.init()
			squirrelDBAPI.Ready()

			client, err := api.NewClient(api.Config{
				RoundTripper: roundTripperFromHTTPHandler{handler: squirrelDBAPI},
			})
			if err != nil {
				t.Fatal(err)
			}

			apiClient := v1.NewAPI(client)

			response, warning, err := apiClient.Query(context.Background(), tt.promql, tt.time)
			if err != nil {
				t.Fatal(err)
			}

			if warning != nil {
				t.Fatal(warning)
			}

			if response.Type() != model.ValVector {
				t.Fatalf("this test only work with ValVector response, got %v", response.Type())
			}

			valueVector := []*model.Sample(response.(model.Vector)) //nolint:forcetypeassert // checked just above

			sort.Sort(model.Samples(valueVector))
			sort.Sort(model.Samples(tt.wantSamples))

			// This is cmpopts.EquateApprox rewrite for model.SampleValue
			approx := cmp.FilterValues(
				func(x, y model.SampleValue) bool {
					return !math.IsNaN(float64(x)) &&
						!math.IsNaN(float64(y)) &&
						!math.IsInf(float64(x), 0) &&
						!math.IsInf(float64(y), 0)
				},
				cmp.Comparer(func(x, y model.SampleValue) bool {
					const (
						frac = 0.001
						marg = 0
					)

					relMarg := frac * math.Min(math.Abs(float64(x)), math.Abs(float64(y)))

					return math.Abs(float64(x-y)) <= math.Max(marg, relMarg)
				}),
			)

			// we convert []*Sample to []Sample, or else cmp.Diff don't use our approx function on
			// SampleValue inside the pointer.
			if diff := cmp.Diff(samplesPointerToValue(tt.wantSamples), samplesPointerToValue(valueVector), approx); diff != "" {
				t.Errorf("result mismatch (-want +got)\n%s", diff)
			}
		})
	}
}

type roundTripperFromHTTPHandler struct {
	handler http.Handler
}

func (rt roundTripperFromHTTPHandler) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	rt.handler.ServeHTTP(rec, req)

	return rec.Result(), nil
}

type metricLabelsPoints struct {
	labels labels.Labels
	points []types.MetricPoint
}

func makeIdxAndStore(data []metricLabelsPoints) (*dummy.Index, *dummy.MemoryTSDB) {
	metrics := make([]types.MetricLabel, len(data))
	store := &dummy.MemoryTSDB{Data: make(map[types.MetricID]types.MetricData)}

	for idx, m := range data {
		metrics[idx] = types.MetricLabel{
			Labels: m.labels,
			ID:     types.MetricID(idx),
		}
		store.Data[types.MetricID(idx)] = types.MetricData{
			ID:         types.MetricID(idx),
			Points:     m.points,
			TimeToLive: 86400,
		}
	}

	idx := dummy.NewIndex(metrics)

	return idx, store
}

func samplesPointerToValue(in []*model.Sample) []model.Sample {
	result := make([]model.Sample, len(in))

	for i, v := range in {
		result[i] = *v
	}

	return result
}
