// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/api/requestscoped"
	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/mutable"
	"github.com/bleemeo/squirreldb/tests/common/testcases"
	"github.com/bleemeo/squirreldb/types"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/rs/zerolog/log"
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

	const (
		tenantLabelName = "__account_id"
	)

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
				labelName:       metricCPUUsed,
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
				labelName:       metricDiskUsed,
				"mountpoint":    testcases.DiskMount1,
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
				labelName:       metricDiskUsed,
				"mountpoint":    testcases.DiskMount1,
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
				labelName:       "uptime",
				labelInstance:   "server:8015",
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

	storeData := make([]types.Timeseries, len(data))
	for i, d := range data {
		storeData[i] = types.Timeseries{
			Labels: d.Labels,
			Points: d.Points,
		}
	}

	store := &dummy.MemoryTSDB{}
	store.AddData(storeData, 86400)

	// The new request factory is used to make produce new different *http.Request for tests.
	// Since we run tests cases twice, this ensure that *http.Request mutation can live between
	// the two tests.
	newReqFactory := func(method string, url string, body []byte, headers map[string]string) func() *http.Request {
		return func() *http.Request {
			var reader io.Reader

			if body != nil {
				reader = bytes.NewReader(body)
			}

			req := httptest.NewRequestWithContext(context.Background(), method, url, reader)

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
				"GET", urlWithParam("/api/v1/query", map[string]string{"query": metricDiskUsed}), nil, nil,
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
				urlWithParam("/api/v1/query", map[string]string{"query": metricDiskUsed}),
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
				urlWithParam("/api/v1/query", map[string]string{"query": metricDiskUsed}),
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
				urlWithParam("/api/v1/query", map[string]string{"query": metricDiskUsed}),
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
				Queryable:           store,
				Appendable:          store,
				RequireTenantHeader: tt.requireTenantHeader,
				TenantLabelName:     tenantLabelName,
			}

			if err := api.init(t.Context()); err != nil {
				t.Fatal(err)
			}

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
				{Name: labelName, Value: "\xC0"}, // invalid UTF-8
			},
			expectStatus:         http.StatusNoContent,
			expectedMetricsCount: 0,
		},
		{
			name: "invalid-label-name",
			labels: []prompb.Label{
				{Name: tenantLabelName, Value: tenantValue},
				{Name: labelName, Value: "name"},
				{Name: "label", Value: "\xC0"}, // invalid UTF-8
			},
			expectStatus:         http.StatusNoContent,
			expectedMetricsCount: 0,
		},
		{
			name: "missing-tenant-header",
			labels: []prompb.Label{
				{Name: labelName, Value: "name"},
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
				{Name: labelName, Value: "name"},
				{Name: "group", Value: "my_group"},
			},
			expectStatus:         http.StatusNoContent,
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
						"group": labelInstance,
					},
				},
			})

			provider := mutable.NewProvider(t.Context(),
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
			requestScopedStore := requestscoped.NewStore(requestscoped.Options{
				DefaultMaxEvaluatedSeries: 1024,
				DefaultMaxEvaluatedPoints: 1024,
				DefaultTTL:                86400,
				TenantLabelName:           tenantLabelName,
				RequireTenantHeader:       true,
				Queryable:                 nil,
				Appendable:                dummyWriter,
				MutableLabelDetector:      labelProcessor,
			})

			sLogger := slog.New(slog.NewTextHandler(logger.TestWriter(t), nil))
			writeHandler := remote.NewWriteHandler(sLogger, reg, requestScopedStore, allowedProtoMsgs, false)

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

			if !test.skipTenantHeader {
				req.Header.Add(types.HeaderTenant, tenantValue)
			}

			ctx, _, err := requestScopedStore.ContextWithRequest(req.Context(), req, "write")
			if err != nil {
				t.Fatal(err)
			}

			req = req.WithContext(ctx)

			recorder := httptest.NewRecorder()
			irw := &interceptor{OrigWriter: recorder}
			writeHandler.ServeHTTP(irw, req)

			resp := recorder.Result()
			defer resp.Body.Close()

			if resp.StatusCode != test.expectStatus {
				t.Fatalf("wanted status %d, got %v", test.expectStatus, resp.StatusCode)
			}

			if dummyWriter.CountMetrics() != test.expectedMetricsCount {
				t.Fatalf("wanted %d metrics written, got %d", test.expectedMetricsCount, dummyWriter.CountMetrics())
			}

			if len(test.absentMatchers) > 0 {
				metrics, err := dummyIndex.Search(t.Context(), now, now, test.absentMatchers)
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

func timeSeriesToPromPBTimeseries(metric types.Timeseries) prompb.TimeSeries {
	samples := make([]prompb.Sample, 0, len(metric.Points))

	for _, pt := range metric.Points {
		samples = append(samples, prompb.Sample{
			Value:     pt.Value,
			Timestamp: pt.Timestamp,
		})
	}

	return prompb.TimeSeries{
		Labels:  prompb.FromLabels(metric.Labels, nil),
		Samples: samples,
	}
}

func metricsToWriteRequest(data []types.Timeseries) *prompb.WriteRequest {
	series := make([]prompb.TimeSeries, 0, len(data))

	for _, metric := range data {
		series = append(series, timeSeriesToPromPBTimeseries(metric))
	}

	return &prompb.WriteRequest{
		Timeseries: series,
	}
}

func writeRequestToBody(wr *prompb.WriteRequest) (*bytes.Reader, error) {
	data, err := proto.Marshal(wr)
	if err != nil {
		return nil, err
	}

	encoded := snappy.Encode(nil, data)
	body := bytes.NewReader(encoded)

	return body, nil
}

// TestWriteHandlerOfAPI test the "full" API (using normal init() method) and with actual datapoints.
func TestWriteHandlerOfAPI(t *testing.T) {
	t.Parallel()

	const (
		tenantLabelName = "__account_id"
		tenantValue     = "1234"
	)

	tests := []struct {
		name          string
		writtenPoints []types.Timeseries
	}{
		{
			name: "one-points",
			writtenPoints: []types.Timeseries{
				{
					Points: []types.MetricPoint{
						{
							Timestamp: time.Now().UnixMilli(),
							Value:     42.1,
						},
					},
					Labels: labels.FromMap(map[string]string{
						labelName: "single_metrics",
					}),
				},
			},
		},
		{
			name: "one-points-future",
			writtenPoints: []types.Timeseries{
				{
					Points: []types.MetricPoint{
						{
							Timestamp: time.Now().Add(8 * time.Hour).UnixMilli(),
							Value:     42.1,
						},
					},
					Labels: labels.FromMap(map[string]string{
						labelName: "single_metrics",
					}),
				},
			},
		},
		{
			name: "three-points-past-present-and-future",
			writtenPoints: []types.Timeseries{
				{
					Points: []types.MetricPoint{
						{
							Timestamp: time.Now().Add(8 * time.Hour).UnixMilli(),
							Value:     42.1,
						},
					},
					Labels: labels.FromMap(map[string]string{
						labelName: "metric1_future",
					}),
				},
				{
					Points: []types.MetricPoint{
						{
							Timestamp: time.Now().UnixMilli(),
							Value:     42.1,
						},
					},
					Labels: labels.FromMap(map[string]string{
						labelName: "metric2_present",
					}),
				},
				{
					Points: []types.MetricPoint{
						{
							Timestamp: time.Now().Add(-1 * time.Hour).UnixMilli(),
							Value:     42.1,
						},
					},
					Labels: labels.FromMap(map[string]string{
						labelName: "metric2_past",
					}),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := dummy.NewMutableLabelStore(dummy.MutableLabels{})

			provider := mutable.NewProvider(t.Context(),
				prometheus.NewRegistry(),
				&dummy.LocalCluster{},
				store,
				logger.NewTestLogger(true),
			)
			labelProcessor := mutable.NewLabelProcessor(provider, tenantLabelName)

			dummyWriter := new(dummy.MemoryTSDB)

			// Try to be as close as daemon.go apiTask
			api := makeAPI(dummyWriter, labelProcessor)

			if err := api.init(t.Context()); err != nil {
				t.Fatal(err)
			}

			api.Ready()

			body, err := writeRequestToBody(metricsToWriteRequest(test.writtenPoints))
			if err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequest(http.MethodPost, "http://localhost:9201/api/v1/write", body) //nolint: noctx
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()
			api.ServeHTTP(recorder, req)

			resp := recorder.Result()
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusNoContent {
				t.Errorf("response is: %s", recorder.Body.String())
				t.Fatalf("StatusCode = %v, want %v", resp.StatusCode, http.StatusNoContent)
			}

			metricsInStore := dummyWriter.Data()

			lessFunc := func(a, b types.Timeseries) bool {
				return labels.Compare(a.Labels, b.Labels) < 0
			}

			if diff := cmp.Diff(
				test.writtenPoints, metricsInStore, cmpopts.SortSlices(lessFunc),
				cmp.Comparer(labels.Equal)); diff != "" {
				t.Errorf("MetricsFromStore mismatch (-want +got)\n%s", diff)
			}
		})
	}
}

// TestAppenderInvalidRequest tests that the appender still works after a lot of invalid requests.
// It ensures that the remoteWriteGate is correctly released with invalid inputs.
func TestAppenderInvalidRequest(t *testing.T) {
	t.Parallel()

	const maxConcurrentWrite = 5

	api := makeAPI(dummy.DiscardTSDB{}, nil)
	api.MaxConcurrentRemoteRequests = maxConcurrentWrite
	api.RequireTenantHeader = true

	if err := api.init(t.Context()); err != nil {
		t.Fatal(err)
	}

	api.Ready()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	metrics1 := labels.FromStrings("__name__", "metric1")

	// Send some requests with a missing tenant header.
	for range 2 * maxConcurrentWrite {
		body, err := writeRequestToBody(metricsToWriteRequest(
			[]types.Timeseries{{Labels: metrics1, Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}}},
		))
		if err != nil {
			t.Fatal(err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:9201/api/v1/write", body)
		if err != nil {
			t.Fatal(err)
		}

		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, req)

		resp := recorder.Result()
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("response is: %s", recorder.Body.String())
			t.Fatalf("StatusCode = %v, want %v", resp.StatusCode, http.StatusBadRequest)
		}

		gotBody, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(string(gotBody), requestscoped.ErrMissingTenantHeader.Error()) {
			t.Errorf("body = %q, want contains %q", gotBody, requestscoped.ErrMissingTenantHeader.Error())
		}
	}

	// Send some requests with an invalid TTL header.
	for range 2 * maxConcurrentWrite {
		body, err := writeRequestToBody(metricsToWriteRequest(
			[]types.Timeseries{{Labels: metrics1, Points: []types.MetricPoint{{Timestamp: time.Now().UnixMilli(), Value: 42}}}},
		))
		if err != nil {
			t.Fatal(err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:9201/api/v1/write", body)
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set(types.HeaderTenant, "my_tenant")
		req.Header.Set(types.HeaderTimeToLive, "invalid_ttl")

		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, req)

		resp := recorder.Result()
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Errorf("response is: %s", recorder.Body.String())
			t.Fatalf("StatusCode = %v, want %v", resp.StatusCode, http.StatusUnprocessableEntity)
		}

		gotBody, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(string(gotBody), requestscoped.ErrParseTTLHeader.Error()) {
			t.Errorf("body = %q, want contains %q", gotBody, requestscoped.ErrParseTTLHeader.Error())
		}
	}
}

func TestPromQLInstantQuery(t *testing.T) { //nolint:maintidx
	t0 := time.Date(2023, 10, 2, 13, 46, 18, 0, time.UTC)

	metricsConstant := testcases.GetConstant()
	labelsMetric1 := metricsConstant.LabelsMetric1
	labelsMetric2 := metricsConstant.LabelsMetric2
	labelsMetric3 := metricsConstant.LabelsMetric3

	defaultStoreData := []types.Timeseries{
		{
			Labels: labelsMetric1,
			Points: generatePoints(1696257680000, 10000, 100, 4.2e+09),
		},
		{
			Labels: labelsMetric2,
			Points: generatePoints(1696257680000, 10000, 100, 42e+09),
		},
		{
			Labels: labelsMetric3,
			Points: generatePoints(1696257680000, 10000, 100, 4e+02),
		},
	}

	type options struct {
		MaxEvaluatedPoints uint64
		MaxEvaluatedSeries uint32
	}

	tests := []struct {
		name        string
		promql      string
		reqHeader   map[string]string
		apiOption   options
		storeData   []types.Timeseries
		time        time.Time
		wantErr     bool
		wantSamples []*model.Sample
	}{
		{
			name:   "simple",
			promql: metricCPUUsed,
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: metricCPUUsed,
					}),
					Points: []types.MetricPoint{
						{Timestamp: t0.UnixMilli(), Value: 12},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{labelName: metricCPUUsed},
					Value:     12,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "multiple",
			promql: metricCPUUsed,
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName:     metricCPUUsed,
						labelInstance: "server1:8015",
					}),
					Points: []types.MetricPoint{
						{Timestamp: t0.Add(-time.Minute).UnixMilli(), Value: 8},
						{Timestamp: t0.UnixMilli(), Value: 10},
					},
				},
				{
					Labels: labels.FromMap(map[string]string{
						labelName:     metricCPUUsed,
						labelInstance: "server2:8015",
					}),
					Points: []types.MetricPoint{
						{Timestamp: t0.Add(-time.Minute).UnixMilli(), Value: 9},
					},
				},
			},
			time: t0,
			wantSamples: []*model.Sample{
				{
					Metric:    model.Metric{labelName: metricCPUUsed, labelInstance: "server1:8015"},
					Value:     10,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
				{
					Metric:    model.Metric{labelName: metricCPUUsed, labelInstance: "server2:8015"},
					Value:     9,
					Timestamp: model.TimeFromUnixNano(t0.UnixNano()),
				},
			},
		},
		{
			name:   "aggregate_multiple",
			promql: "sum(cpu_used)",
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName:     metricCPUUsed,
						labelInstance: "server1:8015",
					}),
					Points: []types.MetricPoint{
						{Timestamp: t0.Add(-time.Minute).UnixMilli(), Value: 8},
						{Timestamp: t0.UnixMilli(), Value: 10},
					},
				},
				{
					Labels: labels.FromMap(map[string]string{
						labelName:     metricCPUUsed,
						labelInstance: "server2:8015",
					}),
					Points: []types.MetricPoint{
						{Timestamp: t0.Add(-time.Minute).UnixMilli(), Value: 9},
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
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: "request_total",
					}),
					Points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: t0.Add(-50 * time.Second).UnixMilli(), Value: 0},
						{Timestamp: t0.Add(-40 * time.Second).UnixMilli(), Value: 200},
						{Timestamp: t0.Add(-30 * time.Second).UnixMilli(), Value: 400},
						{Timestamp: t0.Add(-20 * time.Second).UnixMilli(), Value: 600},
						{Timestamp: t0.Add(-10 * time.Second).UnixMilli(), Value: 800},
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
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: "request_total",
					}),
					Points: []types.MetricPoint{
						{Timestamp: t0.Add(-70 * time.Second).UnixMilli(), Value: 500},
						{Timestamp: t0.Add(-60 * time.Second).UnixMilli(), Value: 510},
						{Timestamp: t0.Add(-50 * time.Second).UnixMilli(), Value: 520},
						{Timestamp: t0.Add(-40 * time.Second).UnixMilli(), Value: 530},
						{Timestamp: t0.Add(-30 * time.Second).UnixMilli(), Value: 540},
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
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: "request_total",
					}),
					Points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: t0.Add(-50 * time.Second).Add(123 * time.Millisecond).UnixMilli(), Value: 0},
						{Timestamp: t0.Add(-40 * time.Second).Add(123 * time.Millisecond).UnixMilli(), Value: 200},
						{Timestamp: t0.Add(-30 * time.Second).Add(124 * time.Millisecond).UnixMilli(), Value: 400},
						{Timestamp: t0.Add(-20 * time.Second).Add(122 * time.Millisecond).UnixMilli(), Value: 600},
						{Timestamp: t0.Add(-10 * time.Second).Add(123 * time.Millisecond).UnixMilli(), Value: 800},
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
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: "request_total",
					}),
					Points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: t0.Add(-50 * time.Second).UnixMilli(), Value: 0},
						{Timestamp: t0.Add(-40 * time.Second).UnixMilli(), Value: 207},
						{Timestamp: t0.Add(-30 * time.Second).UnixMilli(), Value: 410},
						{Timestamp: t0.Add(-20 * time.Second).UnixMilli(), Value: 580},
						{Timestamp: t0.Add(-10 * time.Second).UnixMilli(), Value: 800},
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
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: "request_total",
					}),
					Points: []types.MetricPoint{
						// PromQL will extrapolate this point, which for a counter make sense: if at t0-50s the value is zero
						// and assuming no reset of counter, then the counter was zero before.
						// {Timestamp: (t0.Add(-60 * time.Second)).UnixMilli(), Value: 0},  <-- extrapolated point
						{Timestamp: t0.Add(-50 * time.Second).UnixMilli(), Value: 0},
						{Timestamp: t0.Add(-40 * time.Second).UnixMilli(), Value: 207},
						{Timestamp: t0.Add(-30 * time.Second).UnixMilli(), Value: 410},
						{Timestamp: t0.Add(-20 * time.Second).UnixMilli(), Value: 580},
						{Timestamp: t0.Add(-10 * time.Second).UnixMilli(), Value: 800},
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
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: "squirreldb_requests_points_total",
					}),
					Points: []types.MetricPoint{
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
			storeData: []types.Timeseries{
				{
					Labels: labels.FromMap(map[string]string{
						labelName: "squirreldb_requests_points_total",
					}),
					Points: []types.MetricPoint{
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
		{
			name:      "no-header",
			promql:    testcases.MetricName1,
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "1234",
						testcases.DiskItem:     testcases.DiskMount1,
					},
					Value:     4.2e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "5678",
						testcases.DiskItem:     "/srv",
					},
					Value:     42e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
		{
			name: "no-header-with-default-1",
			apiOption: options{
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			promql:    testcases.MetricName1,
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "1234",
						testcases.DiskItem:     testcases.DiskMount1,
					},
					Value:     4.2e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "5678",
						testcases.DiskItem:     "/srv",
					},
					Value:     42e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
		{
			name: "no-header-with-default-2",
			apiOption: options{
				MaxEvaluatedPoints: 200,
				MaxEvaluatedSeries: 2,
			},
			promql:    "disk_used + {__name__=~\"disk_.*\"}",
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantErr:   true,
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "1234",
						testcases.DiskItem:     testcases.DiskMount1,
					},
					Value:     4.2e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "5678",
						testcases.DiskItem:     "/srv",
					},
					Value:     42e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
		{
			name: "filter-account",
			reqHeader: map[string]string{
				types.HeaderForcedMatcher: testcases.AccountLabel + "=1234",
			},
			promql:    testcases.MetricName1,
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "1234",
						testcases.DiskItem:     testcases.DiskMount1,
					},
					Value:     4.2e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
		{
			name: "limit-series",
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedSeries: "1",
			},
			promql:    testcases.MetricName1,
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantErr:   true,
		},
		{
			name: "limit-series-and-filter-account",
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedSeries: "1",
				types.HeaderForcedMatcher:      testcases.AccountLabel + "=1234",
			},
			promql:    testcases.MetricName1,
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantErr:   false,
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{
						labelName:              testcases.MetricName1,
						testcases.AccountLabel: "1234",
						testcases.DiskItem:     testcases.DiskMount1,
					},
					Value:     4.2e+09,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
		{
			name: "limit-points",
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedPoints: "100",
			},
			promql:    "count_over_time(disk_used[10m])",
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantErr:   true,
		},
		{
			name: "limit-points-HeaderMaxEvaluatedPoints",
			reqHeader: map[string]string{
				types.HeaderMaxEvaluatedPoints: "100",
				types.HeaderForcedMatcher:      testcases.AccountLabel + "=1234",
			},
			promql:    "count_over_time(disk_used[10m])",
			storeData: defaultStoreData,
			time:      time.UnixMilli(1696257685488),
			wantErr:   false,
			wantSamples: []*model.Sample{
				{
					Metric: model.Metric{
						testcases.AccountLabel: "1234",
						testcases.DiskItem:     testcases.DiskMount1,
					},
					Value:     59,
					Timestamp: model.TimeFromUnixNano(time.UnixMilli(1696257685488).UnixNano()),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := makeTSDB(tt.storeData)
			squirrelDBAPI := makeAPI(store, nil)
			squirrelDBAPI.PromQLMaxEvaluatedPoints = tt.apiOption.MaxEvaluatedPoints
			squirrelDBAPI.PromQLMaxEvaluatedSeries = tt.apiOption.MaxEvaluatedSeries

			if err := squirrelDBAPI.init(t.Context()); err != nil {
				t.Fatal(err)
			}

			squirrelDBAPI.Ready()

			client, err := api.NewClient(api.Config{
				RoundTripper: roundTripperFromHTTPHandler{
					handler: squirrelDBAPI,
					headers: tt.reqHeader,
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			apiClient := v1.NewAPI(client)

			response, warning, err := apiClient.Query(t.Context(), tt.promql, tt.time)
			if err != nil {
				if tt.wantErr {
					return
				}

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
	headers map[string]string
}

func (rt roundTripperFromHTTPHandler) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()

	for k, v := range rt.headers {
		req.Header.Add(k, v)
	}

	rt.handler.ServeHTTP(rec, req)

	return rec.Result(), nil
}

func makeTSDB(data []types.Timeseries) *dummy.MemoryTSDB {
	metrics := make([]types.MetricLabel, len(data))
	store := &dummy.MemoryTSDB{}
	store.AddData(data, 86400)

	for idx, m := range data {
		metrics[idx] = types.MetricLabel{
			Labels: m.Labels,
			ID:     types.MetricID(idx),
		}
	}

	return store
}

func samplesPointerToValue(in []*model.Sample) []model.Sample {
	result := make([]model.Sample, len(in))

	for i, v := range in {
		result[i] = *v
	}

	return result
}

func generatePoints(maxT int64, stepT int64, count int64, value float64) []types.MetricPoint {
	result := make([]types.MetricPoint, count)

	for i := range result {
		result[i] = types.MetricPoint{
			Timestamp: maxT - stepT*(count-int64(i)),
			Value:     value,
		}
	}

	return result
}

// Try to be as close as daemon.go apiTask.
func makeAPI(
	backend types.TSDB,
	labelProcessor requestscoped.MutableLabelDetector,
) *API {
	promReqitry := prometheus.NewRegistry()

	return &API{
		ListenAddress:               "not used",
		Queryable:                   backend,
		Appendable:                  backend,
		PromQLMaxEvaluatedPoints:    uint64(config.DefaultConfig().PromQL.MaxEvaluatedPoints),
		PromQLMaxEvaluatedSeries:    uint32(config.DefaultConfig().PromQL.MaxEvaluatedSeries),
		MaxConcurrentRemoteRequests: config.DefaultConfig().RemoteStorage.MaxConcurrentRequests,
		MaxRequestBodySizeBytes:     config.DefaultConfig().MaxRequestBodySize << 20,
		TenantLabelName:             config.DefaultConfig().TenantLabelName,
		MutableLabelDetector:        labelProcessor,
		RequireTenantHeader:         config.DefaultConfig().RequireTenantHeader,
		MetricRegistry:              promReqitry,
		Logger:                      log.With().Logger(),
		FuturePointsBackdateOffset:  config.DefaultConfig().MaxAllowedTimeInFuture,
	}
}
