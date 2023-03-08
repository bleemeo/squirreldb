package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"squirreldb/api/remotestorage"
	"squirreldb/cassandra/mutable"
	"squirreldb/dummy"
	"squirreldb/logger"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
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
	t0 := time.Now().Add(-time.Minute)

	data := []struct {
		ID     types.MetricID
		Points []types.MetricPoint
		Labels labels.Labels
	}{
		{
			ID: 1,
			Labels: labels.FromMap(map[string]string{
				"__account_id": "1234",
				"__name__":     "cpu_used",
			}),
			Points: []types.MetricPoint{
				{Timestamp: t0.Unix() * 1000, Value: 11},
				{Timestamp: t0.Add(10*time.Second).Unix() * 1000, Value: 21},
			},
		},
		{
			ID: 2,
			Labels: labels.FromMap(map[string]string{
				"__account_id": "1234",
				"__name__":     "disk_used",
				"mountpoint":   "/home",
			}),
			Points: []types.MetricPoint{
				{Timestamp: t0.Unix() * 1000, Value: 12},
				{Timestamp: t0.Add(10*time.Second).Unix() * 1000, Value: 22},
			},
		},
		{
			ID: 3,
			Labels: labels.FromMap(map[string]string{
				"__account_id": "1235",
				"__name__":     "disk_used",
				"mountpoint":   "/home",
			}),
			Points: []types.MetricPoint{
				{Timestamp: t0.Unix() * 1000, Value: 13},
				{Timestamp: t0.Add(10*time.Second).Unix() * 1000, Value: 23},
			},
		},
		{
			ID: 4,
			Labels: labels.FromMap(map[string]string{
				"__account_id": "1236",
				"__name__":     "uptime",
				"instance":     "server:8015",
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

	api := API{
		Index:  dummy.NewIndex(idxData),
		Reader: store,
		Writer: store,
	}
	api.init()

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
		name             string
		makeRequest      func() *http.Request
		validateResponse func(t *testing.T, resp *http.Response)
	}{
		{
			name:        "readiness",
			makeRequest: newReqFactory("GET", "http://localhost:9201/ready", nil, nil),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}
			},
		},
		{
			name:        "readiness2",
			makeRequest: newReqFactory("GET", "/ready", nil, nil),
			validateResponse: func(t *testing.T, resp *http.Response) {
				t.Helper()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
				}
			},
		},
		{
			name:        "promql-query",
			makeRequest: newReqFactory("GET", urlWithParam("/api/v1/query", map[string]string{"query": "disk_used"}), nil, nil),
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
			name: "promql-query-forced-matcher",
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
			name:        "labels_values",
			makeRequest: newReqFactory("GET", "/api/v1/label/__name__/values", nil, nil),
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
			name: "promql-query-forced-matcher-2",
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
	}

	// First validate that all check fail when not ready
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name+"-not-ready", func(t *testing.T) {
			req := tt.makeRequest()
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			resp := w.Result()

			defer resp.Body.Close()

			want := http.StatusServiceUnavailable

			if resp.StatusCode != want {
				t.Errorf("StatusCode = %d, want %d", resp.StatusCode, want)
			}
		})
	}

	api.Ready()

	// First validate that all check fail when not ready
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := tt.makeRequest()
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			resp := w.Result()

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
		name           string
		labels         []prompb.Label
		expectStatus   int
		absentMatchers []*labels.Matcher
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
			expectStatus: http.StatusBadRequest,
		},
		{
			name: "invalid-label-name",
			labels: []prompb.Label{
				{Name: tenantLabelName, Value: tenantValue},
				{Name: "la-bel", Value: "val"},
			},
			expectStatus: http.StatusBadRequest,
		},
		// Mutable labels should be removed when writing.
		{
			name: "invalid-mutable-label",
			labels: []prompb.Label{
				{Name: tenantLabelName, Value: tenantValue},
				{Name: "group", Value: "my_group"},
			},
			expectStatus: http.StatusOK,
			absentMatchers: []*labels.Matcher{
				{Name: "group", Value: "my_group"},
			},
		},
	}

	for _, test := range tests {
		test := test

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

			dummyIndex := &dummy.Index{
				StoreMetricIDInMemory: true,
			}
			appendable := remotestorage.New(
				&dummy.MemoryTSDB{},
				dummyIndex,
				1,
				tenantLabelName,
				labelProcessor,
				prometheus.NewRegistry(),
			)
			writeHandler := remote.NewWriteHandler(log.NewLogfmtLogger(os.Stderr), appendable)

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

			req.Header.Add(types.HeaderTenant, tenantValue)

			recorder := httptest.NewRecorder()
			irw := &interceptor{OrigWriter: recorder}
			writeHandler.ServeHTTP(irw, req)

			resp := recorder.Result()
			defer resp.Body.Close()

			if resp.StatusCode != test.expectStatus {
				t.Fatalf("wanted status %d, got %v", test.expectStatus, resp.StatusCode)
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
