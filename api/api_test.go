package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"net/url"
	"squirreldb/dummy"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
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

func TestAPIRoute(t *testing.T) {
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
				map[string]string{"X-PromQL-Forced-Matcher": "__account_id=1234"},
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
			name: "promql-query-forced-matcher",
			makeRequest: newReqFactory(
				"GET",
				"/api/v1/label/__name__/values",
				nil,
				map[string]string{"X-PromQL-Forced-Matcher": "__account_id=1236"},
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
