package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"squirreldb/api/remotestorage"
	"squirreldb/dummy"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

// Test that when we try to write an invalid label or metric name we get an HTTP 400 status code.
// We rely on Prometheus returning a 500 error by default that we convert to a 400, but this behaviour
// could be changed in the future.
// See ServeHTTP function in https://github.com/prometheus/prometheus/blob/main/storage/remote/write_handler.go.
func Test_InterceptorStatusCode(t *testing.T) {
	appendable := remotestorage.New(dummy.DiscardTSDB{}, &dummy.Index{}, 1, prometheus.NewRegistry())
	writeHandler := remote.NewWriteHandler(log.NewLogfmtLogger(os.Stderr), appendable)

	tests := []struct {
		name   string
		labels []prompb.Label
	}{
		{
			name: "invalid-metric-name",
			labels: []prompb.Label{
				{Name: "__name__", Value: "na-me"},
			},
		},
		{
			name: "invalid-label-name",
			labels: []prompb.Label{
				{Name: "la-bel", Value: "val"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wr := &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: test.labels,
						Samples: []prompb.Sample{
							{
								Value:     10,
								Timestamp: time.Now().Unix() / 1e6,
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

			req, err := http.NewRequest("POST", "http://localhost:9201/write", body) //nolint: noctx
			if err != nil {
				t.Fatal(err)
			}

			recorder := httptest.NewRecorder()
			irw := &interceptor{OrigWriter: recorder}
			writeHandler.ServeHTTP(irw, req)

			resp := recorder.Result()
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("wanted status 400, got %v", resp.StatusCode)
			}
		})
	}
}
