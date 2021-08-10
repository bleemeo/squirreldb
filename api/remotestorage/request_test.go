package remotestorage

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func Test_decodeRequest(t *testing.T) {
	// Test case generated with Prometheus's example_write_adapter modified
	// to dump it's request.Body to a file
	tests := []struct {
		file    string
		wantErr bool
		// wantCount is the actual number of TimeSeries in the file
		// and want may be smaller which means we only compare first entry (allow to avoid
		// too long test code)
		wantCount int
		want      prompb.WriteRequest
	}{
		{
			file:      "testdata/write_req_empty",
			wantCount: 4,
			want: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "up"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 0.0, Timestamp: 1579537265100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_duration_seconds"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 0.002502474, Timestamp: 1579537265100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_scraped"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 0.0, Timestamp: 1579537265100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_post_metric_relabeling"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 0.0, Timestamp: 1579537265100},
						},
					},
				},
			},
		},
		{
			file:      "testdata/write_req_one",
			wantCount: 5,
			want: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "dummy_metric"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
							{Name: "label_test", Value: "value-test"},
						},
						Samples: []prompb.Sample{
							{Value: 42.0, Timestamp: 1579538385100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "up"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538385100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_duration_seconds"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 0.003211523, Timestamp: 1579538385100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_scraped"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538385100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_post_metric_relabeling"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538385100},
						},
					},
				},
			},
		},
		{
			file:      "testdata/write_req_backlog",
			wantCount: 35,
			want: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "dummy_metric"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
							{Name: "label_test", Value: "value-test"},
						},
						Samples: []prompb.Sample{
							{Value: 42.0, Timestamp: 1579538575100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "up"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538575100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_duration_seconds"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 0.000956179, Timestamp: 1579538575100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_scraped"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538575100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_post_metric_relabeling"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538575100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "dummy_metric"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
							{Name: "label_test", Value: "value-test"},
						},
						Samples: []prompb.Sample{
							{Value: 42.0, Timestamp: 1579538585100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "up"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538585100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_duration_seconds"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 0.002707379, Timestamp: 1579538585100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_scraped"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538585100},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "scrape_samples_post_metric_relabeling"},
							{Name: "instance", Value: "localhost:8574"},
							{Name: "job", Value: "empty"},
						},
						Samples: []prompb.Sample{
							{Value: 1.0, Timestamp: 1579538585100},
						},
					},
				},
			},
		},
		{
			file:      "testdata/write_req_large",
			wantCount: 100,
			want: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "promhttp_metric_handler_requests_total"},
							{Name: "code", Value: "503"},
							{Name: "instance", Value: "localhost:8015"},
							{Name: "job", Value: "glouton"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1579537088498},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		wr := prompb.WriteRequest{}
		reqCtx := requestContext{
			pb: &wr,
		}

		t.Run(tt.file, func(t *testing.T) {
			f, err := os.Open(tt.file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer f.Close()
			if err := decodeRequest(f, &reqCtx); (err != nil) != tt.wantErr {
				t.Errorf("decodeRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(wr.Timeseries) != tt.wantCount {
				t.Errorf("len(decodeRequest().Timeseries) = %v, want %v", len(wr.Timeseries), tt.wantCount)
			}
			wr.Timeseries = wr.Timeseries[:len(tt.want.Timeseries)]
			if !reflect.DeepEqual(wr, tt.want) {
				t.Errorf("decodeRequest() = %v, want %v", wr, tt.want)
			}
		})
	}
}

func Benchmark_decodeRequest(b *testing.B) {
	tests := []string{
		"testdata/write_req_empty",
		"testdata/write_req_one",
		"testdata/write_req_backlog",
		"testdata/write_req_large",
	}

	for _, file := range tests {
		b.Run(file, func(b *testing.B) {
			data, err := ioutil.ReadFile(file)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			reader := bytes.NewReader(data)
			wr := prompb.WriteRequest{}
			reqCtx := requestContext{
				pb: &wr,
			}
			for n := 0; n < b.N; n++ {
				if err := decodeRequest(reader, &reqCtx); err != nil {
					b.Fatal(err)
				}
				if _, err := reader.Seek(0, io.SeekStart); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
