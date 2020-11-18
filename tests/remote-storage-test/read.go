package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

type readRequest struct {
	name     string
	request  prompb.ReadRequest
	response prompb.ReadResponse
}

func read(now time.Time) {
	log.Println("Starting read phase")

	workChannel := make(chan readRequest, *threads)

	var wg sync.WaitGroup

	for n := 0; n < *threads; n++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			readWorker(workChannel)
		}()
	}

	workChannel <- readRequest{
		name: "read one week data",
		request: prompb.ReadRequest{
			Queries: []*prompb.Query{
				{
					StartTimestampMs: time2Millisecond(now.Add(-time.Hour * 24 * 7)),
					EndTimestampMs:   time2Millisecond(now),
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: "nowStr", Value: *nowStr},
						{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "large_write"},
						{Type: prompb.LabelMatcher_EQ, Name: "size", Value: "week"},
					},
				},
			},
		},
		response: prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: "__name__", Value: "large_write"},
								{Name: "nowStr", Value: *nowStr},
								{Name: "size", Value: "week"},
							},
							Samples: makeSample(
								now.Add(-time.Hour*24*7),
								10*time.Second,
								0,
								1,
								3600*24*7/10,
							),
						},
					},
				},
			},
		},
	}

	workChannel <- readRequest{
		name: "read one minutes from large_write",
		request: prompb.ReadRequest{
			Queries: []*prompb.Query{
				{
					StartTimestampMs: time2Millisecond(now.Add(-time.Minute)),
					EndTimestampMs:   time2Millisecond(now),
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: "nowStr", Value: *nowStr},
						{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "large_write"},
					},
				},
			},
		},
		response: prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: "__name__", Value: "large_write"},
								{Name: "nowStr", Value: *nowStr},
								{Name: "size", Value: "hour"},
							},
							Samples: makeSample(
								now.Add(-time.Minute),
								10*time.Second,
								354*2,
								2,
								6,
							),
						},
						{
							Labels: []prompb.Label{
								{Name: "__name__", Value: "large_write"},
								{Name: "nowStr", Value: *nowStr},
								{Name: "size", Value: "week"},
							},
							Samples: makeSample(
								now.Add(-time.Minute),
								10*time.Second,
								60474,
								1,
								6,
							),
						},
					},
				},
			},
		},
	}
	workChannel <- readRequest{
		name: "multiple-query",
		request: prompb.ReadRequest{
			Queries: []*prompb.Query{
				{
					StartTimestampMs: time2Millisecond(now.Add(-time.Minute)),
					EndTimestampMs:   time2Millisecond(now),
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: "nowStr", Value: *nowStr},
						{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "sub_second"},
					},
				},
				{
					StartTimestampMs: time2Millisecond(now.Add(-time.Minute)),
					EndTimestampMs:   time2Millisecond(now),
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: "nowStr", Value: *nowStr},
						{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "high_precision"},
					},
				},
			},
		},
		response: prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: "__name__", Value: "sub_second"},
								{Name: "nowStr", Value: *nowStr},
							},
							Samples: makeSample(
								now.Add(-time.Minute),
								time.Millisecond,
								0,
								0.001,
								1000,
							),
						},
					},
				},
				{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: "__name__", Value: "high_precision"},
								{Name: "nowStr", Value: *nowStr},
							},
							Samples: makeSample(
								now.Add(-time.Minute),
								time.Second,
								42.123456789,
								00.000000001,
								60,
							),
						},
					},
				},
			},
		},
	}

	for n := 0; n < *scale; n++ {
		samples := makeSample(
			now.Add(-time.Minute),
			10*time.Second,
			100,
			-0.1,
			6,
		)
		workChannel <- readRequest{
			name: fmt.Sprintf("filler-batch-full-%d", n),
			request: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						StartTimestampMs: time2Millisecond(now.Add(-time.Minute)),
						EndTimestampMs:   time2Millisecond(now),
						Matchers: []*prompb.LabelMatcher{
							{Type: prompb.LabelMatcher_EQ, Name: "nowStr", Value: *nowStr},
							{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "filler"},
							{Type: prompb.LabelMatcher_EQ, Name: "batch", Value: "yes"},
							{Type: prompb.LabelMatcher_EQ, Name: "scale", Value: strconv.FormatInt(int64(n), 10)},
						},
					},
				},
			},
			response: prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{
									{Name: "__name__", Value: "filler"},
									{Name: "batch", Value: "yes"},
									{Name: "nowStr", Value: *nowStr},
									{Name: "scale", Value: strconv.FormatInt(int64(n), 10)},
								},
								Samples: samples,
							},
						},
					},
				},
			},
		}

		i := rand.Intn(6) // nolint: gosec
		workChannel <- readRequest{
			name: fmt.Sprintf("filler-batch-full-%d", n),
			request: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						StartTimestampMs: time2Millisecond(now.Add(-time.Minute).Add(10 * time.Second * time.Duration(i))),
						EndTimestampMs:   time2Millisecond(now.Add(-time.Minute).Add(10 * time.Second * time.Duration(i))),
						Matchers: []*prompb.LabelMatcher{
							{Type: prompb.LabelMatcher_EQ, Name: "nowStr", Value: *nowStr},
							{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "filler"},
							{Type: prompb.LabelMatcher_EQ, Name: "scale", Value: strconv.FormatInt(int64(n), 10)},
						},
					},
				},
			},
			response: prompb.ReadResponse{
				Results: []*prompb.QueryResult{
					{
						Timeseries: []*prompb.TimeSeries{
							{
								Labels: []prompb.Label{
									{Name: "__name__", Value: "filler"},
									{Name: "batch", Value: "no"},
									{Name: "nowStr", Value: *nowStr},
									{Name: "scale", Value: strconv.FormatInt(int64(n), 10)},
								},
								Samples: samples[i : i+1],
							},
							{
								Labels: []prompb.Label{
									{Name: "__name__", Value: "filler"},
									{Name: "batch", Value: "yes"},
									{Name: "nowStr", Value: *nowStr},
									{Name: "scale", Value: strconv.FormatInt(int64(n), 10)},
								},
								Samples: samples[i : i+1],
							},
						},
					},
				},
			},
		}
	}

	close(workChannel)
	wg.Wait()

	log.Println("Finished read phase")
}

func readWorker(workChannel chan readRequest) {
	for req := range workChannel {
		body, err := req.request.Marshal()
		if err != nil {
			log.Fatalf("Unable to marshal req: %v", err)
		}

		compressedBody := snappy.Encode(nil, body)

		request, err := http.NewRequest("POST", *remoteRead, bytes.NewBuffer(compressedBody)) // nolint: noctx
		if err != nil {
			log.Fatalf("unable to create request: %v", err)
		}

		request.Header.Set("Content-Encoding", "snappy")
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("X-Prometheus-Remote-Read-Version", "2.0.0")

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			log.Fatalf("read failed: %v", err)
		}

		content, _ := ioutil.ReadAll(response.Body)

		if response.StatusCode >= 300 {
			log.Fatalf("Response code = %d, content: %s", response.StatusCode, content)
		}

		response.Body.Close()

		uncompressed, err := snappy.Decode(nil, content)
		if err != nil {
			log.Fatalf("failed to uncompress: %v", err)
		}

		var pbResponce prompb.ReadResponse

		if err := proto.Unmarshal(uncompressed, &pbResponce); err != nil {
			log.Fatalf("failed to decode: %v", err)
		}

		equal(req.name, pbResponce, req.response)
	}
}

func labelsEqual(got, want []prompb.Label) bool {
	return cmpLabels(got, want) == 0
}

func samplesIncluded(got, want []prompb.Sample) string {
	gotIndex := 0

	for ; gotIndex < len(got); gotIndex++ {
		if got[gotIndex].Timestamp == want[0].Timestamp {
			break
		}
	}

	if gotIndex == len(got) {
		return fmt.Sprintf("got = %s want %s", fmtSample(got[0]), fmtSample(want[0]))
	}

	got = got[gotIndex:]
	if len(got) > len(want) {
		got = got[:len(want)]
	}

	return samplesEqual2(got, want)
}

func samplesEqual(got, want []prompb.Sample) string {
	if len(want) < len(got) {
		msg := samplesIncluded(got, want)

		if msg != "" {
			msg = samplesIncluded(sortedCopySample(got), sortedCopySample(want))
			if msg == "" {
				msg = "after sort, got is a superset of want"
			}
		} else {
			msg = "got is a superset of want"
		}

		return msg
	}

	if len(got) != len(want) {
		return fmt.Sprintf("len(got) = %d want %d", len(got), len(want))
	}

	msg := samplesEqual2(got, want)
	if msg != "" {
		msg = samplesEqual2(sortedCopySample(got), sortedCopySample(want))
		if msg == "" {
			msg = "equal after sort"
		}
	}

	return msg
}

func samplesEqual2(got, want []prompb.Sample) string {
	for i, g := range got {
		w := want[i]

		if g.Timestamp != w.Timestamp || g.Value != w.Value {
			return fmt.Sprintf("got = %s want %s", fmtSample(g), fmtSample(w))
		}
	}

	return ""
}

func sortedCopySample(v []prompb.Sample) []prompb.Sample {
	v2 := make([]prompb.Sample, len(v))

	copy(v2, v)

	sort.Slice(v2, func(i, j int) bool {
		return v2[i].Timestamp < v2[j].Timestamp
	})

	return v2
}

func cmpLabels(a, b []prompb.Label) int {
	a2 := make([]labels.Label, len(a))
	b2 := make([]labels.Label, len(b))

	for i, x := range a {
		a2[i] = labels.Label{
			Name:  x.Name,
			Value: x.Value,
		}
	}

	for i, x := range b {
		b2[i] = labels.Label{
			Name:  x.Name,
			Value: x.Value,
		}
	}

	return labels.Compare(a2, b2)
}

func sortTimeseries(v []*prompb.TimeSeries) []*prompb.TimeSeries {
	v2 := make([]*prompb.TimeSeries, len(v))
	copy(v2, v)

	// should this sort be done by SquirrelDB ?
	sort.Slice(v2, func(i, j int) bool {
		return cmpLabels(v2[i].Labels, v2[j].Labels) < 0
	})

	return v2
}

func equal(name string, got, want prompb.ReadResponse) {
	for i, gotResult := range got.Results {
		if i >= len(want.Results) {
			log.Printf("%s: got more result than expected. Extra result labels of 1st timeseries: %v", name, gotResult.Timeseries[0].Labels)

			continue
		}

		sortedTimeseries := sortTimeseries(gotResult.Timeseries)

		for j, gotTS := range sortedTimeseries {
			if j >= len(want.Results[i].Timeseries) {
				log.Printf("%s: got more timeseries than expected. Extra timeseries labels of 1st timeseries: %v", name, gotTS.Labels)

				continue
			}

			wantTS := want.Results[i].Timeseries[j]

			if !labelsEqual(gotTS.Labels, wantTS.Labels) {
				log.Printf("%s: labels = %v want %v", name, gotTS.Labels, wantTS.Labels)
			}

			if msg := samplesEqual(gotTS.Samples, wantTS.Samples); msg != "" {
				log.Printf("%s: Results[%d].TS[%d] = %s", name, i, j, msg)
			}
		}

		if len(want.Results[i].Timeseries) > len(gotResult.Timeseries) {
			log.Printf("%s: want %d more TS in Results[%d]", name, len(want.Results[i].Timeseries)-len(gotResult.Timeseries), i)
		}
	}

	if len(want.Results) > len(got.Results) {
		log.Printf("%s: want %d more Results", name, len(want.Results)-len(got.Results))
	}
}

func fmtSample(s prompb.Sample) string {
	t := time.Unix(s.Timestamp/1000, (s.Timestamp%1000)*1e6)

	return fmt.Sprintf("%v @ %v", s.Value, t)
}
