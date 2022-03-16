package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"squirreldb/daemon"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/sync/errgroup"
)

//nolint:lll,gochecknoglobals
var (
	remoteWrite       = flag.String("write-urls", "", "URL of remote write (if both url are unset, start a built-in SquirrelDB and use it)")
	remoteRead        = flag.String("read-urls", "", "URL of read write (if both url are unset, start a built-in SquirrelDB and use it)")
	processes         = flag.Int("test.processes", 1, "Number of simulated SquirrelDB processes (only used is URLs are empty)")
	writeThreads      = flag.Int("test.write-threads", 2, "Number of writer threads")
	writeScale        = flag.Int("test.write-scale", 10, "Number of simulated agent")
	churn             = flag.Duration("test.churn", 5*time.Second, "Delay after which an agent will be replaced by another one.")
	writeResolution   = flag.Duration("test.write-resolution", 500*time.Millisecond, "Resolution of simulated agents.")
	readThreads       = flag.Int("test.read-threads", 2, "Number of reading threads")
	readDelay         = flag.Duration("test.read-delay", 500*time.Millisecond, "Delay between read requests")
	readRangeSize     = flag.Duration("test.read-range", 5*time.Second, "How much in the past reader requests")
	readQueryPerAgent = flag.Bool("read.query-per-agent", false, "Do one query per agent instead of one global query")
	runDuration       = flag.Duration("test.run-duration", 17*time.Second, "Duration of the test")
)

func main() {
	daemon.SetTestEnvironment()

	err := daemon.RunWithSignalHandler(run)

	if *remoteRead == "" && *remoteWrite == "" {
		metricResult, _ := prometheus.DefaultGatherer.Gather()
		for _, mf := range metricResult {
			_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
		}
	}

	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cfg, err := daemon.Config()
	if err != nil {
		return err
	}

	readURLs := strings.Split(*remoteRead, ",")
	writeURLs := strings.Split(*remoteWrite, ",")

	if readURLs[0] == "" && writeURLs[0] == "" { //nolint:nestif
		readURLs = nil
		writeURLs = nil

		for n := 0; n < *processes; n++ {
			squirreldb := &daemon.SquirrelDB{
				Config: cfg,
				MetricRegistry: prometheus.WrapRegistererWith(
					map[string]string{"process": strconv.FormatInt(int64(n), 10)},
					prometheus.DefaultRegisterer,
				),
			}

			if n == 0 {
				err = squirreldb.DropCassandraData(ctx, false)
				if err != nil {
					return err
				}

				err = squirreldb.DropTemporaryStore(ctx, false)
				if err != nil {
					return err
				}
			}

			err = squirreldb.Start(ctx)
			if err != nil {
				return err
			}

			readURLs = append(readURLs, fmt.Sprintf("http://127.0.0.1:%d/api/v1/read", squirreldb.ListenPort()))
			writeURLs = append(writeURLs, fmt.Sprintf("http://127.0.0.1:%d/api/v1/write", squirreldb.ListenPort()))

			defer squirreldb.Stop()
		}
	}

	sim := Simulator{
		readURLs:          readURLs,
		writeURLs:         writeURLs,
		agentFirstWrite:   make(map[string]time.Time),
		agentFirstReceive: make(map[string]time.Time),
		writerRunning:     *writeThreads,
	}

	readChan := make(chan prompb.ReadRequest)
	writeChan := make(chan prompb.WriteRequest)
	group, ctx := errgroup.WithContext(ctx)

	for n := 0; n < *readThreads; n++ {
		group.Go(func() error {
			return sim.readWorker(ctx, readChan)
		})
	}

	for n := 0; n < *writeThreads; n++ {
		group.Go(func() error {
			return sim.writeWorker(ctx, writeChan)
		})
	}

	group.Go(func() error {
		return sim.writeProducer(ctx, writeChan)
	})

	group.Go(func() error {
		return sim.readProducer(ctx, readChan)
	})

	log.Printf("Will run for %v", *runDuration)

	err = group.Wait()
	if err != nil {
		return err
	}

	var (
		minDelay time.Duration
		maxDelay time.Duration
		sumDelay time.Duration
		count    int
	)

	for k, sendTime := range sim.agentFirstWrite {
		receiveTime, ok := sim.agentFirstReceive[k]
		if !ok {
			err = fmt.Errorf("agent %s were never seen on read", k)
			log.Println(err)
		} else {
			delay := receiveTime.Sub(sendTime)
			sumDelay += delay
			count++

			if minDelay == 0 || minDelay > delay {
				minDelay = delay
			}

			if maxDelay == 0 || maxDelay < delay {
				maxDelay = delay
			}
		}
	}

	avgDelay := sumDelay / time.Duration(count)

	log.Printf(
		"Write %d points, read %d points in %d queries",
		sim.pointWrite,
		sim.pointRead,
		sim.queryCount,
	)
	log.Printf(
		"Delay between write and read: min/avg/max= %v/%v/%v",
		minDelay.Truncate(time.Millisecond),
		avgDelay.Truncate(time.Millisecond),
		maxDelay.Truncate(time.Millisecond),
	)

	return err
}

type Simulator struct {
	agentFirstWrite   map[string]time.Time
	agentFirstReceive map[string]time.Time
	writeURLs         []string
	activeAgent       []string
	readURLs          []string
	// index to next replaced agent
	idxChurn         int
	idxRead          int
	idxWrite         int
	writerRunning    int
	pointWrite       int
	queryCount       int
	pointRead        int
	l                sync.Mutex
	writerTerminated bool
}

func (s *Simulator) writeProducer(ctx context.Context, ch chan prompb.WriteRequest) error {
	defer close(ch)

	deadline := time.Now().Add(*runDuration)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "hostname.failed"
	}

	pid := os.Getpid()
	counter := 0
	nextChurn := time.Now().Add(*churn)

	if *churn == 0 {
		// 1 year is forever for this testing program
		nextChurn = time.Now().Add(365 * 24 * time.Hour)
	}

	for ctx.Err() == nil {
		if time.Now().After(deadline) {
			break
		}

		s.l.Lock()

		if len(s.activeAgent) == 0 {
			for n := 0; n < *writeScale; n++ {
				counter++
				s.activeAgent = append(
					s.activeAgent,
					fmt.Sprintf("%s-%d-%d", hostname, pid, counter),
				)
			}
		}

		if time.Now().After(nextChurn) {
			counter++
			s.activeAgent[s.idxChurn] = fmt.Sprintf("%s-%d-%d", hostname, pid, counter)
			s.idxChurn = (s.idxChurn + 1) % len(s.activeAgent)

			nextChurn = time.Now().Add(*churn)
		}

		s.l.Unlock()

		ts := []prompb.TimeSeries{}

		for i, name := range s.activeAgent {
			ts = append(ts, prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: "instance", Value: name},
					{Name: "__name__", Value: "process_cpu_seconds_total"},
				},
				Samples: []prompb.Sample{
					{Timestamp: time2Millisecond(time.Now()), Value: float64(i)},
				},
			})
			ts = append(ts, prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: "instance", Value: name},
					{Name: "__name__", Value: "go_info"},
				},
				Samples: []prompb.Sample{
					{Timestamp: time2Millisecond(time.Now()), Value: 1},
				},
			})
		}

		ch <- prompb.WriteRequest{
			Timeseries: ts,
		}

		select {
		case <-ctx.Done():
			continue
		case <-time.After(*writeResolution):
		}
	}

	log.Println("write producer terminated")

	return ctx.Err()
}

func time2Millisecond(t time.Time) int64 {
	return t.Unix()*1000 + (t.UnixNano()%1e9)/1e6
}

func (s *Simulator) missingAgents() map[string]time.Time {
	s.l.Lock()
	defer s.l.Unlock()

	results := make(map[string]time.Time)

	for k, createdAt := range s.agentFirstWrite {
		if _, ok := s.agentFirstReceive[k]; !ok {
			results[k] = createdAt
		}
	}

	return results
}

func (s *Simulator) readProducer(ctx context.Context, ch chan prompb.ReadRequest) error {
	defer close(ch)

	deadline := time.Now().Add(*runDuration)
	deadline2 := deadline.Add(*runDuration / 2)

	for ctx.Err() == nil {
		if time.Now().After(deadline2) {
			break
		}

		select {
		case <-ctx.Done():
			continue
		case <-time.After(*readDelay):
		}

		s.l.Lock()
		writerTerminated := s.writerTerminated
		s.l.Unlock()

		missingAgent := s.missingAgents()
		queries := []*prompb.Query{}

		if len(missingAgent) == 0 && writerTerminated {
			break
		}

		minStartQuery := time.Now()

		for k, createdAt := range missingAgent {
			if time.Since(createdAt) < *readRangeSize {
				continue
			}

			queryFrom := createdAt.Add(-5 * time.Second)

			if minStartQuery.After(queryFrom) {
				minStartQuery = queryFrom
			}

			queries = append(queries, &prompb.Query{
				StartTimestampMs: time2Millisecond(queryFrom),
				EndTimestampMs:   time2Millisecond(time.Now()),
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "instance", Value: k},
					{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "process_cpu_seconds_total"},
				},
			})
		}

		if *readQueryPerAgent {
			s.l.Lock()

			for _, name := range s.activeAgent {
				queries = append(queries, &prompb.Query{
					StartTimestampMs: time2Millisecond(time.Now().Add(-*readRangeSize)),
					EndTimestampMs:   time2Millisecond(time.Now()),
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: "instance", Value: name},
						{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "process_cpu_seconds_total"},
					},
				})
			}

			s.l.Unlock()
		} else {
			queryFrom := time.Now().Add(-*readRangeSize)
			if queryFrom.After(minStartQuery) {
				queryFrom = minStartQuery
			}

			queries = []*prompb.Query{
				{
					StartTimestampMs: time2Millisecond(queryFrom),
					EndTimestampMs:   time2Millisecond(time.Now()),
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "process_cpu_seconds_total"},
					},
				},
			}
		}

		if len(queries) > 0 {
			ch <- prompb.ReadRequest{Queries: queries}
		}
	}

	return ctx.Err()
}

func (s *Simulator) readURL() string {
	s.l.Lock()
	defer s.l.Unlock()

	u := s.readURLs[s.idxRead]
	s.idxRead = (s.idxRead + 1) % len(s.readURLs)

	return u
}

func (s *Simulator) writeURL() string {
	s.l.Lock()
	defer s.l.Unlock()

	u := s.writeURLs[s.idxWrite]
	s.idxWrite = (s.idxWrite + 1) % len(s.writeURLs)

	return u
}

func (s *Simulator) writeWorker(ctx context.Context, workChannel chan prompb.WriteRequest) error {
	for req := range workChannel {
		if ctx.Err() != nil {
			// we continue to quickly drain workChannel
			continue
		}

		seenAgent := make(map[string]bool)
		pointCount := 0

		for _, ts := range req.Timeseries {
			for _, lbl := range ts.Labels {
				if lbl.Name == "instance" {
					seenAgent[lbl.Value] = true

					break
				}
			}

			pointCount += len(ts.Samples)
		}

		body, err := req.Marshal()
		if err != nil {
			return err
		}

		compressedBody := snappy.Encode(nil, body)

		request, err := http.NewRequestWithContext(ctx, "POST", s.writeURL(), bytes.NewBuffer(compressedBody))
		if err != nil {
			return err
		}

		request.Header.Set("Content-Encoding", "snappy")
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("X-Prometheus-Remote-Read-Version", "2.0.0")

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}

		if response.StatusCode >= 300 {
			content, _ := ioutil.ReadAll(response.Body)
			log.Printf("Response code = %d, content: %s", response.StatusCode, content)

			return err
		}

		_, err = io.Copy(ioutil.Discard, response.Body)
		if err != nil {
			log.Printf("Failed to read response: %v", err)

			return err
		}

		response.Body.Close()

		s.l.Lock()

		for name := range seenAgent {
			if _, ok := s.agentFirstReceive[name]; !ok {
				s.agentFirstWrite[name] = time.Now()
			}
		}

		s.pointWrite += pointCount

		s.l.Unlock()
	}

	s.l.Lock()

	s.writerRunning--

	if s.writerRunning == 0 {
		s.writerTerminated = true
	}

	s.l.Unlock()

	return ctx.Err()
}

func (s *Simulator) readWorker(ctx context.Context, workChannel chan prompb.ReadRequest) error {
	for req := range workChannel {
		if ctx.Err() != nil {
			// we continue to quickly drain workChannel
			continue
		}

		body, err := req.Marshal()
		if err != nil {
			return err
		}

		compressedBody := snappy.Encode(nil, body)

		request, err := http.NewRequestWithContext(ctx, "POST", s.readURL(), bytes.NewBuffer(compressedBody))
		if err != nil {
			return err
		}

		request.Header.Set("Content-Encoding", "snappy")
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("X-Prometheus-Remote-Read-Version", "2.0.0")

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}

		content, _ := ioutil.ReadAll(response.Body)

		if response.StatusCode >= 300 {
			err = fmt.Errorf("response code = %d, content: %s", response.StatusCode, content)

			return err
		}

		response.Body.Close()

		uncompressed, err := snappy.Decode(nil, content)
		if err != nil {
			return err
		}

		var pbResponce prompb.ReadResponse

		if err := proto.Unmarshal(uncompressed, &pbResponce); err != nil {
			return err
		}

		seenAgent := make(map[string]bool)
		pointCount := 0

		for _, result := range pbResponce.Results {
			for _, ts := range result.Timeseries {
				for _, lbl := range ts.Labels {
					if lbl.Name == "instance" {
						seenAgent[lbl.Value] = true

						break
					}
				}

				pointCount += len(ts.Samples)
			}
		}

		s.l.Lock()

		s.queryCount += len(req.Queries)
		s.pointRead += pointCount

		for name := range seenAgent {
			if _, ok := s.agentFirstReceive[name]; !ok {
				s.agentFirstReceive[name] = time.Now()
			}
		}

		s.l.Unlock()
	}

	return ctx.Err()
}
