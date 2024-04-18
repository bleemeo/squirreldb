// This produce a remote write generator.
// It design is:
//   - one thread will produce metrics batch.
//   - each metrics batch belong to a single agent.
//     Each batch will contains metric-count metrics. Some metrics have common name (like cpu_used, disk_used /...)
//     other are more unique (e.g. disk_used /srv/generated1234). The common metrics are generated first, so a small
//     metric-count will produce only common name.
//   - The generation thread will create tenant-count * agent-count metrics batch every write-resolution
//     With default value, it simulate 10 tenant, each with 10 agents using a resolution of 10 seconds.
//   - The producer thread will send metrics batch a channel. This channel will be read by multiple writer threads
//   - Each writer thread will send on HTTP remote write request per batch of metrics, wait for the reply and process
//     next batch.
//   - Produced metrics value is a constant. This benchmark isn't good to test metric points compression (since value
//     aren't realistic). It more to test the index part.
//
// If the producer thread need to scale, run the generator program multiple time.
package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"math/rand"
	"net/http"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/types"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

//nolint:lll,gochecknoglobals
var (
	remoteWrite     = flag.String("remote-url", "", "URL of remote write")
	writeThreads    = flag.Int("writer-threads", 2, "Number of writer threads")
	agentCount      = flag.Int("agent-count", 10, "Number of simulated agent per tenant")
	metricCount     = flag.Int("metric-count", 20, "Number of simulated metric per agent")
	tenantCount     = flag.Int("tenant-count", 10, "Number of simulated tenant")
	tenantLabel     = flag.String("tenant-labels", "__account_id", "This should match SquirrelDB configuration")
	writeTTL        = flag.Duration("write-ttl", 7*24*time.Hour, "Metric TTL used during write")
	churn           = flag.Duration("agent-churn", 0, "Delay after which one agent is replaced by another one. 0 means disable churn")
	writeResolution = flag.Duration("write-resolution", 10*time.Second, "Resolution of simulated agents.")
	runDuration     = flag.Duration("run-duration", 0, "Duration of the test, use 0 for no limit")
	seed            = flag.Int64("random-seed", 0, "Test random seed used to generate various uuid (of tenant, agent id...). Use 0 to pick a randomized one")
)

func main() {
	log.Logger = logger.NewTestLogger(true)

	flag.Parse()

	if *remoteWrite == "" {
		log.Fatal().Msg("remote-url is required")

		return
	}

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	if err := daemon.RunWithSignalHandler(run); err != nil {
		log.Fatal().Err(err).Msg("Run failed")
	}
}

func run(deadlineCtx context.Context) error { //nolint: contextcheck
	rnd := rand.New(rand.NewSource(*seed)) //nolint:gosec

	producer := NewProducer(ProducerOption{
		TenantLabel: *tenantLabel,
		TenantCount: *tenantCount,
		AgentCount:  *agentCount,
		MetricCount: *metricCount,
		Resolution:  *writeResolution,
		Churn:       *churn,
		Seed:        rnd.Int63(),
	})

	writers := make([]*Writer, *writeThreads)
	for idx := range writers {
		writers[idx] = NewWriter(WriterOption{
			URL: *remoteWrite,
			TTL: *writeTTL,
		})
	}

	group, ctx := errgroup.WithContext(context.Background())

	if *runDuration > 0 {
		var cancel context.CancelFunc

		deadlineCtx, cancel = context.WithTimeout(deadlineCtx, *runDuration)
		defer cancel()

		go func() {
			// If the errgroup context is stopped, stop the deadlineCtx.
			// deadlineCtx is used for work generation.
			// ctx (errgroup context) is used for writers.
			// We use two context, because work generation need to be stopped before writers
			<-ctx.Done()
			cancel()
		}()
	}

	workChannel := make(chan prompb.WriteRequest, 10)

	for _, writer := range writers {
		writer := writer

		group.Go(func() error {
			return writer.Run(ctx, workChannel)
		})
	}

	group.Go(func() error {
		producer.Run(deadlineCtx, workChannel)
		close(workChannel)

		return nil
	})

	return group.Wait()
}

type WriterOption struct {
	URL string
	TTL time.Duration
}

type Writer struct {
	option WriterOption
}

func NewWriter(option WriterOption) *Writer {
	return &Writer{option: option}
}

func (w *Writer) Run(ctx context.Context, workChannel chan prompb.WriteRequest) error {
	ttlString := strconv.FormatInt(int64(w.option.TTL.Seconds()), 10)

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

		request, err := http.NewRequestWithContext(ctx, http.MethodPost, w.option.URL, bytes.NewBuffer(compressedBody))
		if err != nil {
			return err
		}

		request.Header.Set("Content-Encoding", "snappy")
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header.Set("X-Prometheus-Remote-Read-Version", "2.0.0")
		request.Header.Set(types.HeaderTimeToLive, ttlString)

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}

		if response.StatusCode >= http.StatusMultipleChoices {
			content, _ := io.ReadAll(response.Body)
			log.Printf("Response code = %d, content: %s", response.StatusCode, content)

			return err
		}

		_, err = io.Copy(io.Discard, response.Body)
		if err != nil {
			log.Printf("Failed to read response: %v", err)

			return err
		}

		response.Body.Close()
	}

	return nil
}

type ProducerOption struct {
	TenantLabel string
	TenantCount int
	AgentCount  int
	MetricCount int
	Resolution  time.Duration
	Churn       time.Duration
	Seed        int64
}

type Producer struct {
	option ProducerOption
}

func NewProducer(option ProducerOption) *Producer {
	return &Producer{option: option}
}

func makeIDs(rnd *rand.Rand, count int) []string {
	ids := make([]string, count)

	for idx := range ids {
		ids[idx] = strconv.FormatUint(rnd.Uint64(), 10)
	}

	return ids
}

func makeMetrics(rnd *rand.Rand, tenantLabel string, tenantID string, agentID string, count int) [][]prompb.Label {
	result := make([][]prompb.Label, count)

	commonList := []map[string]string{
		{
			"__name__": "cpu_used",
		},
		{
			"__name__": "cpu_user",
		},
		{
			"__name__": "cpu_system",
		},
		{
			"__name__": "cpu_idle",
		},
		{
			"__name__": "mem_total",
		},
		{
			"__name__": "mem_free",
		},
		{
			"__name__": "mem_available",
		},
		{
			"__name__": "mem_available_perc",
		},
		{
			"__name__": "system_load1",
		},
		{
			"__name__": "swap_used_perc",
		},
		{
			"__name__": "disk_used_perc",
			"item":     "/",
		},
		{
			"__name__": "io_reads",
			"item":     "/dev/sda",
		},
		{
			"__name__": "io_writes",
			"item":     "/dev/sda",
		},
		{
			"__name__": "disk_used_perc",
			"item":     "/srv",
		},
		{
			"__name__": "io_reads",
			"item":     "/dev/sdb",
		},
		{
			"__name__": "io_writes",
			"item":     "/dev/sdb",
		},
		{
			"__name__": "net_bits_recv",
			"item":     "eth0",
		},
		{
			"__name__": "net_bits_recv",
			"item":     "enp1s0",
		},
		{
			"__name__": "net_bits_recv",
			"item":     "eno1",
		},
		{
			"__name__": "net_bits_recv",
			"item":     "vio0",
		},
	}

	for idx := range result {
		row := []prompb.Label{
			{Name: tenantLabel, Value: tenantID},
			{Name: "instance_id", Value: agentID},
		}

		switch {
		// first metrics are common (unmodified)
		case idx < len(commonList):
			for k, v := range commonList[idx] {
				row = append(row, prompb.Label{Name: k, Value: v})
			}
		// following group metrics similar, only item change not the name
		// and the change is common to all agents
		case idx < 2*len(commonList):
			for k, v := range commonList[idx%len(commonList)] {
				if k != "__name__" {
					v += strconv.FormatInt(int64(idx), 10)
				}

				row = append(row, prompb.Label{Name: k, Value: v})
			}

			if len(commonList[idx%len(commonList)]) == 1 {
				// only name, create a item
				row = append(row, prompb.Label{Name: "item_made", Value: strconv.FormatInt(int64(idx), 10)})
			}
		// on the next one, name still don't change but items is randomized
		case idx < 3*len(commonList):
			for k, v := range commonList[idx%len(commonList)] {
				if k != "__name__" {
					v += strconv.FormatInt(rnd.Int63(), 10)
				}

				row = append(row, prompb.Label{Name: k, Value: v})
			}

			if len(commonList[idx%len(commonList)]) == 1 {
				// only name, create a item
				row = append(row, prompb.Label{Name: "item_made", Value: strconv.FormatInt(rnd.Int63(), 10)})
			}
		// finally both name & item is randomized
		default:
			for k, v := range commonList[idx%len(commonList)] {
				v += strconv.FormatInt(rnd.Int63(), 10)
				row = append(row, prompb.Label{Name: k, Value: v})
			}
		}

		result[idx] = row
	}

	return result
}

func (p *Producer) Run(ctx context.Context, targetChannel chan prompb.WriteRequest) {
	ticker := time.NewTicker(p.option.Resolution)
	rnd := rand.New(rand.NewSource(p.option.Seed)) //nolint:gosec
	tenants := makeIDs(rnd, p.option.TenantCount)
	agents := make(map[string][]string, p.option.TenantCount)
	metrics := make(map[string][][]prompb.Label, p.option.AgentCount*p.option.MetricCount)

	for _, tenantID := range tenants {
		agents[tenantID] = makeIDs(rnd, p.option.AgentCount)

		for _, agentID := range agents[tenantID] {
			metrics[agentID] = makeMetrics(rnd, p.option.TenantLabel, tenantID, agentID, p.option.MetricCount)
		}
	}

	var (
		nextChurn      time.Time
		churnTenantIdx int
		churnAgentIdx  int
	)

	if p.option.Churn != 0 {
		nextChurn = time.Now().Add(p.option.Churn)
	}

	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}

		if ctx.Err() != nil {
			break
		}

		now := time.Now().Truncate(time.Millisecond)

		if !nextChurn.IsZero() && time.Now().After(nextChurn) {
			churnAgentIdx = (churnAgentIdx + 1) % p.option.AgentCount
			if churnAgentIdx == 0 {
				churnTenantIdx = (churnTenantIdx + 1) % p.option.TenantCount
			}

			tenantID := tenants[churnTenantIdx]
			oldAgentID := agents[tenantID][churnAgentIdx]
			newAgentID := strconv.FormatUint(rnd.Uint64(), 10)
			agents[tenantID][churnAgentIdx] = newAgentID

			delete(metrics, oldAgentID)

			metrics[newAgentID] = makeMetrics(rnd, p.option.TenantLabel, tenantID, newAgentID, p.option.MetricCount)

			nextChurn = time.Now().Add(p.option.Churn)
		}

		for _, metricsList := range metrics {
			result := make([]prompb.TimeSeries, len(metricsList))

			for idx, metric := range metricsList {
				result[idx] = prompb.TimeSeries{
					Labels: metric,
					Samples: []prompb.Sample{
						{
							Timestamp: now.UnixMilli(),
							Value:     float64(idx),
						},
					},
				}
			}

			targetChannel <- prompb.WriteRequest{Timeseries: result}
		}
	}
}
