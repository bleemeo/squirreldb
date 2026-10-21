package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/core/processsetup"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/tests/common/testsetup"
	"github.com/bleemeo/squirreldb/types"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog/log"
)

var errUnexpectedSeries = errors.New("unexpected series returned")

// A just-written point is only queryable once the ClickHouse backend has drained
// its write-ahead log (normally ~5s); reads are eventually consistent. Retry each
// query until it matches or the budget elapses — a consistent read passes on the
// first try, so this only adds delay right after a write. The budget is a bit above
// the normal drain delay so a slow drain doesn't fail the test spuriously.
const (
	queryRetryBudget = 10 * time.Second
	queryRetryStep   = 500 * time.Millisecond
)

func retryUntilConsistent(ctx context.Context, fn func() error) error {
	start := time.Now()

	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			if attempt > 1 {
				// Log the observed write->visible delay (how long the drain took).
				log.Info().Dur("delay", time.Since(start)).Msg("query became consistent (write now visible)")
			}

			return nil
		}

		if time.Since(start) >= queryRetryBudget {
			return err
		}

		select {
		case <-ctx.Done():
			return err
		case <-time.After(queryRetryStep):
		}
	}
}

func main() {
	testsetup.SetTestEnvironment()

	err := processsetup.RunWithSignalHandler(run)
	if err != nil {
		log.Fatal().Err(err).Msg("Run daemon failed")
	}
}

func run(ctx context.Context, _ context.Context) error {
	cfg, warnings, err := testsetup.LoadConfigForTest()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	squirreldb := &daemon.SquirrelDB{
		Config:            cfg,
		MetricRegistry:    prometheus.DefaultRegisterer,
		Logger:            log.With().Str("component", "daemon").Logger(),
		DropDataOnStartup: true, // the backend drops its data when it first starts
	}

	err = squirreldb.SquirrelDBCore().TestTools().DropTemporaryStore(ctx, false)
	if err != nil {
		return err
	}

	err = squirreldb.Start(ctx)
	if err != nil {
		return err
	}

	defer squirreldb.Stop()

	squirrelDBURL := fmt.Sprintf("http://127.0.0.1:%d", squirreldb.ListenPort())

	// t0 is 14:00 UTC yesterday: the whole dataset is in the past, and the anchor falls
	// on a 15-min bucket, hour and day boundary at once (see the dataset spans).
	yesterday := time.Now().Add(-24 * time.Hour)
	t0 := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 14, 0, 0, 0, time.UTC)

	return runAggMatrix(ctx, cfg.Backend, t0, squirrelDBURL)
}

func preaggregate(ctx context.Context, squirrelDBURL string, from time.Time, to time.Time) error {
	aggregateURL := fmt.Sprintf(
		"%s/debug/preaggregate?from=%s&to=%s",
		squirrelDBURL,
		from.Format("2006-01-02"),
		to.Format("2006-01-02"),
	)

	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, aggregateURL, nil)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Printf("write failed: %v", err)

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

	return response.Body.Close()
}

// Make repeting samples from the values.
func makeSamples(
	fromTime time.Time,
	stepTime time.Duration,
	values []int,
	count int,
) []prompb.Sample {
	samples := make([]prompb.Sample, 0, count*len(values))

	for i := range count {
		for j, value := range values {
			samples = append(samples, prompb.Sample{
				Value:     float64(value),
				Timestamp: fromTime.Add(stepTime * time.Duration(len(values)*i+j)).UnixMilli(),
			})
		}
	}

	return samples
}

func writeSeries(ctx context.Context, req prompb.WriteRequest, squirrelDBURL string) error {
	log.Info().Msg("Writing series")

	if ctx.Err() != nil {
		return ctx.Err()
	}

	body, err := req.Marshal()
	if err != nil {
		log.Printf("Unable to marshal req: %v", err)

		return err
	}

	writeURL := squirrelDBURL + "/api/v1/write"
	compressedBody := snappy.Encode(nil, body)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, writeURL, bytes.NewBuffer(compressedBody))
	if err != nil {
		log.Printf("unable to create request: %v", err)

		return err
	}

	request.Header.Set("Content-Encoding", "snappy")
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Printf("write failed: %v", err)

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

	_ = response.Body.Close()

	return nil
}

var (
	errUnexpectedSource   = errors.New("read routed to an unexpected source")
	errCoverage           = errors.New("coverage mismatch (raw vs aggregate)")
	errAggRawDiffer       = errors.New("aggregate and raw differ beyond tolerance")
	errUnexpectedRawValue = errors.New("raw value differs from the fixed expectation")
	errOutageOutOfSpan    = errors.New("probe outage falls outside the written span")
	errProbeIncomplete    = errors.New("probe_status is not fully queryable yet")
)

// Every point written and every eval window is placed relative to t0, the newest
// instant the dataset holds, so a case's span and the history it needs are read off
// the same reference. Each series declares how far back it starts and at which
// interval; they don't have to agree, and they don't: the matrix series only need a
// day, probe_status needs a month.
const (
	// The matrix series (cpu_used, mem_used_perc, the counters, ...): one day at 10s,
	// which covers the widest look-behind they are queried with ([12h]).
	matrixSpan = 24 * time.Hour
	matrixStep = 10 * time.Second

	// probe_status is queried like the Bleemeo status page: a [30d:60s] subquery at the
	// 60s probe interval, evaluated at 24h steps. The earliest of those steps looks back
	// 30 days from t0-2d, hence a span of a bit over 32 days.
	probeSpan = 33 * 24 * time.Hour
	probeStep = time.Minute

	// probeLookback MUST stay equal to the range of the [30d:60s] subqueries in the
	// probe_* cases: probeExpectations counts the samples such a window holds.
	probeLookback = 30 * 24 * time.Hour

	// probeStatusPageSpan is how far back the status-page cases are evaluated. Every
	// step then looks back over the same outages (see probeOutages), so one expected
	// value holds for all of them.
	probeStatusPageSpan = 2 * 24 * time.Hour
)

// defaultExpectedTol is the relative tolerance of a checkExpected comparison when the
// case does not set expectedTol. A case whose expectation is computed rather than
// eyeballed should set a tight one — 5% of an uptime percentage would hide 40 minutes
// of downtime — as probeExpectedTol does: the probe_* expectations come out of the
// outage table, so only float noise separates them from the query result.
const (
	defaultExpectedTol = 0.05
	probeExpectedTol   = 1e-9
)

// compareStrategy is how a case checks the aggregate read against the raw one.
type compareStrategy int

const (
	// cmpClose: each aggregate value is within relTol of the raw value (continuous
	// metrics and *_over_time / rate, which the aggregate reconstructs approximately).
	cmpClose compareStrategy = iota
	// cmpExact: aggregate equals raw. For functions that MUST fall back to raw
	// (count_over_time, changes, ...), where a regression re-routing them to the
	// aggregate would change the value and trip this; and for the reads the aggregate
	// reproduces exactly, such as last_over_time on a bucket-aligned range.
	cmpExact
)

// evalWindow is the time span a case is evaluated over: step == 0 is an instant
// query at `end`; step > 0 is a range query over [start, end] stepping by step. The
// zero value means "use the matrix default window".
type evalWindow struct {
	start time.Time
	end   time.Time
	step  time.Duration
}

func (w evalWindow) instant() bool { return w.step == 0 }

// aggCase is one raw-vs-aggregate comparison over a single-series PromQL query.
// Coverage (same eval steps as raw) is always required — it is the direct guard on
// the lookback/snapping fix.
type aggCase struct {
	name  string
	query string
	// window is the eval span; the zero value uses the matrix default window.
	window evalWindow
	// wantSource is the read_source_total{source} the natural read must hit; checked
	// on ClickHouse only (the metric is ClickHouse-specific).
	wantSource string
	strategy   compareStrategy
	// relTol is the allowed relative error vs raw (cmpClose).
	relTol float64
	// expected (when checkExpected) is a fixed value checked against the raw result,
	// validating raw itself. expectedTol is the relative tolerance of that check; 0
	// means defaultExpectedTol, loose enough for an expectation that is an order of
	// magnitude rather than a computed value.
	expected      float64
	expectedTol   float64
	checkExpected bool
	// skipIfBackends lists the backends this case does not apply to.
	skipIfBackends []string
	// knownLimitation, when set, is why the aggregate cannot reproduce raw here. The
	// case still runs, and only a VALUE difference is tolerated (and logged with its
	// measured error): a coverage, routing or raw-value regression still fails. A case
	// that stops differing is logged too, so the field gets dropped when it is fixed.
	knownLimitation string
}

// runAggMatrix writes the matrix' series then, for each case, reads it twice: raw
// via the ForceRaw header (the reference) and auto-routed (the path under test). On
// ClickHouse it confirms the auto-routed read hit the expected source (via
// read_source_total); it compares the two results per the case strategy on every
// backend. cpu_used is already written by write(); this adds the discrete and
// counter series. Cases that don't apply to a backend list it in skipIfBackends.
func runAggMatrix(ctx context.Context, backend string, t0 time.Time, squirrelDBURL string) error { //nolint:maintidx
	if err := writeMatrixSeries(ctx, t0, squirrelDBURL); err != nil {
		return err
	}

	if err := writeProbeStatusSeries(ctx, t0, squirrelDBURL); err != nil {
		return err
	}

	// Cassandra aggregate is built by a background task every day. Explicitly run it now.
	// Only over the matrix span: the probe_status cases read raw (their 60s step is below
	// Cassandra's 5-min aggregation resolution), so they need no pre-aggregated data.
	if backend == "cassandra" {
		if err := preaggregate(ctx, squirrelDBURL, t0.Add(-matrixSpan), t0); err != nil {
			return err
		}
	}

	rawAPI, err := newQueryAPI(squirrelDBURL, map[string]string{types.HeaderForceRaw: "true"})
	if err != nil {
		return err
	}

	natAPI, err := newQueryAPI(squirrelDBURL, nil)
	if err != nil {
		return err
	}

	if err := waitProbeSeriesVisible(ctx, rawAPI, t0); err != nil {
		return err
	}

	// Default window: step >= aggMinStep (1h) so instant vectors auto-route to the
	// snapped aggregate, and [2h] range selectors span >= 2 buckets so they route to
	// the plain aggregate. It sits well inside the written data so both paths cover
	// every step. A case overrides it via its own window (e.g. an instant query).
	defaultWindow := evalWindow{
		start: t0.Add(-matrixSpan + 3*time.Hour),
		end:   t0.Add(-time.Hour),
		step:  time.Hour,
	}

	// This window is in the middle of the dataset with enough data before all
	// timestamp such as a PromQL with large look-behind (`my_metric[12h]`) works.
	windowMiddleOfDataset := evalWindow{
		start: t0.Add(-12 * time.Hour),
		end:   t0,
		step:  time.Hour,
	}

	// Same window, off the hour: a subquery evaluates its inner vector on the multiples
	// of its own step (whole hours here), which no longer coincide with the eval steps
	// of the outer query. The snapped read has to cover that grid too.
	windowUnalignedWithSubqueryStep := evalWindow{
		start: t0.Add(-12*time.Hour + 5*time.Minute),
		end:   t0,
		step:  time.Hour,
	}

	// The status-page window: the 24h step Bleemeo evaluates its 30-day queries at.
	probeStatusPageWindow := evalWindow{
		start: t0.Add(-probeStatusPageSpan),
		end:   t0,
		step:  24 * time.Hour,
	}

	// The incident-timeline window brackets the long outage with an hour of OK on each
	// side, at the 60s step the timeline is drawn at — so the compared series carries
	// both OK and critical, not a flat line.
	probeIncidentWindow := evalWindow{
		start: probeLongOutage.start(t0).Add(-time.Hour),
		end:   probeLongOutage.start(t0).Add(probeLongOutage.duration() + time.Hour),
		step:  time.Minute,
	}

	probeEpisodes, probeUptimePercent := probeExpectations(t0)

	// limitationSnapIsBucketAligned is what remains of the snapping error now that the
	// aggregate stores a per-bucket last value: the snapped point carries the last
	// sample of the last bucket that ENDED at/before the eval instant, so an instant on
	// the 15-min bucket grid reads the raw sample itself, and an instant off that grid
	// reads a value up to one bucket old.
	const limitationSnapIsBucketAligned = "an eval instant off the 15-min bucket grid reads " +
		"the last sample of the previous bucket edge, up to one bucket old"

	// Relative error tiers, tightest first: what the difference between the aggregated
	// and the raw result would look like on a graph.
	const (
		// 0.1% error, error small enough to possibly be due to rounding errors.
		// Note: some case do test for cmpExact (0% error), roundingError is only used
		//  when aggregation is not totally equal but very close to raw data.
		roundingError = 0.001

		// 0.5% error, small enough to not be visible.
		invisibleError = 0.005

		// 1% error. Still okay, but might start to be visible in some case
		barelyVisibleError = 0.01

		// 5% error, start to be visible, but good enough for the trade-off.
		visibleError = 0.05
	)

	cases := []aggCase{
		{
			name:          "min_over_time",
			query:         `min_over_time(cpu_used{instance="server-1"}[2h])`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        roundingError,
			expected:      0,
			checkExpected: true,
		},
		{
			name:          "max_over_time",
			query:         `max_over_time(cpu_used{instance="server-1"}[2h])`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        roundingError,
			expected:      2,
			checkExpected: true,
		},
		{
			name:           "avg_over_time",
			query:          `avg_over_time(cpu_used{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			expected:       1,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra isn't exact enough
		},
		{
			// sum_over_time: window-edge bucket alignment gives up to ~one bucket of slack.
			name:           "sum_over_time",
			query:          `sum_over_time(cpu_used{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"}, // Cassandra is broken here, value is ~30 times lower than expected.
		},
		{
			name:       "rate",
			query:      `rate(requests_total{instance="server-1"}[2h])`,
			window:     defaultWindow,
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     roundingError,
		},
		{
			// The guard on the counter family reading the bucket max: serve it the
			// per-bucket last value instead and the climb inside the reset bucket is lost,
			// dropping this by a third (see aggValueExpr).
			name:       "rate with resets",
			query:      `rate(counter_with_reset_total{instance="server-1"}[1h])`,
			window:     defaultWindow,
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     roundingError,
		},
		{
			name:       "more_rate",
			query:      `rate(requests_total{instance="server-1"}[6h])`,
			window:     windowMiddleOfDataset,
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     roundingError,
		},
		{
			name:       "more_rate with resets",
			query:      `rate(counter_with_reset_total{instance="server-1"}[6h])`,
			window:     windowMiddleOfDataset,
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     roundingError,
		},
		{
			name:       "more_rate with resets2",
			query:      `rate(counter_with_reset_total{instance="server-1"}[90m])`,
			window:     windowUnalignedWithSubqueryStep,
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     roundingError,
		},
		{
			name:          "memory average",
			query:         `avg_over_time(mem_used_perc{instance="server-1"}[1h])`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        roundingError,
			expected:      50,
			checkExpected: true,
		},
		{
			name:          "sum of memory average",
			query:         `sum(avg_over_time(mem_used_perc[1h]))`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        roundingError,
			expected:      100,
			checkExpected: true,
		},
		{
			name:           "sum of disk average (range)",
			query:          `sum(avg_over_time(disk_used_perc[1h]))`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"}, // Cassandra had much higher relative error
		},
		{
			name:           "sum of disk average (instant)",
			query:          `sum(avg_over_time(disk_used_perc[1h]))`,
			window:         evalWindow{end: t0.Add(-matrixSpan + time.Hour)},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			expected:       126.099, // run test to get the value
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "sum of disk average2",
			query:          `sum(avg_over_time(disk_used_perc[1h]))`,
			window:         evalWindow{end: t0.Add(-matrixSpan + 4*time.Hour)},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			expected:       73.90,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 1h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[1h])`,
			window:         evalWindow{end: t0},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 3h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[3h])`,
			window:         evalWindow{end: t0},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 6h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[6h])`,
			window:         evalWindow{end: t0},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			expected:       50,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 9h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window:         evalWindow{end: t0},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 12h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[12h])`,
			window:         evalWindow{end: t0},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			expected:       50,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:       "avg_over_time query_range 9h",
			query:      `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window:     evalWindow{start: t0.Add(-5 * 9 * time.Hour), end: t0, step: 9 * time.Hour},
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     roundingError,
		},
		{
			name:           "sum_over_time query_range 9h",
			query:          `sum_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window:         evalWindow{start: t0.Add(-5 * 9 * time.Hour), end: t0, step: 9 * time.Hour},
			wantSource:     "aggregated",
			strategy:       cmpExact,
			skipIfBackends: []string{"cassandra"}, // Cassandra is wrong by a x30 (it sum the pre-aggregated average)
		},
		{
			name:  "sum_over_time query_range 9h - offset 3 minutes",
			query: `sum_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window: evalWindow{
				start: t0.Add(-5 * 9 * time.Hour).Add(-3 * time.Minute),
				end:   t0.Add(-3 * time.Minute),
				step:  9 * time.Hour,
			},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         visibleError,
			skipIfBackends: []string{"cassandra"}, // Cassandra is wrong by a x30 (it sum the pre-aggregated average)
		},
		{
			name:  "avg_over_time query_range 9h + offset 3 minutes",
			query: `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window: evalWindow{
				start: t0.Add(-5 * 9 * time.Hour).Add(3 * time.Minute),
				end:   t0.Add(3 * time.Minute),
				step:  9 * time.Hour,
			},
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     invisibleError,
		},
		{
			name:  "avg_over_time query_range 9h - offset 3 minutes",
			query: `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window: evalWindow{
				start: t0.Add(-5 * 9 * time.Hour).Add(-3 * time.Minute),
				end:   t0.Add(-3 * time.Minute),
				step:  9 * time.Hour,
			},
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     invisibleError,
		},
		{
			// stddev/stdvar_over_time are served by max(max) — one representative point
			// per bucket, not the true per-point spread. This validates the approximation
			// stays close to raw: over a full 6h period the bucket-max series tracks the
			// wave, so its stddev stays near raw's amplitude/√2 (=14.14 for amplitude 20),
			// variance near amplitude²/2 (=200). relTol is the accepted approximation
			// bound — tune to the observed error + margin on first run.
			name:           "stddev_over_time (max approximation)",
			query:          `stddev_over_time(disk_used_perc{instance="server-1"}[6h])`,
			window:         evalWindow{end: t0},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         barelyVisibleError,
			expected:       14.142,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "stdvar_over_time (max approximation)",
			query:          `stdvar_over_time(disk_used_perc{instance="server-1"}[6h])`,
			window:         evalWindow{end: t0},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         barelyVisibleError * 2, // variance ~ stddev², so ~2x the stddev relative error
			expected:       200,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			// An instant query (step 0) over a huge [range] must auto-route to the
			// aggregate rather than scan the whole raw window — the case
			// ForcePreAggregated used to cover manually.
			name:           "instant large range",
			query:          `avg_over_time(cpu_used{instance="server-1"}[20h])`,
			window:         evalWindow{end: t0.Add(-time.Hour)},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         visibleError,
			expected:       1,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			// present_over_time depends only on presence, so the aggregate serves it (returns 1).
			name:          "present_over_time",
			query:         `present_over_time(cpu_used{instance="server-1"}[2h])`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        roundingError,
			expected:      1,
			checkExpected: true,
		},
		{
			// Instant vectors are snapped to the eval grid, valued from the bucket's last
			// sample: on this window every step is on the bucket grid, so the snapped point
			// IS the sample raw returns — a discrete metric stays discrete, and any drift
			// in the snapping shows up here as an inequality.
			name:           "bare cpu_used",
			query:          `cpu_used{instance="server-1"}`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpExact,
			skipIfBackends: []string{"cassandra"}, // Not equal enough
		},
		{
			name:           "bare service_status (discrete)",
			query:          `service_status{instance="server-1"}`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpExact,
			skipIfBackends: []string{"cassandra"}, // Not equal enough
		},
		{
			name:           "sum(cpu_used) instant agg",
			query:          `sum(cpu_used{instance="server-1"})`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpExact,
			skipIfBackends: []string{"cassandra"}, // Not equal enough
		},
		{
			// Must fall back to raw: count_over_time counts points, not values.
			name:           "count_over_time falls back to raw",
			query:          `count_over_time(cpu_used{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "raw",
			strategy:       cmpExact,
			skipIfBackends: []string{"cassandra"}, // No fallback to raw result is large error
		},
		{
			// Must fall back to raw: changes needs per-point order.
			name:           "changes falls back to raw",
			query:          `changes(service_status{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "raw",
			strategy:       cmpExact,
			skipIfBackends: []string{"cassandra"}, // No fallback to raw result is large error
		},
		{
			name:           "predict_linear",
			query:          `predict_linear(cpu_used{instance="server-1"}[2h], 3600)`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         invisibleError,
			skipIfBackends: []string{"cassandra"}, // Cassandra is not exact enough
		},
		{
			name:          "complex_subquery",
			query:         `count_over_time((disk_used_perc{instance="server-1"} > 50)[6h:2m])`,
			window:        windowMiddleOfDataset,
			wantSource:    "raw",
			strategy:      cmpClose,
			relTol:        roundingError,
			checkExpected: true,
			expected:      90,
		},
		{
			name:           "complex_subquery2",
			query:          `count_over_time((disk_used_perc{instance="server-1"} > 50)[6h:15m])`,
			window:         windowMiddleOfDataset,
			wantSource:     "raw",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"},
		},
		// The three complex_subquery3_thr* cases are one subquery at three thresholds,
		// including the two that no bucket extremum could ever get right. They pass on
		// the value each threshold is applied to being the sample at the eval instant:
		// the subquery instants are whole hours, so they land on the 15-min bucket grid
		// and the snapped point is that exact raw sample (see aggSnapQuery). Any of the
		// three failing means the snapped value stopped being the raw sample — 50 is the
		// canary, it sits exactly on the wave's 50-crossings where a bucket max reads
		// 55.1 and a bucket average 52.6.
		{
			name:           "complex_subquery3_thr42",
			query:          `count_over_time((disk_used_perc{instance="server-1"} > 42)[6h:1h])`,
			window:         windowMiddleOfDataset,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			checkExpected:  true,
			expected:       4,
			skipIfBackends: []string{"cassandra"},
		},
		{
			// On the crossings the raw sample is 50 to float noise, so raw itself counts 2
			// to 4 points depending on that noise — no checkExpected, but the aggregate has
			// to agree with it point for point all the same.
			name:           "complex_subquery3_thr50",
			query:          `count_over_time((disk_used_perc{instance="server-1"} > 50)[6h:1h])`,
			window:         windowMiddleOfDataset,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"},
		},
		{
			// 32.7 sits just above the wave trough (32.679), the other threshold a bucket
			// extremum gets wrong (its bucket max reads 35.8 there).
			name:           "complex_subquery3_thr32.7",
			query:          `count_over_time((disk_used_perc{instance="server-1"} > 32.7)[6h:1h])`,
			window:         windowMiddleOfDataset,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			checkExpected:  true,
			expected:       4,
			skipIfBackends: []string{"cassandra"},
		},
		{
			name:           "complex_subquery4",
			query:          `avg_over_time((disk_used_perc{instance="server-1"} > 50)[6h:15m])`,
			window:         windowMiddleOfDataset,
			wantSource:     "raw",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"},
		},
		{
			// The same 4 points as complex_subquery3_thr42, averaged instead of counted: it
			// checks their values, where the count only checks how many passed.
			name:           "complex_subquery5",
			query:          `avg_over_time((disk_used_perc{instance="server-1"} > 42)[6h:1h])`,
			window:         windowMiddleOfDataset,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			skipIfBackends: []string{"cassandra"},
		},
		{
			// complex_subquery3_thr42 on a window off the hour: the inner instants stay on whole
			// hours, so only the step-aligned half of the snap grid can serve them. With a
			// grid anchored on hints.Start alone every point falls a whole step short of an
			// instant, past the lookback, and the query returns nothing at all.
			name:           "complex_subquery6_unaligned_window",
			query:          `count_over_time((disk_used_perc{instance="server-1"} > 42)[6h:1h])`,
			window:         windowUnalignedWithSubqueryStep,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         roundingError,
			checkExpected:  true,
			expected:       4,
			skipIfBackends: []string{"cassandra"},
		},
		{
			// A subquery step that is not a multiple of the bucket: its instants drift
			// across the 15-min grid (70 min advances the offset by 10 min each step), so
			// only one in three lands on a bucket edge and the others read the previous
			// edge's sample, a few minutes stale. On the 50-crossings that is enough to
			// flip a point — this is the residue of the snapping error, measured.
			name:            "complex_subquery7_step70m",
			query:           `count_over_time((disk_used_perc{instance="server-1"} > 50)[6h:1h10m])`,
			window:          windowMiddleOfDataset,
			wantSource:      "aggregated",
			strategy:        cmpClose,
			relTol:          roundingError,
			skipIfBackends:  []string{"cassandra"},
			knownLimitation: limitationSnapIsBucketAligned,
		},
		// The three probe_* cases are the Bleemeo status-page queries, verbatim but for the
		// label matcher, at the scale they run at in production ([30d:60s] subqueries
		// evaluated at 24h steps). They run there with ForceRaw; what these cases answer is
		// whether that header can go, so what matters is that the auto-routed read lands on
		// raw by itself and returns the same values. It does on the 60s subquery step being
		// below aggMinStep, and on the [300s] range being under two buckets — NOT on the
		// functions: a subquery selector reports Range 0 and loses its function name, see
		// TestSelectHintsRouting in clickhouse/tsdb.
		{
			// Incidents over the period: clamp_max collapses warning and critical into 1,
			// and increase() over that 0/1 series counts one step per episode whatever its
			// length. Every step sees the same outages, hence one expected count.
			name:          "probe_status incident count",
			query:         `round(increase(clamp_max(probe_status{instance="server-1"}, 1)[30d:60s]))`,
			window:        probeStatusPageWindow,
			wantSource:    "raw",
			strategy:      cmpExact,
			expected:      probeEpisodes,
			expectedTol:   probeExpectedTol,
			checkExpected: true,
		},
		{
			// Uptime over the period: the share of samples that are not OK, warnings
			// included. The expectation is computed from the outage table, so its tolerance
			// is float noise and nothing more.
			name: "probe_status uptime",
			query: `((1 - count_over_time((probe_status{instance="server-1"} > 0)[30d:60s])` +
				` / count_over_time(probe_status{instance="server-1"}[30d:60s])) * 100)`,
			window:        probeStatusPageWindow,
			wantSource:    "raw",
			strategy:      cmpExact,
			expected:      probeUptimePercent,
			expectedTol:   probeExpectedTol,
			checkExpected: true,
		},
		{
			// When the incidents happened: a 5-minute max at the 60s step the timeline is
			// drawn at. The window brackets the long outage, so the compared series carries
			// both OK and critical instead of a flat line.
			name:       "probe_status incident timeline",
			query:      `max_over_time(probe_status{instance="server-1"}[300s])`,
			window:     probeIncidentWindow,
			wantSource: "raw",
			strategy:   cmpExact,
		},
		{
			// last_over_time reads the per-bucket last value, so it is exact whenever the
			// [range] ends on a bucket edge — every step of this window does.
			name:           "last_over_time",
			query:          `last_over_time(cpu_used{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpExact,
			skipIfBackends: []string{"cassandra"}, // Cassandra's aggregate has no last value
		},
	}

	var firstError error

	for _, c := range cases {
		skipped := slices.Contains(c.skipIfBackends, backend)

		// A skipped case is still RUN — to measure and log its relative error for
		// cross-backend comparison (e.g. where Cassandra works but is excluded because
		// its error is too large) — but never fails the run.
		relErr, err := runAggCase(ctx, c, backend, rawAPI, natAPI, squirrelDBURL)

		switch {
		case skipped:
			log.Info().Msgf("agg matrix case SKIPPED on %s: %s%s", backend, c.name, relErrorMessage(relErr, c))
		case c.knownLimitation != "" && errors.Is(err, errAggRawDiffer):
			log.Warn().Msgf("agg matrix case KNOWN LIMITATION: %s%s: %s (%s)",
				c.name, relErrorMessage(relErr, c), c.knownLimitation, err)
		case err != nil:
			log.Error().Msgf("agg matrix case KO: %s%s: %s", c.name, relErrorMessage(relErr, c), err)

			if firstError == nil {
				firstError = err
			}
		case c.knownLimitation != "":
			log.Warn().Msgf("agg matrix case %s now matches raw%s: drop its knownLimitation (%s)",
				c.name, relErrorMessage(relErr, c), c.knownLimitation)
		default:
			log.Info().Msgf("agg matrix case OK: %s (source=%s)%s", c.name, c.wantSource, relErrorMessage(relErr, c))
		}
	}

	log.Info().Msgf("End of raw-vs-aggregate matrix (backend=%s)", backend)

	return firstError
}

// runQuery evaluates query over w and flattens the single result series to a
// timestamp(ms)->value map. w decides instant (api.Query) vs range (api.QueryRange).
func runQuery(ctx context.Context, api v1.API, query string, w evalWindow) (map[int64]float64, error) {
	if w.instant() {
		values, _, _, err := api.Query(ctx, query, w.end)
		if err != nil {
			return nil, err
		}

		return seriesToMap(values)
	}

	values, _, _, err := api.QueryRange(ctx, query, v1.Range{Start: w.start, End: w.end, Step: w.step})
	if err != nil {
		return nil, err
	}

	return seriesToMap(values)
}

// runAggCase runs one case and returns the measured max relative error vs raw (for
// the caller to log so runs can be compared).
func runAggCase(
	ctx context.Context, c aggCase, backend string, rawAPI, natAPI v1.API, squirrelDBURL string,
) (float64, error) {
	window := c.window

	// Warm up until the auto-routed query is consistent (WAL drained, MV populated).
	err := retryUntilConsistent(ctx, func() error {
		_, err := runQuery(ctx, natAPI, c.query, window)

		return err
	})
	if err != nil {
		return 0, fmt.Errorf("case %s: %w", c.name, err)
	}

	// Reference (raw) first, so its source increment stays outside the window below.
	raw, err := runQuery(ctx, rawAPI, c.query, window)
	if err != nil {
		return 0, fmt.Errorf("case %s raw: %w", c.name, err)
	}

	// Confirm the auto-routed read actually hit the expected source, via the
	// backend-specific read-source metric (see aggregatedSourceMetric).
	sourceName, sourceLabels := aggregatedSourceMetric(backend, c.wantSource)

	var before float64

	body, err := scrapeMetrics(ctx, squirrelDBURL)
	if err != nil {
		return 0, err
	}

	before = counterValue(body, sourceName, sourceLabels)

	agg, err := runQuery(ctx, natAPI, c.query, window)
	if err != nil {
		return 0, fmt.Errorf("case %s aggregate: %w", c.name, err)
	}

	body, err = scrapeMetrics(ctx, squirrelDBURL)
	if err != nil {
		return 0, err
	}

	relErr, cmpErr := compareResults(c, raw, agg)

	// The source check comes first even when the comparison failed: reading the wrong
	// source explains any value difference, and a case with a knownLimitation tolerates
	// only the difference, never the wrong source.
	if delta := counterValue(body, sourceName, sourceLabels) - before; delta < 1 {
		return relErr, fmt.Errorf("%w: %s wanted source %q (delta=%v)",
			errUnexpectedSource, c.name, c.wantSource, delta)
	}

	return relErr, cmpErr
}

// compareResults checks the aggregate read against raw per the case strategy and
// returns the measured max relative error vs raw (the number one would put in
// relTol) — reported by the caller so successive runs can be compared.
func compareResults(c aggCase, raw, agg map[int64]float64) (float64, error) {
	// The aggregate must return a point at exactly the raw eval steps; a snapping /
	// lookback regression surfaces here as missing steps.
	if len(raw) != len(agg) {
		return 0, fmt.Errorf("%w: %s raw has %d points, aggregate has %d", errCoverage, c.name, len(raw), len(agg))
	}

	for ts := range raw {
		if _, ok := agg[ts]; !ok {
			return 0, fmt.Errorf("%w: %s aggregate missing a point at %d", errCoverage, c.name, ts)
		}
	}

	relErr := maxRelError(raw, agg)

	// Fixed expectation validates the raw result itself (and thus the write).
	if c.checkExpected {
		tol := c.expectedTol
		if tol == 0 {
			tol = defaultExpectedTol
		}

		for ts, v := range raw {
			if math.Abs(v-c.expected) > tol*math.Abs(c.expected)+1e-9 {
				return relErr, fmt.Errorf("%w: %s raw=%v want~%v at %d", errUnexpectedRawValue, c.name, v, c.expected, ts)
			}
		}
	}

	switch c.strategy {
	case cmpExact:
		for ts, v := range agg {
			if math.Abs(v-raw[ts]) > 1e-9 {
				return relErr, fmt.Errorf("%w: %s agg=%v raw=%v must be identical at %d", errAggRawDiffer, c.name, v, raw[ts], ts)
			}
		}
	case cmpClose:
		for ts, v := range agg {
			if math.Abs(v-raw[ts]) > c.relTol*math.Abs(raw[ts])+1e-9 {
				return relErr, fmt.Errorf("%w: %s agg=%v raw=%v relTol=%v at %d", errAggRawDiffer, c.name, v, raw[ts], c.relTol, ts)
			}
		}
	}

	return relErr, nil
}

// maxRelError is the largest |agg-raw|/|raw| over the shared points, skipping points
// where raw is ~0 (relative error is undefined there). It is what one would set relTol
// to for the case to just pass.
func maxRelError(raw, agg map[int64]float64) float64 {
	worst := 0.0

	for ts, a := range agg {
		r := raw[ts]
		if math.Abs(r) < 1e-9 {
			continue
		}

		if e := math.Abs(a-r) / math.Abs(r); e > worst {
			worst = e
		}
	}

	return worst
}

// relErrorMessage produce the relative error feedback, if applicable.
func relErrorMessage(x float64, c aggCase) string {
	if c.strategy == cmpExact && x == 0 {
		return ""
	}

	return ", relative error measured ~" + formatRelError(x)
}

// formatRelError rounds a relative error to one significant figure — an order of
// magnitude to eyeball and compare across runs (e.g. "0.04", "1e-15"), not exact
// digits. Returns "0" for an exact match.
func formatRelError(x float64) string {
	if x <= 0 {
		return "0"
	}

	mag := math.Pow(10, math.Floor(math.Log10(x)))

	return strconv.FormatFloat(math.Round(x/mag)*mag, 'g', 1, 64)
}

// seriesToMap flattens a one-series matrix or a one-element vector into a
// timestamp(ms)->value map.
func seriesToMap(v model.Value) (map[int64]float64, error) {
	switch val := v.(type) {
	case model.Matrix:
		if len(val) != 1 {
			return nil, fmt.Errorf("%w: got %d series, want 1", errUnexpectedSeries, len(val))
		}

		out := make(map[int64]float64, len(val[0].Values))
		for _, p := range val[0].Values {
			out[int64(p.Timestamp)] = float64(p.Value)
		}

		return out, nil
	case model.Vector:
		if len(val) != 1 {
			return nil, fmt.Errorf("%w: got %d series, want 1", errUnexpectedSeries, len(val))
		}

		return map[int64]float64{int64(val[0].Timestamp): float64(val[0].Value)}, nil
	default:
		return nil, fmt.Errorf("%w: unexpected value type %T", errUnexpectedSeries, v)
	}
}

// writeMatrixSeries adds metrics used runAggMatrix:
//   - service_status a discrete (values in {0,1}) metrics
//   - requests_total a monotonic counter
//   - counter_with_reset_total a counter that resets to 0 every 90 minutes
//   - cpu_used a gauge metrics, which had known min/avg/max value
//   - mem_used_perc two gauge metrics using sin and cos values in range [45,55] with
//     a period of 15 minutes.
//   - disk_used_perc two gauge metrics using sin and cos values in range [30,70] with
//     a period of 6 hours.
//
// They all span matrixSpan at matrixStep. probe_status is written separately: it is the
// one series needing a month of history (see writeProbeStatusSeries).
func writeMatrixSeries(ctx context.Context, t0 time.Time, squirrelDBURL string) error {
	step := matrixStep
	minTS := t0.Add(-matrixSpan)
	n := int(matrixSpan / step)

	// Mostly up with a periodic 0; every 15-min bucket still contains a 1, so the
	// snapped (bucket max) value is 1 while raw carries both 0 and 1.
	statusValues := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0}

	// Consecutive values of a metric. With one point per 10s, this is 5 minutes of points,
	// which also corresponds to the Cassandra aggregation size.
	// These values have an average of 1, a max of 2 and a min of 0 on 5 minutes (and the same on 15 mintues,
	// the Clickhouse aggregation size)
	cpuValues := []int{
		2, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 0,
	}

	req := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "service_status"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: makeSamples(minTS, step, statusValues, n/len(statusValues)+1),
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "requests_total"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: makeCounterSamples(minTS, step, n),
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "counter_with_reset_total"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: makeCounterWithResetSamples(minTS, step, n, int(90*time.Minute/step)),
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "cpu_used"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: makeSamples(minTS, step, cpuValues, n/len(cpuValues)+1),
			},
			// Two smooth gauges over [45, 55], one full period per 15-min bucket, so
			// over any aligned bucket min=45, max=55, avg=50 on both.
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "mem_used_perc"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: makeWaveSamples(minTS, step, 15*time.Minute, n, 50, 5, math.Sin),
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "mem_used_perc"},
					{Name: "instance", Value: "server-2"},
				},
				Samples: makeWaveSamples(minTS, step, 15*time.Minute, n, 50, 5, math.Cos),
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "disk_used_perc"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: makeWaveSamples(minTS, step, 6*time.Hour, n, 50, 20, math.Sin),
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "disk_used_perc"},
					{Name: "instance", Value: "server-2"},
				},
				Samples: makeWaveSamples(minTS, step, 6*time.Hour, n, 50, 20, math.Cos),
			},
		},
	}

	return writeSeries(ctx, req, squirrelDBURL)
}

// probe_status values, as a Bleemeo probe reports them.
const (
	probeOK       = 0
	probeWarning  = 1
	probeCritical = 2
)

// probeOutage is one period where probe_status is not OK. It starts atHour into the day
// beginning daysBefore days before t0 (days run 14:00 to 14:00, like t0 itself) and
// covers minutes consecutive samples.
type probeOutage struct {
	daysBefore int
	atHour     time.Duration
	minutes    int
	value      float64
}

func (o probeOutage) start(t0 time.Time) time.Time {
	return t0.Add(-time.Duration(o.daysBefore)*24*time.Hour + o.atHour)
}

func (o probeOutage) duration() time.Duration {
	return time.Duration(o.minutes) * probeStep
}

// probeLongOutage is singled out from the table below because the incident-timeline
// case brackets it: 90 minutes is longer than the four buckets aggMinStep spans, so a
// read routed to the aggregate could not hide it inside one bucket.
var probeLongOutage = probeOutage{daysBefore: 5, atHour: 4 * time.Hour, minutes: 90, value: probeCritical} //nolint:gochecknoglobals,lll

// probeOutages is every period where probe_status is not OK, in chronological order.
// They all sit inside (t0-30d, t0-2d], the span every step of the status-page window
// looks back over, so the episode count and the uptime are the same at every step.
// Day -15 is deliberately left out of the table: a full-OK day.
var probeOutages = []probeOutage{ //nolint:gochecknoglobals
	// Flapping: five isolated one-minute failures spread over a day, hours apart so each
	// is its own incident. One is a warning — which counts as an incident and as
	// downtime just like a critical, since both queries only test for "not OK".
	{daysBefore: 25, atHour: 2 * time.Hour, minutes: 1, value: probeCritical},
	{daysBefore: 25, atHour: 5 * time.Hour, minutes: 1, value: probeCritical},
	{daysBefore: 25, atHour: 8 * time.Hour, minutes: 1, value: probeWarning},
	{daysBefore: 25, atHour: 11 * time.Hour, minutes: 1, value: probeCritical},
	{daysBefore: 25, atHour: 13 * time.Hour, minutes: 1, value: probeCritical},
	// One 20-minute outage: consecutive samples, spanning two 15-min buckets at most.
	{daysBefore: 20, atHour: 6 * time.Hour, minutes: 20, value: probeCritical},
	// A warning-only episode.
	{daysBefore: 10, atHour: 9 * time.Hour, minutes: 3, value: probeWarning},
	// A short outage (4 minutes) but that don't start at aligned hours
	{daysBefore: 8, atHour: 1*time.Hour + 3*time.Minute, minutes: 4, value: probeCritical},
	probeLongOutage,
}

// probeExpectations derives what the status-page queries must return over a window
// covering every outage: increase() over the clamped series counts one step per
// episode, and the uptime is what the not-OK samples leave of the window's samples.
// Contiguous outages count as one episode (no drop between them for increase() to
// see), which is what keeps this honest if the table ever gains adjacent entries.
func probeExpectations(t0 time.Time) (episodes, uptimePercent float64) {
	notOKMinutes := 0
	episodeCount := 0

	var prevEnd time.Time

	for _, o := range probeOutages {
		if !o.start(t0).Equal(prevEnd) {
			episodeCount++
		}

		notOKMinutes += o.minutes
		prevEnd = o.start(t0).Add(o.duration())
	}

	windowSamples := int(probeLookback / probeStep)

	return float64(episodeCount), (1 - float64(notOKMinutes)/float64(windowSamples)) * 100
}

// writeProbeStatusSeries writes probe_status: a month of a 60s probe that is OK except
// during probeOutages. It is written on its own (not with the matrix series) because it
// is the one series needing a month of history — see probeSpan.
func writeProbeStatusSeries(ctx context.Context, t0 time.Time, squirrelDBURL string) error {
	samples, err := makeProbeSamples(t0)
	if err != nil {
		return err
	}

	req := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "probe_status"},
					{Name: "instance", Value: "server-1"},
				},
				Samples: samples,
			},
		},
	}

	return writeSeries(ctx, req, squirrelDBURL)
}

// waitProbeSeriesVisible blocks until every probe_status point written is queryable. A
// write only becomes visible once the backend has drained it, and the status-page cases
// check exact counts against the outage table — a partial read would fail them for the
// wrong reason, so this turns that race into an explicit "not all points are back yet".
func waitProbeSeriesVisible(ctx context.Context, rawAPI v1.API, t0 time.Time) error {
	// A count over the whole span, on raw points: the range is left-open, so the sample
	// on the far edge is not counted.
	query := fmt.Sprintf(`count_over_time(probe_status{instance="server-1"}[%dm])`, int(probeSpan/time.Minute))
	want := float64(probeSpan / probeStep)

	return retryUntilConsistent(ctx, func() error {
		values, err := runQuery(ctx, rawAPI, query, evalWindow{end: t0})
		if err != nil {
			return err
		}

		for _, got := range values {
			if got == want {
				return nil
			}

			return fmt.Errorf("%w: %v of %v points", errProbeIncomplete, got, want)
		}

		return fmt.Errorf("%w: no point at all", errProbeIncomplete)
	})
}

// makeProbeSamples emits probeSpan of samples ending on t0, OK unless an outage covers
// the sample. Outages are placed by timestamp, so moving the sampling interval does not
// move them; an outage falling outside the span is an error rather than a silent gap.
func makeProbeSamples(t0 time.Time) ([]prompb.Sample, error) {
	from := t0.Add(-probeSpan)
	samples := make([]prompb.Sample, int(probeSpan/probeStep)+1)

	for i := range samples {
		samples[i] = prompb.Sample{
			Value:     probeOK,
			Timestamp: from.Add(probeStep * time.Duration(i)).UnixMilli(),
		}
	}

	for _, o := range probeOutages {
		first := int(o.start(t0).Sub(from) / probeStep)
		if first < 0 || first+o.minutes > len(samples) {
			return nil, fmt.Errorf("%w: outage %d days before t0 is outside the written span",
				errOutageOutOfSpan, o.daysBefore)
		}

		for j := range o.minutes {
			samples[first+j].Value = o.value
		}
	}

	return samples, nil
}

// makeWaveSamples emits count points of center + amplitude*wave(angle), where angle
// advances 2π per period — a smooth gauge (wave = math.Sin / math.Cos) oscillating
// over [center-amplitude, center+amplitude].
func makeWaveSamples(
	fromTime time.Time, stepTime, period time.Duration, count int, //nolint:unparam
	center, amplitude float64, wave func(float64) float64, //nolint:unparam
) []prompb.Sample {
	samples := make([]prompb.Sample, count)

	for i := range samples {
		elapsed := stepTime * time.Duration(i)
		angle := 2 * math.Pi * float64(elapsed) / float64(period)

		samples[i] = prompb.Sample{
			Value:     center + amplitude*wave(angle),
			Timestamp: fromTime.Add(elapsed).UnixMilli(),
		}
	}

	return samples
}

// makeCounterSamples emits a monotonic counter: value i at the i-th step.
func makeCounterSamples(fromTime time.Time, stepTime time.Duration, count int) []prompb.Sample {
	samples := make([]prompb.Sample, count)
	for i := range samples {
		samples[i] = prompb.Sample{
			Value:     float64(i),
			Timestamp: fromTime.Add(stepTime * time.Duration(i)).UnixMilli(),
		}
	}

	return samples
}

// makeCounterWithResetSamples emits a counter that climbs by 1 each step and resets
// to 0 every resetEvery steps (a sawtooth) — exercising rate/increase reset handling
// through the aggregate (whose bucket max is the pre-reset peak).
func makeCounterWithResetSamples(fromTime time.Time, stepTime time.Duration, count, resetEvery int) []prompb.Sample {
	samples := make([]prompb.Sample, count)
	for i := range samples {
		samples[i] = prompb.Sample{
			Value:     float64(i % resetEvery),
			Timestamp: fromTime.Add(stepTime * time.Duration(i)).UnixMilli(),
		}
	}

	return samples
}

// headerRoundTripper sets fixed headers on every request — used to attach the
// ForceRaw header to the reference query.
type headerRoundTripper struct {
	base    http.RoundTripper
	headers map[string]string
}

func (rt headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, v := range rt.headers {
		req.Header.Set(k, v)
	}

	return rt.base.RoundTrip(req)
}

func newQueryAPI(squirrelDBURL string, headers map[string]string) (v1.API, error) {
	client, err := api.NewClient(api.Config{
		Address:      squirrelDBURL,
		RoundTripper: headerRoundTripper{base: api.DefaultRoundTripper, headers: headers},
	})
	if err != nil {
		return nil, fmt.Errorf("create Prometheus client: %w", err)
	}

	return v1.NewAPI(client), nil
}

// scrapeMetrics fetches the /metrics exposition text.
func scrapeMetrics(ctx context.Context, squirrelDBURL string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, squirrelDBURL+"/metrics", nil)
	if err != nil {
		return "", err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// counterValue returns the value of the sample named `name` carrying every label in
// `labels` (label order in the exposition doesn't matter), or 0 if absent.
func counterValue(body, name string, labels map[string]string) float64 {
	for line := range strings.SplitSeq(body, "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}

		// name must be followed by '{' (labelled) or a space (bare), so a longer
		// metric sharing this prefix isn't matched.
		if rest := line[len(name):]; rest == "" || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}

		matched := true

		for k, v := range labels {
			if !strings.Contains(line, k+`="`+v+`"`) {
				matched = false

				break
			}
		}

		if !matched {
			continue
		}

		fields := strings.Fields(line)

		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}

		return value
	}

	return 0
}

// aggregatedSourceMetric maps a case wantSource to the backend's read-source metric.
// ClickHouse counts Select calls by source; Cassandra counts read points by type
// (raw/aggregated, no snap — snap cases don't run on Cassandra). It panics on an
// unknown backend, since the matrix only runs on the two above.
func aggregatedSourceMetric(backend, wantSource string) (name string, labels map[string]string) {
	switch backend {
	case "clickhouse":
		return "squirreldb_clickhouse_read_source_total", map[string]string{"source": wantSource}
	case "cassandra":
		return "squirreldb_tsdb_requests_points_total", map[string]string{"operation": "read", "type": wantSource}
	default:
		panic("unknown backend")
	}
}
