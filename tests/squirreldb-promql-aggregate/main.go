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
	tMin := time.Now().Add(-48 * time.Hour)
	tMax := time.Now().Add(-24 * time.Hour)
	minTS := time.Date(tMin.Year(), tMin.Month(), tMin.Day(), 14, 0, 0, 0, time.UTC)
	maxTS := time.Date(tMax.Year(), tMax.Month(), tMax.Day(), 14, 0, 0, 0, time.UTC)

	return runAggMatrix(ctx, cfg.Backend, minTS, maxTS, squirrelDBURL)
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
	errValueOutOfSet      = errors.New("aggregate value is not an allowed discrete value")
	errAggRawDiffer       = errors.New("aggregate and raw differ beyond tolerance")
	errUnexpectedRawValue = errors.New("raw value differs from the fixed expectation")
)

// compareStrategy is how a case checks the aggregate read against the raw one.
type compareStrategy int

const (
	// cmpClose: each aggregate value is within relTol of the raw value (continuous
	// metrics and *_over_time / rate, which the aggregate reconstructs approximately).
	cmpClose compareStrategy = iota
	// cmpValuesInSet: each aggregate value is one of allowedSet — the check for an
	// instant vector served by snapping, whose value is the bucket's max (an actual
	// observed value), NOT a downsampled aggregate to compare against raw.
	cmpValuesInSet
	// cmpExact: aggregate equals raw. For functions that MUST fall back to raw
	// (count_over_time, changes, ...): a regression that re-routes them to the
	// aggregate would change the value and trip this.
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
	// allowedSet is the values the aggregate may return (cmpValuesInSet).
	allowedSet []float64
	// expected (when checkExpected) is a fixed value checked against the raw result,
	// validating raw itself.
	expected      float64
	checkExpected bool
	// skipIfBackends lists the backends this case does not apply to.
	skipIfBackends []string
}

// runAggMatrix writes the matrix' series then, for each case, reads it twice: raw
// via the ForceRaw header (the reference) and auto-routed (the path under test). On
// ClickHouse it confirms the auto-routed read hit the expected source (via
// read_source_total); it compares the two results per the case strategy on every
// backend. cpu_used is already written by write(); this adds the discrete and
// counter series. Cases that don't apply to a backend list it in skipIfBackends.
func runAggMatrix(ctx context.Context, backend string, minTS, maxTS time.Time, squirrelDBURL string) error { //nolint:maintidx,lll
	if err := writeMatrixSeries(ctx, minTS, maxTS, squirrelDBURL); err != nil {
		return err
	}

	// Cassandra aggregate is built by a background task every day. Explicitly run it now.
	if backend == "cassandra" {
		if err := preaggregate(ctx, squirrelDBURL, minTS, minTS.Add(24*time.Hour)); err != nil {
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

	// Default window: step >= aggMinStep (1h) so instant vectors auto-route to the
	// snapped aggregate, and [2h] range selectors span >= 2 buckets so they route to
	// the plain aggregate. It sits well inside the written data so both paths cover
	// every step. A case overrides it via its own window (e.g. an instant query).
	defaultWindow := evalWindow{
		start: minTS.Add(3 * time.Hour),
		end:   maxTS.Add(-time.Hour),
		step:  time.Hour,
	}

	// onlyClickHouse marks a case that exercises ClickHouse-specific behaviour: the
	// instant-vector snapping, and the raw-fallback correction — both absent from the
	// Cassandra pre-aggregation model.
	onlyClickHouse := []string{"cassandra"}

	cases := []aggCase{
		{
			name:          "min_over_time",
			query:         `min_over_time(cpu_used{instance="server-1"}[2h])`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        0.001,
			expected:      0,
			checkExpected: true,
		},
		{
			name:          "max_over_time",
			query:         `max_over_time(cpu_used{instance="server-1"}[2h])`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        0.001,
			expected:      2,
			checkExpected: true,
		},
		{
			name:           "avg_over_time",
			query:          `avg_over_time(cpu_used{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
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
			relTol:         0.001,
			skipIfBackends: onlyClickHouse, // Cassandra is broken here, value is ~30 times lower than expected.
		},
		{
			name:       "rate",
			query:      `rate(requests_total{instance="server-1"}[2h])`,
			window:     defaultWindow,
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     0.001,
		},
		{
			name:       "rate with resets",
			query:      `rate(counter_with_reset_total{instance="server-1"}[1h])`,
			window:     defaultWindow,
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     0.001,
		},
		{
			name:          "memory average",
			query:         `avg_over_time(mem_used_perc{instance="server-1"}[1h])`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        0.001,
			expected:      50,
			checkExpected: true,
		},
		{
			name:          "sum of memory average",
			query:         `sum(avg_over_time(mem_used_perc[1h]))`,
			window:        defaultWindow,
			wantSource:    "aggregated",
			strategy:      cmpClose,
			relTol:        0.001,
			expected:      100,
			checkExpected: true,
		},
		{
			name:           "sum of disk average (range)",
			query:          `sum(avg_over_time(disk_used_perc[1h]))`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			skipIfBackends: []string{"cassandra"}, // Cassandra had much higher relative error
		},
		{
			name:           "sum of disk average (instant)",
			query:          `sum(avg_over_time(disk_used_perc[1h]))`,
			window:         evalWindow{end: minTS.Add(time.Hour)},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			expected:       126.099, // run test to get the value
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "sum of disk average2",
			query:          `sum(avg_over_time(disk_used_perc[1h]))`,
			window:         evalWindow{end: minTS.Add(4 * time.Hour)},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			expected:       73.90,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 1h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[1h])`,
			window:         evalWindow{end: maxTS},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 3h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[3h])`,
			window:         evalWindow{end: maxTS},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 6h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[6h])`,
			window:         evalWindow{end: maxTS},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			expected:       50,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 9h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window:         evalWindow{end: maxTS},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:           "avg_over_time 12h",
			query:          `avg_over_time(disk_used_perc{instance="server-1"}[12h])`,
			window:         evalWindow{end: maxTS},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			expected:       50,
			checkExpected:  true,
			skipIfBackends: []string{"cassandra"}, // Cassandra don't support instant query routing
		},
		{
			name:       "avg_over_time query_range 9h",
			query:      `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window:     evalWindow{start: maxTS.Add(-5 * 9 * time.Hour), end: maxTS, step: 9 * time.Hour},
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     0.001,
		},
		{
			name:           "sum_over_time query_range 9h",
			query:          `sum_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window:         evalWindow{start: maxTS.Add(-5 * 9 * time.Hour), end: maxTS, step: 9 * time.Hour},
			wantSource:     "aggregated",
			strategy:       cmpExact,
			skipIfBackends: onlyClickHouse, // Cassandra is wrong by a x30 (it sum the pre-aggregated average)
		},
		{
			name:  "sum_over_time query_range 9h - offset 3 minutes",
			query: `sum_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window: evalWindow{
				start: maxTS.Add(-5 * 9 * time.Hour).Add(-3 * time.Minute),
				end:   maxTS.Add(-3 * time.Minute),
				step:  9 * time.Hour,
			},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.05,
			skipIfBackends: onlyClickHouse, // Cassandra is wrong by a x30 (it sum the pre-aggregated average)
		},
		{
			name:  "avg_over_time query_range 9h + offset 3 minutes",
			query: `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window: evalWindow{
				start: maxTS.Add(-5 * 9 * time.Hour).Add(3 * time.Minute),
				end:   maxTS.Add(3 * time.Minute),
				step:  9 * time.Hour,
			},
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     0.005,
		},
		{
			name:  "avg_over_time query_range 9h - offset 3 minutes",
			query: `avg_over_time(disk_used_perc{instance="server-1"}[9h])`,
			window: evalWindow{
				start: maxTS.Add(-5 * 9 * time.Hour).Add(-3 * time.Minute),
				end:   maxTS.Add(-3 * time.Minute),
				step:  9 * time.Hour,
			},
			wantSource: "aggregated",
			strategy:   cmpClose,
			relTol:     0.005,
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
			window:         evalWindow{end: maxTS},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.01,
			expected:       14.142,
			checkExpected:  true,
			skipIfBackends: onlyClickHouse,
		},
		{
			name:           "stdvar_over_time (max approximation)",
			query:          `stdvar_over_time(disk_used_perc{instance="server-1"}[6h])`,
			window:         evalWindow{end: maxTS},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.02, // variance ~ stddev², so ~2x the stddev relative error
			expected:       200,
			checkExpected:  true,
			skipIfBackends: onlyClickHouse,
		},
		{
			// An instant query (step 0) over a huge [range] must auto-route to the
			// aggregate rather than scan the whole raw window — the case
			// ForcePreAggregated used to cover manually.
			name:           "instant large range",
			query:          `avg_over_time(cpu_used{instance="server-1"}[20h])`,
			window:         evalWindow{end: maxTS.Add(-time.Hour)},
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.05,
			expected:       1,
			checkExpected:  true,
			skipIfBackends: onlyClickHouse,
		},
		{
			// present_over_time depends only on presence, so the aggregate serves it (returns 1).
			name:           "present_over_time",
			query:          `present_over_time(cpu_used{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.001,
			expected:       1,
			checkExpected:  true,
			skipIfBackends: onlyClickHouse,
		},
		{
			// Instant vectors are snapped to the eval grid; the value is the bucket
			// max, so check it is an actual observed value rather than comparing to raw.
			name:           "bare cpu_used",
			query:          `cpu_used{instance="server-1"}`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpValuesInSet,
			allowedSet:     []float64{0, 1, 2}, // because cpu_used is generated with only those 3 values
			skipIfBackends: onlyClickHouse,
		},
		{
			name:           "bare service_status (discrete)",
			query:          `service_status{instance="server-1"}`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpValuesInSet,
			allowedSet:     []float64{0, 1},
			skipIfBackends: onlyClickHouse,
		},
		{
			name:           "sum(cpu_used) instant agg",
			query:          `sum(cpu_used{instance="server-1"})`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpValuesInSet,
			allowedSet:     []float64{0, 1, 2},
			skipIfBackends: onlyClickHouse,
		},
		{
			// Must fall back to raw: count_over_time counts points, not values.
			name:           "count_over_time falls back to raw",
			query:          `count_over_time(cpu_used{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "raw",
			strategy:       cmpExact,
			skipIfBackends: onlyClickHouse,
		},
		{
			// Must fall back to raw: changes needs per-point order.
			name:           "changes falls back to raw",
			query:          `changes(service_status{instance="server-1"}[2h])`,
			window:         defaultWindow,
			wantSource:     "raw",
			strategy:       cmpExact,
			skipIfBackends: onlyClickHouse,
		},
		{
			name:           "predict_linear",
			query:          `predict_linear(cpu_used{instance="server-1"}[2h], 3600)`,
			window:         defaultWindow,
			wantSource:     "aggregated",
			strategy:       cmpClose,
			relTol:         0.002,
			skipIfBackends: onlyClickHouse, // Cassandra is not exact enough
		},
	}

	var firstError error

	for _, c := range cases {
		skipped := slices.Contains(c.skipIfBackends, backend)

		// A cmpValuesInSet skip is snapping-specific (no raw comparison to report), so
		// skip it outright. A cmpClose/cmpExact skip is still RUN — to measure and log
		// its relative error for cross-backend comparison (e.g. where Cassandra works
		// but is excluded because its error is too large) — but never fails the run.
		if skipped && c.strategy == cmpValuesInSet {
			log.Info().Msgf("agg matrix case SKIPPED on %s: %s", backend, c.name)

			continue
		}

		relErr, err := runAggCase(ctx, c, backend, rawAPI, natAPI, squirrelDBURL)

		switch {
		case skipped:
			log.Info().Msgf("agg matrix case SKIPPED on %s: %s%s", backend, c.name, relErrorMessage(relErr, c))
		case err != nil:
			log.Error().Msgf("agg matrix case KO: %s%s: %s", c.name, relErrorMessage(relErr, c), err)

			if firstError == nil {
				firstError = err
			}
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
		values, _, err := api.Query(ctx, query, w.end)
		if err != nil {
			return nil, err
		}

		return seriesToMap(values)
	}

	values, _, err := api.QueryRange(ctx, query, v1.Range{Start: w.start, End: w.end, Step: w.step})
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

	relErr, err := compareResults(c, raw, agg)
	if err != nil {
		return relErr, err
	}

	if delta := counterValue(body, sourceName, sourceLabels) - before; delta < 1 {
		return relErr, fmt.Errorf("%w: %s wanted source %q (delta=%v)",
			errUnexpectedSource, c.name, c.wantSource, delta)
	}

	return relErr, nil
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
		for ts, v := range raw {
			if math.Abs(v-c.expected) > 0.05*math.Abs(c.expected)+1e-9 {
				return relErr, fmt.Errorf("%w: %s raw=%v want~%v at %d", errUnexpectedRawValue, c.name, v, c.expected, ts)
			}
		}
	}

	switch c.strategy {
	case cmpValuesInSet:
		for ts, v := range agg {
			if !inSet(v, c.allowedSet) {
				return relErr, fmt.Errorf("%w: %s agg=%v not in %v at %d", errValueOutOfSet, c.name, v, c.allowedSet, ts)
			}
		}
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
	if c.strategy == cmpValuesInSet {
		return ""
	}

	if c.strategy == cmpExact && x == 0 {
		return ""
	}

	return ", relative error measured ~%s" + formatRelError(x)
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

func inSet(v float64, set []float64) bool {
	for _, s := range set {
		if math.Abs(v-s) < 1e-9 {
			return true
		}
	}

	return false
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
func writeMatrixSeries(ctx context.Context, minTS, maxTS time.Time, squirrelDBURL string) error {
	step := 10 * time.Second
	n := int(maxTS.Sub(minTS) / step)

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
