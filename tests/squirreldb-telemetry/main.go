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

// Command squirreldb-telemetry exercises backend.TelemetryStore() end-to-end,
// against whichever backend is configured (Cassandra or ClickHouse; it skips on
// the dummy backend). It simulates N independent SquirrelDB processes (N separate
// daemon.SquirrelDB that share nothing but the underlying store) and checks the
// node-ID ownership contract:
//
//   - each process acquires a node ID; normally the N IDs are all distinct.
//     A same-ID collision at this first step is only a (very visible) warning,
//     not a failure — the contract is soft right after a simultaneous claim.
//   - after one refresh cycle, the N IDs MUST be distinct (no lingering conflict).
//   - N/2 processes stop gracefully (release the ID, then CloseAll the backend).
//   - N/2 fresh processes start immediately and re-acquire: the pool must not
//     grow — the freed IDs are reused, so the live set is again the original N
//     distinct IDs.
//
// It cannot exercise the crash case (a process dying without releasing): that
// path relies on the lease/lock TTL lapsing, which there is no way to force here.
//
// The store's ownership timing (lease refresh for ClickHouse, per-ID lock for
// Cassandra) is normally minutes; the test shortens it through the
// SquirrelDB.TelemetryStoreTiming test hook so a run takes ~1 minute.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core"
	"github.com/bleemeo/squirreldb/core/processsetup"
	"github.com/bleemeo/squirreldb/telemetry"
	"github.com/bleemeo/squirreldb/tests/common/testsetup"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

//nolint:gochecknoglobals
var (
	processes = flag.Int("test.processes", 4,
		"Number of simulated SquirrelDB processes (must be even and >= 2)")
	observeDelay = flag.Duration("test.observe-delay", 35*time.Second,
		"How long to wait after the initial claim before re-checking the IDs (> 2 refresh cycles)")
	restartObserveDelay = flag.Duration("test.restart-observe-delay", 20*time.Second,
		"How long to wait after the restart before the final ID check (> 1 refresh cycle)")
	overallTimeout = flag.Duration("test.timeout", 5*time.Minute,
		"Hard timeout for the whole test")
)

// storeTiming shortens the node-ID ownership timing so the test runs in ~1 min
// instead of minutes. Every backend reads only the fields it needs; setting all
// of them keeps a single struct valid for both Cassandra and ClickHouse.
//
// The lease/lock TTL must stay comfortably larger than the refresh interval so a
// live node never lets its lease lapse mid-test; observeDelay must in turn cover
// at least two refresh intervals.
//
//nolint:gochecknoglobals
var storeTiming = telemetry.StoreTiming{
	// ClickHouse (lease):
	RefreshInterval:  15 * time.Second,
	LeaseTTL:         45 * time.Second,
	ClaimSettleDelay: time.Second,
	// Cassandra (per-ID lock):
	IDLockTTL:        45 * time.Second,
	IDLockRetryDelay: 2 * time.Second,
}

func main() {
	flag.Parse()

	testsetup.SetTestEnvironment()

	err := processsetup.RunWithSignalHandler(run)

	if errors.Is(err, testsetup.ErrSkipBackend) {
		log.Info().Msg(err.Error())

		return
	}

	if err != nil {
		log.Fatal().Err(err).Msg("telemetry store test failed")
	}

	log.Info().Msg("telemetry store test succeeded")
}

func run(ctx context.Context, shutdownCtx context.Context) error {
	cfg, warnings, err := testsetup.LoadConfigForTest()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	if cfg.Backend == "dummy" {
		// The dummy backend's telemetry store is a no-op with no real IDs.
		return testsetup.ErrSkipBackend
	}

	if *processes < 2 || *processes%2 != 0 {
		return fmt.Errorf("test.processes must be even and >= 2, got %d", *processes)
	}

	ctx, cancel := context.WithTimeout(ctx, *overallTimeout)
	defer cancel()

	// Start from an empty pool so the reuse check is deterministic (no leftover
	// free slots from a previous run). A single dedicated instance drops and
	// recreates the schema on startup — concurrent keyspace/table creation across
	// several instances is not supported, so this must happen before any test
	// process starts.
	log.Info().Msg("Resetting the store (drop + recreate schema)")

	if err := resetStore(ctx, cfg); err != nil {
		return fmt.Errorf("reset store: %w", err)
	}

	n := *processes
	half := n / 2

	// Step 1: start the N processes and have each acquire a node ID. The first one
	// starts alone so it creates any lazily-created tables (the Cassandra lock/state
	// tables) before the rest start concurrently.
	log.Info().Msgf("Starting %d processes and acquiring a node ID each", n)

	procs, err := startProcesses(ctx, cfg, 0, 1)
	if err != nil {
		return err
	}

	// Closure (not a direct call) so cleanup stops whatever procs holds at return,
	// including the processes started later at the restart step. Already-stopped
	// processes are a no-op (process.stop is idempotent).
	defer func() { stopAll(shutdownCtx, procs) }()

	rest, err := startProcesses(ctx, cfg, 1, n-1)
	if err != nil {
		return err
	}

	procs = append(procs, rest...)

	// Step 2: right after the (concurrent) claim, IDs should be distinct — but a
	// collision here is only a soft violation, so warn instead of failing.
	initialIDs := currentIDs(procs)
	if dupes := duplicates(initialIDs); len(dupes) > 0 {
		log.Warn().Msgf("SOFT VIOLATION: two processes acquired the same telemetry ID right after claim: %v "+
			"(tolerated, should self-heal within one refresh cycle)", dupes)
	} else {
		log.Info().Msgf("Initial IDs are all distinct: %v", initialIDs)
	}

	// Step 3: after a refresh cycle, IDs MUST be distinct.
	log.Info().Msgf("Waiting %s (a few refresh cycles) then re-checking IDs", *observeDelay)
	sleep(ctx, *observeDelay)

	stableIDs := currentIDs(procs)
	if dupes := duplicates(stableIDs); len(dupes) > 0 {
		return fmt.Errorf("IDs still conflict after a refresh cycle: %v (ids=%v)", dupes, stableIDs)
	}

	log.Info().Msgf("After refresh, IDs are all distinct: %v", stableIDs)

	originalSet := toSet(stableIDs)

	// Step 4: stop the first half gracefully (release the ID, then close). This is
	// a clean shutdown — the freed IDs become immediately reusable.
	log.Info().Msgf("Stopping %d processes (graceful: release then CloseAll)", half)
	stopAll(ctx, procs[:half])

	// The kept processes keep their (still distinct) IDs.
	keptIDs := currentIDs(procs[half:])

	// Step 5: start half fresh processes immediately (no delay). They must reuse
	// the just-freed IDs, so the pool does not grow.
	log.Info().Msgf("Starting %d fresh processes immediately and re-acquiring", half)

	restarted, err := startProcesses(ctx, cfg, n, half)
	if err != nil {
		return err
	}

	// The live set is now the kept half plus the restarted half. Reassigning procs
	// makes the deferred cleanup stop exactly this set (the first half is already
	// stopped).
	live := make([]*process, 0, n)
	live = append(live, procs[half:]...)
	live = append(live, restarted...)
	procs = live

	liveIDs := currentIDs(procs)
	if dupes := duplicates(liveIDs); len(dupes) > 0 {
		return fmt.Errorf("after restart, live IDs conflict: %v (ids=%v)", dupes, liveIDs)
	}

	if got, want := toSet(liveIDs), originalSet; !sameSet(got, want) {
		return fmt.Errorf("after restart the ID pool changed: got %v, want the original set %v "+
			"(kept=%v, restarted reused nothing?)", liveIDs, stableIDs, keptIDs)
	}

	log.Info().Msgf("After restart, live IDs are the original %d distinct IDs: %v", n, liveIDs)

	// Step 6: after another refresh cycle, the live set must still be distinct.
	log.Info().Msgf("Waiting %s then doing a final ID check", *restartObserveDelay)
	sleep(ctx, *restartObserveDelay)

	finalIDs := currentIDs(procs)
	if dupes := duplicates(finalIDs); len(dupes) > 0 {
		return fmt.Errorf("final IDs conflict after restart+refresh: %v (ids=%v)", dupes, finalIDs)
	}

	if got, want := toSet(finalIDs), originalSet; !sameSet(got, want) {
		return fmt.Errorf("final ID pool changed: got %v, want %v", finalIDs, stableIDs)
	}

	log.Info().Msgf("Final IDs are stable and distinct: %v", finalIDs)

	return ctx.Err()
}

// process is one simulated SquirrelDB instance holding a telemetry node ID.
type process struct {
	idx     int
	sqDB    *core.SquirrelDB
	backend core.Backend
	cluster types.Cluster
	idFn    func() string
	release func()

	stopOnce sync.Once
}

// currentID reads the currently-owned ID afresh (the store may re-acquire a
// different ID after a split — see telemetry.Store.AcquireNodeID).
func (p *process) currentID() string {
	return p.idFn()
}

// stop performs a graceful shutdown: release the ID (frees the slot immediately),
// then close the backend and cluster. Idempotent.
func (p *process) stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		if p.release != nil {
			p.release()
		}

		if p.backend != nil {
			p.backend.CloseAll(ctx)
		}

		if p.cluster != nil {
			_ = p.cluster.Close()
		}
	})
}

// resetStore drops and recreates the backend schema on a single dedicated
// instance. Starting the backend with DropDataOnStartup drops the data and then
// recreates the schema, so afterwards the store is clean and its keyspace/database
// exists — which the test processes then rely on rather than racing to create.
func resetStore(ctx context.Context, cfg config.Config) error {
	sqDB := core.New(
		core.Deps{
			Logger: log.With().Str("component", "reset").Logger(),
			// A private registry per process avoids duplicate-registration panics when
			// several instances (and restarts) register the same backend metrics.
			MetricRegistry: prometheus.NewRegistry(),
		},
		cfg,
		core.SquirrelDBOption{
			InternalTesting: core.InternalTestingOption{DropDataOnStartup: true},
		},
	)

	backend, err := sqDB.Primary(ctx)
	if err != nil {
		return err
	}

	backend.CloseAll(ctx)

	return nil
}

// startProcesses spins up count processes concurrently, each acquiring a node ID.
// baseIdx offsets the process index/label so restarted processes never collide
// with earlier ones (in logs and metric registries). On any error every
// already-started process is stopped before returning.
func startProcesses(ctx context.Context, cfg config.Config, baseIdx, count int) ([]*process, error) {
	procs := make([]*process, count)

	var group errgroup.Group

	for i := range count {
		group.Go(func() error {
			p, err := startProcess(ctx, cfg, baseIdx+i)
			if err != nil {
				return fmt.Errorf("process %d: %w", baseIdx+i, err)
			}

			procs[i] = p

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		stopAll(context.Background(), procs) //nolint:contextcheck

		return nil, err
	}

	return procs, nil
}

// startProcess builds one independent daemon.SquirrelDB, starts its backend and
// acquires a telemetry node ID from it.
func startProcess(ctx context.Context, cfg config.Config, idx int) (*process, error) {
	sqDB := core.New(
		core.Deps{
			Logger: log.With().Str("component", "daemon").Int("process", idx).Logger(),
			// A private registry per process avoids duplicate-registration panics when
			// several instances (and restarts) register the same backend metrics.
			MetricRegistry: prometheus.NewRegistry(),
		},
		cfg,
		core.SquirrelDBOption{
			InternalTesting: core.InternalTestingOption{TelemetryStoreTiming: storeTiming},
		},
	)

	p := &process{idx: idx, sqDB: sqDB}

	cluster, err := sqDB.Cluster(ctx)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}

	p.cluster = cluster

	backend, err := sqDB.Primary(ctx)
	if err != nil {
		p.stop(ctx)

		return nil, fmt.Errorf("backend: %w", err)
	}

	p.backend = backend

	store, err := backend.TelemetryStore(ctx)
	if err != nil {
		p.stop(ctx)

		return nil, fmt.Errorf("telemetry store: %w", err)
	}

	idFn, release, err := store.AcquireNodeID(ctx)
	if err != nil {
		p.stop(ctx)

		return nil, fmt.Errorf("acquire node id: %w", err)
	}

	p.idFn = idFn
	p.release = release

	log.Info().Msgf("Process %d acquired telemetry ID %s", idx, idFn())

	return p, nil
}

func stopAll(ctx context.Context, procs []*process) {
	for _, p := range procs {
		if p != nil {
			p.stop(ctx)
		}
	}
}

// currentIDs reads the current ID of every process, in order.
func currentIDs(procs []*process) []string {
	ids := make([]string, len(procs))
	for i, p := range procs {
		ids[i] = p.currentID()
	}

	return ids
}

// duplicates returns the IDs that appear more than once.
func duplicates(ids []string) []string {
	count := make(map[string]int, len(ids))
	for _, id := range ids {
		count[id]++
	}

	var dup []string

	for id, c := range count {
		if c > 1 {
			dup = append(dup, id)
		}
	}

	sort.Strings(dup)

	return dup
}

func toSet(ids []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}

	return set
}

func sameSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}

	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}

	return true
}

// sleep waits for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
