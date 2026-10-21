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

// Package routing provides a clickhouse-go driver.Conn that owns one underlying
// connection per host and routes every call to a healthy host.
//
// Why not a single multi-host clickhouse-go pool: that pool only knows a host is
// "down" when a connection fails to open or ping. A host that is up but missing
// our tables (a node rebuilt from empty) or whose replica is far behind (still
// resyncing) passes the ping yet serves errors or silently-incomplete reads, and
// roughly half the requests would hit it. By owning one conn per host we can run
// a semantic health check (reachable + tables present + replica not lagging) and
// route only to "ready" hosts — and, because a failed call is attributable to a
// specific host, demote it immediately instead of waiting for the next probe.
//
// A host is classified as:
//   - ready: reachable AND holds all expected tables, with an engine matching the
//     configured mode (replicated or not), AND replica lag < threshold;
//   - reachable: ping succeeds but not ready (no tables yet, or lagging);
//   - down: ping fails.
//
// Routing prefers ready hosts; it falls back to merely-reachable hosts only when
// none are ready (e.g. a fresh bootstrap before any table exists, so the schema
// can be created). With no reachable host, calls fail.
package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/bleemeo/squirreldb/clickhouse/ddl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// codeUnknownTable is the ClickHouse error code (UNKNOWN_TABLE) returned when a
// query targets a table that does not exist on the node it reached — the signal
// that the node is degraded (e.g. rebuilt empty), not that the query is wrong.
const codeUnknownTable = 60

// errUnknownHost is returned by OnHost for an address this Conn does not own.
var errUnknownHost = errors.New("routing: address is not a configured ClickHouse host")

// Defaults for the health monitor.
const (
	DefaultProbeInterval = 5 * time.Second
	DefaultLagThreshold  = 30 * time.Second
	probeTimeout         = 3 * time.Second
)

// Query-concurrency limiter (see Conn.sem / acquireRead / acquireWrite).
const (
	// minQueryBudget is the smallest total (read+write) budget: at least one slot
	// each for a read and a write.
	minQueryBudget = 2
	// autoQueryBudgetMargin is subtracted from ClickHouse's detected
	// max_concurrent_queries when the budget is auto-detected, leaving headroom for
	// the health probes (which bypass the limiter) and any other client of that node.
	autoQueryBudgetMargin = 4
)

// Options configures a routing Conn.
type Options struct {
	Addresses []string
	Auth      clickhouse.Auth
	Settings  clickhouse.Settings
	// Compression, when set, is applied to every per-host connection (native
	// protocol block compression). LZ4 markedly cuts the wire size — and the
	// flush latency — of the large, repetitive INSERT blocks the write path sends.
	Compression *clickhouse.Compression
	// ExpectedTables are the tables a host must hold to be considered ready. When
	// empty, the table check is skipped (a reachable host is ready).
	ExpectedTables []ExpectedTable
	// ClusterName, when set, is used to warn (at startup) if the number of
	// configured addresses does not match the cluster's node count, and to decide
	// which engine the expected tables must have (see checkTables). It is never
	// used to discover addresses (that would break NAT/proxy setups).
	ClusterName   string
	ProbeInterval time.Duration // default DefaultProbeInterval
	LagThreshold  time.Duration // default DefaultLagThreshold
	// MaxConcurrentQueries is the total (read+write) budget of in-flight queries
	// this Conn sends, so it never trips ClickHouse's max_concurrent_queries (which
	// would surface as an error). Reads are capped at budget-1 so a write always has
	// a slot. 0 auto-detects from the server's max_concurrent_queries minus a margin
	// (works for 1:1 non-shared setups; set it explicitly for shared servers or an
	// N-SquirrelDB/M-ClickHouse fan-out). A value below minQueryBudget is raised to
	// it; when auto-detect finds an unlimited server (0), no limit is applied.
	MaxConcurrentQueries int
	// Registerer, when set, receives per-host health gauges.
	Registerer prometheus.Registerer
	Logger     zerolog.Logger
}

// ExpectedTable is a table a host must hold to be ready, and whether it is one of
// the MergeTree-family tables the schema replicates in cluster mode (mirrors
// ddl.Object.Replicated). Only those have their engine checked, against
// Options.ClusterName.
type ExpectedTable struct {
	Name       string
	Replicated bool
}

type host struct {
	addr string
	conn driver.Conn

	mu        sync.RWMutex
	reachable bool
	ready     bool
	// cause is the last reason the host is not fully healthy: the ping error when
	// unreachable, or the readiness reason (missing tables, lagging) when reachable
	// but not ready. Empty when ready. Surfaced in pick's error and the debug
	// endpoint so an operator sees WHY a host is excluded (e.g. bad credentials).
	cause string
}

func (h *host) state() (reachable, ready bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.reachable, h.ready
}

func (h *host) lastCause() string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.cause
}

func (h *host) set(reachable, ready bool, causeMessage string, logger zerolog.Logger) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch {
	case h.reachable != reachable && !reachable:
		if causeMessage != "" {
			logger.Warn().Str("host", h.addr).Msgf("Clickhouse host become unreachable: %s", causeMessage)
		} else {
			logger.Warn().Str("host", h.addr).Msg("Clickhouse host become unreachable")
		}
	// unreachable -> reachable-but-not-ready is intentionally NOT logged: at first
	// startup (tables not created yet) it is the normal path and only adds noise.
	// /debug/<backend>/host shows the live state if needed.
	case h.reachable != reachable && reachable && ready:
		if causeMessage != "" {
			logger.Info().Str("host", h.addr).Msgf("Clickhouse host become reachable and ready: %s", causeMessage)
		} else {
			logger.Info().Str("host", h.addr).Msg("Clickhouse host become reachable and ready")
		}
	case h.ready != ready && !ready:
		if causeMessage != "" {
			logger.Warn().Str("host", h.addr).Msgf("Clickhouse host become unready: %s", causeMessage)
		} else {
			logger.Warn().Str("host", h.addr).Msg("Clickhouse host become unready")
		}
	case h.ready != ready && ready:
		if causeMessage != "" {
			logger.Info().Str("host", h.addr).Msgf("Clickhouse host become ready: %s", causeMessage)
		} else {
			logger.Info().Str("host", h.addr).Msg("Clickhouse host become ready")
		}
	}

	h.reachable, h.ready = reachable, ready
	h.cause = causeMessage
}

// Conn is a driver.Conn routing to healthy hosts. It is safe for concurrent use.
type Conn struct {
	opts   Options
	hosts  []*host
	logger zerolog.Logger

	rr            atomic.Uint64
	probeNow      chan struct{}
	cancelMonitor context.CancelFunc
	wg            sync.WaitGroup

	// sem bounds total in-flight queries; readSem (size len(sem)-1) additionally
	// caps reads so a write always finds a free sem slot. Both nil = no limit. A
	// read acquires readSem then sem; a write acquires sem only. Slots are held for
	// the whole query life (until Rows.Close / Batch Send/Abort/Close).
	sem       chan struct{}
	readSem   chan struct{}
	queryWait *prometheus.CounterVec // times an acquisition had to block, by kind

	registerer prometheus.Registerer
	collector  prometheus.Collector
}

var _ driver.Conn = (*Conn)(nil)

// New opens one connection per address, runs an initial health probe so the
// first calls see a populated health view, and starts the background monitor.
func New(ctx context.Context, opts Options) (*Conn, error) {
	if len(opts.Addresses) == 0 {
		return nil, errors.New("routing: no ClickHouse addresses configured")
	}

	if opts.ProbeInterval <= 0 {
		opts.ProbeInterval = DefaultProbeInterval
	}

	if opts.LagThreshold <= 0 {
		opts.LagThreshold = DefaultLagThreshold
	}

	monitorCtx, cancelMontor := context.WithCancel(context.Background())

	c := &Conn{
		opts:          opts,
		logger:        opts.Logger,
		probeNow:      make(chan struct{}, 1),
		cancelMonitor: cancelMontor,
	}

	for _, addr := range opts.Addresses {
		hc, err := clickhouse.Open(&clickhouse.Options{
			Addr:        []string{addr},
			Auth:        opts.Auth,
			Settings:    opts.Settings,
			Compression: opts.Compression,
		})
		if err != nil {
			cancelMontor()

			_ = c.Close()

			return nil, fmt.Errorf("routing: open %s: %w", addr, err)
		}

		c.hosts = append(c.hosts, &host{addr: addr, conn: hc})
	}

	c.probeAll(ctx)
	c.warnClusterSize(ctx)

	if err := c.setupQueryLimit(ctx); err != nil {
		cancelMontor()

		_ = c.Close()

		return nil, err
	}

	c.registerMetrics()

	c.wg.Add(1)

	go c.monitor(monitorCtx) //nolint:contextcheck

	return c, nil
}

// setupQueryLimit resolves the total query budget and builds the semaphores (left
// nil when unlimited). Called once at startup, after probeAll so a host is
// available for auto-detection.
//
// It fails when the budget must be auto-detected but no ClickHouse host answers:
// starting with the limiter disabled would let SquirrelDB overrun ClickHouse's
// max_concurrent_queries. Failing here is deliberate — at first startup ClickHouse
// may simply not be up yet, and the caller retries New (e.g. the process exits and
// is restarted). It never fails once running: the budget is resolved a single time
// here and left untouched afterwards, so a host that dies later never tears down
// the (already installed) limiter or crashes SquirrelDB.
func (c *Conn) setupQueryLimit(ctx context.Context) error {
	budget, err := c.resolveQueryBudget(ctx)
	if err != nil {
		return err
	}

	if budget <= 0 {
		c.logger.Info().Msg("ClickHouse query-concurrency limit disabled (unlimited)")

		return nil
	}

	c.sem = make(chan struct{}, budget)
	c.readSem = make(chan struct{}, budget-1) // budget >= minQueryBudget (2) so this is >= 1

	c.logger.Info().Int("budget", budget).Int("read_budget", budget-1).
		Msg("ClickHouse query-concurrency limit enabled")

	return nil
}

// resolveQueryBudget returns the total read+write query budget: the configured
// value (raised to minQueryBudget), or one auto-detected from the server. A
// configured value needs no reachable host. Auto-detection returns 0 ("no limit")
// only when the server itself reports an unlimited max_concurrent_queries; it
// errors when no host could be reached to read the setting, so the caller retries
// rather than silently running unlimited.
func (c *Conn) resolveQueryBudget(ctx context.Context) (int, error) {
	if c.opts.MaxConcurrentQueries > 0 {
		return max(minQueryBudget, c.opts.MaxConcurrentQueries), nil
	}

	serverLimit, err := c.serverMaxConcurrentQueries(ctx)
	if err != nil {
		return 0, fmt.Errorf(
			"auto-detect max_concurrent_queries (set clickhouse.max_concurrent_queries to skip auto-detection): %w",
			err)
	}

	if serverLimit == 0 {
		return 0, nil // server is unlimited -> we don't limit either
	}

	return max(minQueryBudget, serverLimit-autoQueryBudgetMargin), nil
}

// serverMaxConcurrentQueries reads the server's max_concurrent_queries setting from
// a reachable host. It is a per-server setting; on a cluster of identically
// configured nodes any host answers the same. Note it does NOT reflect per-user
// limits (max_concurrent_queries_for_user) — configure the budget explicitly then.
func (c *Conn) serverMaxConcurrentQueries(ctx context.Context) (int, error) {
	h, err := c.pick()
	if err != nil {
		return 0, err
	}

	var value string

	err = h.conn.QueryRow(ctx,
		"SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'",
	).Scan(&value)
	if err != nil {
		return 0, fmt.Errorf("read max_concurrent_queries: %w", err)
	}

	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse max_concurrent_queries %q: %w", value, err)
	}

	return n, nil
}

// acquireRead reserves a read slot (readSem then sem) or returns ctx.Err(). No-op
// when unlimited. Reads are capped at budget-1 (readSem) so a write always finds a
// free sem slot.
func (c *Conn) acquireRead(ctx context.Context) error {
	if c.sem == nil {
		return nil
	}

	if err := c.acquire(ctx, c.readSem, "read"); err != nil {
		return err
	}

	if err := c.acquire(ctx, c.sem, "read"); err != nil {
		<-c.readSem // release the readSem slot taken above

		return err
	}

	return nil
}

func (c *Conn) releaseRead() {
	if c.sem == nil {
		return
	}

	<-c.sem
	<-c.readSem
}

// acquireWrite reserves a write slot (sem only) or returns ctx.Err(). No-op when
// unlimited.
func (c *Conn) acquireWrite(ctx context.Context) error {
	if c.sem == nil {
		return nil
	}

	return c.acquire(ctx, c.sem, "write")
}

func (c *Conn) releaseWrite() {
	if c.sem == nil {
		return
	}

	<-c.sem
}

// acquire sends on sem, counting a wait if it can't be taken immediately.
func (c *Conn) acquire(ctx context.Context, sem chan struct{}, kind string) error {
	select {
	case sem <- struct{}{}:
		return nil
	default:
	}

	if c.queryWait != nil {
		c.queryWait.WithLabelValues(kind).Inc()
	}

	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// warnClusterSize logs a warning when the number of configured addresses does
// not match the node count ClickHouse reports for the cluster — a hint that
// SquirrelDB's load-balancing is missing nodes (or has stale ones). It never
// changes behaviour; it only warns. Best-effort.
func (c *Conn) warnClusterSize(ctx context.Context) {
	if c.opts.ClusterName == "" {
		return
	}

	h, err := c.pick()
	if err != nil {
		return
	}

	var nodes uint64

	if err := h.conn.QueryRow(ctx,
		"SELECT count() FROM system.clusters WHERE cluster = ?", c.opts.ClusterName,
	).Scan(&nodes); err != nil {
		return
	}

	if nodes != 0 && int(nodes) != len(c.opts.Addresses) {
		c.logger.Warn().Msgf(
			"ClickHouse cluster %q has %d node(s) but %d address(es) are configured; SquirrelDB "+
				"load-balances only over the configured addresses — list every node for full HA",
			c.opts.ClusterName, nodes, len(c.opts.Addresses))
	}
}

func (c *Conn) registerMetrics() {
	if c.opts.Registerer == nil {
		return
	}

	col := newHealthCollector(c)
	if err := c.opts.Registerer.Register(col); err != nil {
		c.logger.Warn().Err(err).Msg("could not register ClickHouse host health metrics")

		return
	}

	c.registerer = c.opts.Registerer
	c.collector = col

	// Only meaningful when the limiter is enabled; a non-zero rate means SquirrelDB
	// is throttling itself to stay under ClickHouse's max_concurrent_queries.
	if c.sem != nil {
		c.queryWait = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "squirreldb",
			Subsystem: "clickhouse",
			Name:      "query_wait_total",
			Help:      "Query acquisitions that had to wait for a concurrency slot, by kind (read/write)",
		}, []string{"kind"})
		if err := c.opts.Registerer.Register(c.queryWait); err != nil {
			c.logger.Warn().Err(err).Msg("could not register ClickHouse query-wait metric")

			c.queryWait = nil
		} else {
			// Initialize both series to 0 so they are exposed even before the first
			// wait (a CounterVec emits no series for an untouched label set).
			c.queryWait.WithLabelValues("read")
			c.queryWait.WithLabelValues("write")
		}
	}
}

// AnyReachable reports whether at least one host currently answers a ping.
func (c *Conn) AnyReachable() bool {
	for _, h := range c.hosts {
		if r, _ := h.state(); r {
			return true
		}
	}

	return false
}

// HostStatus is the current health of one ClickHouse host.
type HostStatus struct {
	Address   string `json:"address"`
	Reachable bool   `json:"reachable"`
	Ready     bool   `json:"ready"`
	// Cause is the reason the host is not fully healthy (ping error, missing tables,
	// lagging); empty when ready.
	Cause string `json:"cause,omitempty"`
}

// Hosts returns the current health of every configured host (for introspection,
// e.g. a debug endpoint).
func (c *Conn) Hosts() []HostStatus {
	statuses := make([]HostStatus, 0, len(c.hosts))

	for _, h := range c.hosts {
		reachable, ready := h.state()
		statuses = append(statuses, HostStatus{
			Address:   h.addr,
			Reachable: reachable,
			Ready:     ready,
			Cause:     h.lastCause(),
		})
	}

	return statuses
}

// pick returns a host to route a call to: a ready one if any, otherwise a
// merely-reachable one (so a fresh cluster can still create its schema), in
// round-robin order. It errors when no host is reachable.
func (c *Conn) pick() (*host, error) {
	var ready, reachable []*host

	for _, h := range c.hosts {
		r, rdy := h.state()
		if rdy {
			ready = append(ready, h)
		}

		if r {
			reachable = append(reachable, h)
		}
	}

	pool := ready
	if len(pool) == 0 {
		pool = reachable
	}

	if len(pool) == 0 {
		return nil, c.noReachableHostError()
	}

	idx := int(c.rr.Add(1)-1) % len(pool) //nolint:gosec

	return pool[idx], nil
}

// noReachableHostError builds the "no reachable host" error, appending each host's
// last cause (e.g. "code: 516 ... AUTHENTICATION_FAILED", "connection refused") so
// the reason is visible wherever the error surfaces — instead of a bare message
// that hides bad credentials or a wrong address.
func (c *Conn) noReachableHostError() error {
	causes := make([]string, 0, len(c.hosts))

	for _, h := range c.hosts {
		if cause := h.lastCause(); cause != "" {
			causes = append(causes, fmt.Sprintf("%s: %s", h.addr, cause))
		}
	}

	if len(causes) == 0 {
		return errors.New("routing: no reachable ClickHouse host")
	}

	return fmt.Errorf("routing: no reachable ClickHouse host (%s)", strings.Join(causes, "; "))
}

// pickSticky is like pick but DETERMINISTIC: it returns the first ready host (in
// configured-address order), else the first reachable one, else errors. Because
// every node shares the same address list, they converge on the same host — used
// by Sticky() so the telemetry lease sees a single, linearizable replica.
func (c *Conn) pickSticky() (*host, error) {
	var firstReachable *host

	for _, h := range c.hosts {
		reachable, ready := h.state()
		if ready {
			return h, nil
		}

		if reachable && firstReachable == nil {
			firstReachable = h
		}
	}

	if firstReachable != nil {
		return firstReachable, nil
	}

	return nil, c.noReachableHostError()
}

// observe reacts to the error of a call routed to h: a connection-level error
// demotes the host to down, an UNKNOWN_TABLE demotes it to reachable-not-ready,
// and both trigger an out-of-band re-probe so the classification is refreshed
// within a request rather than at the next tick. Context errors and ordinary
// query-level server exceptions leave the host's health untouched.
func (c *Conn) observe(h *host, err error) {
	if err == nil {
		return
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	if ex, ok := errors.AsType[*proto.Exception](err); ok {
		if ex.Code == codeUnknownTable {
			h.set(true, false, "query failed with UNKNOWN_TABLE", c.logger)
			c.triggerProbe()
		}

		return
	}

	// Non-exception error: treat as a connection/network failure.
	h.set(false, false, "query failed with non-exception error", c.logger)
	c.triggerProbe()
}

func (c *Conn) triggerProbe() {
	select {
	case c.probeNow <- struct{}{}:
	default:
	}
}

func (c *Conn) monitor(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.opts.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probeAll(ctx)
		case <-c.probeNow:
			c.probeAll(ctx)
		}
	}
}

func (c *Conn) probeAll(ctx context.Context) {
	var wg sync.WaitGroup

	for _, h := range c.hosts {
		wg.Add(1)

		go func(h *host) {
			defer wg.Done()

			c.probe(ctx, h)
		}(h)
	}

	wg.Wait()
}

// probe refreshes a single host's health and logs transitions.
func (c *Conn) probe(parent context.Context, h *host) {
	ctx, cancel := context.WithTimeout(parent, probeTimeout)
	defer cancel()

	if err := h.conn.Ping(ctx); err != nil {
		// Record the ping error as the cause so pick's error and the debug endpoint
		// can show WHY the host is down (bad credentials, refused connection, ...) —
		// otherwise a never-reachable host surfaces only as the generic "no reachable
		// host" (no state transition to log at startup).
		h.set(false, false, err.Error(), c.logger)

		return
	}

	missing, mismatched, err := c.checkTables(ctx, h)
	if err != nil {
		// Reachable, but we cannot determine readiness: keep it out of the ready
		// set without logging a scary transition.
		h.set(true, false, err.Error(), c.logger)

		return
	}

	if len(missing) > 0 {
		h.set(true, false,
			fmt.Sprintf("missing expected tables %v — excluded (rebuilt node? see recovery runbook)", missing),
			c.logger)

		return
	}

	if len(mismatched) > 0 {
		h.set(true, false,
			fmt.Sprintf("tables %v do not match the configured mode (cluster_name=%q) — excluded "+
				"(see the migration runbook)", mismatched, c.opts.ClusterName),
			c.logger)

		return
	}

	lagging, err := c.lagging(ctx, h)
	if err != nil {
		h.set(true, false, err.Error(), c.logger)

		return
	}

	if lagging {
		h.set(true, false, "replica is lagging — excluded until it catches up", c.logger)

		return
	}

	h.set(true, true, "", c.logger)
}

// checkTables returns the expected tables the host does not hold, and those whose
// engine contradicts ClusterName (a non-replicated table in cluster mode, or the
// reverse — see ddl.EngineMatchesMode). Checking names (rather than just a count)
// keeps it correct even if an operator created some table manually, and lets us
// report exactly which table is at fault.
//
// The engine is checked per host, not once cluster-wide, because that is the state
// a migration goes through: the node holding the single-node data still has plain
// MergeTree tables while a freshly added node already has replicated ones.
func (c *Conn) checkTables(ctx context.Context, h *host) (missing, mismatched []string, err error) {
	if len(c.opts.ExpectedTables) == 0 {
		return nil, nil, nil
	}

	names := make([]string, len(c.opts.ExpectedTables))
	for i, table := range c.opts.ExpectedTables {
		names[i] = table.Name
	}

	engines, err := ddl.TableEngines(ctx, h.conn, names)
	if err != nil {
		return nil, nil, err
	}

	missing, mismatched = classifyTables(c.opts.ExpectedTables, engines, c.opts.ClusterName != "")

	return missing, mismatched, nil
}

// classifyTables splits the expected tables into the absent ones and those whose
// engine contradicts clustered (reported as "<name> (<engine>)").
func classifyTables(
	expected []ExpectedTable, engines map[string]string, clustered bool,
) (missing, mismatched []string) {
	for _, table := range expected {
		engine, exists := engines[table.Name]

		switch {
		case !exists:
			missing = append(missing, table.Name)
		case table.Replicated && !ddl.EngineMatchesMode(engine, clustered):
			mismatched = append(mismatched, table.Name+" ("+engine+")")
		}
	}

	return missing, mismatched
}

// lagging reports whether the host's most-delayed replica is behind by more than
// the configured threshold. On a single node (no replicated tables) the delay is
// zero.
func (c *Conn) lagging(ctx context.Context, h *host) (bool, error) {
	var delaySeconds uint64

	err := h.conn.QueryRow(ctx,
		"SELECT toUInt64(ifNull(max(absolute_delay), 0)) FROM system.replicas",
	).Scan(&delaySeconds)
	if err != nil {
		return false, err
	}

	return time.Duration(delaySeconds)*time.Second > c.opts.LagThreshold, nil
}

// --- driver.Conn implementation (route each call to a picked host) ---

// pickFunc selects the host a call routes to. The data-plane methods below take
// it so Conn (round-robin, c.pick) and its sticky view (deterministic single
// host, c.pickSticky) share one implementation and differ only in the strategy.
type pickFunc func() (*host, error)

func (c *Conn) Exec(ctx context.Context, query string, args ...any) error {
	return c.exec(ctx, c.pick, query, args...)
}

func (c *Conn) exec(ctx context.Context, pick pickFunc, query string, args ...any) error {
	if err := c.acquireWrite(ctx); err != nil {
		return err
	}
	defer c.releaseWrite()

	h, err := pick()
	if err != nil {
		return err
	}

	err = h.conn.Exec(ctx, query, args...)
	c.observe(h, err)

	return err
}

func (c *Conn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.query(ctx, c.pick, query, args...)
}

func (c *Conn) query(ctx context.Context, pick pickFunc, query string, args ...any) (driver.Rows, error) {
	if err := c.acquireRead(ctx); err != nil {
		return nil, err
	}

	h, err := pick()
	if err != nil {
		c.releaseRead()

		return nil, err
	}

	rows, err := h.conn.Query(ctx, query, args...)
	c.observe(h, err)

	if err != nil {
		c.releaseRead()

		return nil, err
	}

	// The query runs on ClickHouse until the rows are closed, so hold the slot
	// until then.
	return &limitedRows{Rows: rows, release: c.releaseRead}, nil
}

func (c *Conn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.queryRow(ctx, c.pick, query, args...)
}

func (c *Conn) queryRow(ctx context.Context, pick pickFunc, query string, args ...any) driver.Row {
	if err := c.acquireRead(ctx); err != nil {
		return errorRow{err: err}
	}
	defer c.releaseRead()

	h, err := pick()
	if err != nil {
		return errorRow{err: err}
	}

	row := h.conn.QueryRow(ctx, query, args...)
	c.observe(h, row.Err())

	return row
}

func (c *Conn) Select(ctx context.Context, dest any, query string, args ...any) error {
	return c.doSelect(ctx, c.pick, dest, query, args...)
}

func (c *Conn) doSelect(ctx context.Context, pick pickFunc, dest any, query string, args ...any) error {
	if err := c.acquireRead(ctx); err != nil {
		return err
	}
	defer c.releaseRead()

	h, err := pick()
	if err != nil {
		return err
	}

	err = h.conn.Select(ctx, dest, query, args...)
	c.observe(h, err)

	return err
}

func (c *Conn) PrepareBatch(
	ctx context.Context, query string, opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return c.prepareBatch(ctx, c.pick, query, opts...)
}

func (c *Conn) prepareBatch(
	ctx context.Context, pick pickFunc, query string, opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	if err := c.acquireWrite(ctx); err != nil {
		return nil, err
	}

	h, err := pick()
	if err != nil {
		c.releaseWrite()

		return nil, err
	}

	batch, err := h.conn.PrepareBatch(ctx, query, opts...) //nolint:clickhouselint
	c.observe(h, err)

	if err != nil {
		c.releaseWrite()

		return nil, err
	}

	// The slot is held until the batch is Sent/Aborted/Closed.
	return &limitedBatch{Batch: batch, release: c.releaseWrite}, nil
}

// limitedRows wraps driver.Rows to release a query-concurrency slot when the rows
// are closed (the ClickHouse query runs until then). release is idempotent.
type limitedRows struct {
	driver.Rows

	release func()
	once    sync.Once
}

func (r *limitedRows) Close() error {
	err := r.Rows.Close()
	r.once.Do(r.release)

	return err
}

// limitedBatch wraps driver.Batch to release a query-concurrency slot when the
// batch is finished (Send, Abort or Close — whichever comes first). release is
// idempotent, so the usual Send-then-Close releases exactly once.
type limitedBatch struct {
	driver.Batch

	release func()
	once    sync.Once
}

func (b *limitedBatch) Send() error {
	err := b.Batch.Send()
	b.once.Do(b.release)

	return err
}

func (b *limitedBatch) Abort() error {
	err := b.Batch.Abort()
	b.once.Do(b.release)

	return err
}

func (b *limitedBatch) Close() error {
	err := b.Batch.Close()
	b.once.Do(b.release)

	return err
}

// AsyncInsert is required by driver.Conn (deprecated upstream; SquirrelDB does
// not use it, but the interface must be satisfied).
func (c *Conn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return c.asyncInsert(ctx, c.pick, query, wait, args...)
}

//nolint:staticcheck
func (c *Conn) asyncInsert(ctx context.Context, pick pickFunc, query string, wait bool, args ...any) error {
	h, err := pick()
	if err != nil {
		return err
	}

	err = h.conn.AsyncInsert(ctx, query, wait, args...)
	c.observe(h, err)

	return err
}

func (c *Conn) InsertFormat(ctx context.Context, format, query string, data io.Reader) error {
	return c.insertFormat(ctx, c.pick, format, query, data)
}

func (c *Conn) insertFormat(ctx context.Context, pick pickFunc, format, query string, data io.Reader) error {
	h, err := pick()
	if err != nil {
		return err
	}

	err = h.conn.InsertFormat(ctx, format, query, data)
	c.observe(h, err)

	return err
}

func (c *Conn) QueryFormat(
	ctx context.Context, format, query string, args ...any,
) (io.ReadCloser, error) {
	return c.queryFormat(ctx, c.pick, format, query, args...)
}

func (c *Conn) queryFormat(
	ctx context.Context, pick pickFunc, format, query string, args ...any,
) (io.ReadCloser, error) {
	h, err := pick()
	if err != nil {
		return nil, err
	}

	rc, err := h.conn.QueryFormat(ctx, format, query, args...)
	c.observe(h, err)

	return rc, err
}

func (c *Conn) Ping(ctx context.Context) error {
	return c.ping(ctx, c.pick)
}

func (c *Conn) ping(ctx context.Context, pick pickFunc) error {
	h, err := pick()
	if err != nil {
		return err
	}

	return h.conn.Ping(ctx)
}

func (c *Conn) Contributors() []string {
	h, err := c.pick()
	if err != nil {
		return nil
	}

	return h.conn.Contributors()
}

func (c *Conn) ServerVersion() (*driver.ServerVersion, error) {
	h, err := c.pick()
	if err != nil {
		return nil, err
	}

	return h.conn.ServerVersion()
}

// Stats aggregates the per-host connection-pool stats.
func (c *Conn) Stats() driver.Stats {
	var stats driver.Stats

	for _, h := range c.hosts {
		s := h.conn.Stats()
		stats.MaxOpenConns += s.MaxOpenConns
		stats.MaxIdleConns += s.MaxIdleConns
		stats.Open += s.Open
		stats.Idle += s.Idle
	}

	return stats
}

// Close stops the health monitor and closes every per-host connection.
func (c *Conn) Close() error {
	c.cancelMonitor()
	c.wg.Wait()

	if c.registerer != nil && c.collector != nil {
		c.registerer.Unregister(c.collector)
	}

	if c.registerer != nil && c.queryWait != nil {
		c.registerer.Unregister(c.queryWait)
	}

	var firstErr error

	for _, h := range c.hosts {
		if h.conn == nil {
			continue
		}

		if err := h.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// errorRow is a driver.Row that always returns err, used when no host is
// available to serve a QueryRow.
type errorRow struct {
	err error
}

func (r errorRow) Err() error           { return r.err }
func (r errorRow) Scan(...any) error    { return r.err }
func (r errorRow) ScanStruct(any) error { return r.err }

// Sticky returns a driver.Conn that routes EVERY call to a single host (the first
// ready one, falling back to the first reachable) instead of round-robin. The
// telemetry store uses it: its optimistic lease needs read-your-writes, which
// holds only on a single replica (ReplicatedMergeTree replicates asynchronously,
// so a write on one replica and a read on another race). Since all nodes pick the
// same host (configured-address order), the lease table stays linearizable.
//
// It shares the underlying per-host connections and health monitor; Close is a
// no-op (the underlying Conn is owned by whoever created it).
func (c *Conn) Sticky() driver.Conn {
	return &viewConn{c: c, pick: c.pickSticky}
}

// OnHost returns a driver.Conn that routes every call to one configured address,
// whatever its health. The single-node → cluster migration needs it: the tables it
// renames and attaches partitions from live on a SINGLE host, which is by
// definition not "ready" (its engines do not match the configured mode yet), and a
// round-robin call would hit a node where those tables do not exist.
func (c *Conn) OnHost(addr string) (driver.Conn, error) {
	for _, h := range c.hosts {
		if h.addr == addr {
			return &viewConn{c: c, pick: func() (*host, error) { return h, nil }}, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", errUnknownHost, addr)
}

// viewConn is a driver.Conn over a Conn that sends every call through one fixed
// picking strategy instead of round-robin (see Sticky, OnHost).
type viewConn struct {
	c    *Conn
	pick pickFunc
}

var _ driver.Conn = (*viewConn)(nil)

// The data-plane methods share Conn's implementation, differing only by the pick.

func (s *viewConn) Exec(ctx context.Context, query string, args ...any) error {
	return s.c.exec(ctx, s.pick, query, args...)
}

func (s *viewConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return s.c.query(ctx, s.pick, query, args...)
}

func (s *viewConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return s.c.queryRow(ctx, s.pick, query, args...)
}

func (s *viewConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	return s.c.doSelect(ctx, s.pick, dest, query, args...)
}

func (s *viewConn) PrepareBatch(
	ctx context.Context, query string, opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return s.c.prepareBatch(ctx, s.pick, query, opts...)
}

func (s *viewConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return s.c.asyncInsert(ctx, s.pick, query, wait, args...)
}

func (s *viewConn) InsertFormat(ctx context.Context, format, query string, data io.Reader) error {
	return s.c.insertFormat(ctx, s.pick, format, query, data)
}

func (s *viewConn) QueryFormat(
	ctx context.Context, format, query string, args ...any,
) (io.ReadCloser, error) {
	return s.c.queryFormat(ctx, s.pick, format, query, args...)
}

func (s *viewConn) Ping(ctx context.Context) error {
	return s.c.ping(ctx, s.pick)
}

// Contributors/ServerVersion/Stats don't depend on the picking strategy.
func (s *viewConn) Contributors() []string                        { return s.c.Contributors() }
func (s *viewConn) ServerVersion() (*driver.ServerVersion, error) { return s.c.ServerVersion() }
func (s *viewConn) Stats() driver.Stats                           { return s.c.Stats() }

// Close is a no-op: the underlying Conn (and its monitor) is owned by the caller
// that built it, not by this view.
func (s *viewConn) Close() error { return nil }

// healthCollector exposes the number of reachable and ready ClickHouse hosts as
// Prometheus gauges, read live on each scrape from the monitor's current view.
type healthCollector struct {
	c         *Conn
	reachable *prometheus.Desc
	ready     *prometheus.Desc
}

func newHealthCollector(c *Conn) *healthCollector {
	return &healthCollector{
		c: c,
		reachable: prometheus.NewDesc(
			"squirreldb_clickhouse_reachable_nodes",
			"Number of ClickHouse hosts that answer a ping.",
			nil, nil),
		ready: prometheus.NewDesc(
			"squirreldb_clickhouse_ready_nodes",
			"Number of ClickHouse hosts that hold every expected table and whose replica is not lagging.",
			nil, nil),
	}
}

func (col *healthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- col.reachable

	ch <- col.ready
}

func (col *healthCollector) Collect(ch chan<- prometheus.Metric) {
	var reachable, ready int

	for _, h := range col.c.hosts {
		isReachable, isReady := h.state()
		if isReachable {
			reachable++
		}

		if isReady {
			ready++
		}
	}

	ch <- prometheus.MustNewConstMetric(col.reachable, prometheus.GaugeValue, float64(reachable))

	ch <- prometheus.MustNewConstMetric(col.ready, prometheus.GaugeValue, float64(ready))
}
