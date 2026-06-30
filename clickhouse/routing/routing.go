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
//   - ready: reachable AND holds all expected tables AND replica lag < threshold;
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// codeUnknownTable is the ClickHouse error code (UNKNOWN_TABLE) returned when a
// query targets a table that does not exist on the node it reached — the signal
// that the node is degraded (e.g. rebuilt empty), not that the query is wrong.
const codeUnknownTable = 60

// Defaults for the health monitor.
const (
	DefaultProbeInterval = 5 * time.Second
	DefaultLagThreshold  = 30 * time.Second
	probeTimeout         = 3 * time.Second
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
	ExpectedTables []string
	// ClusterName, when set, is used only to warn (at startup) if the number of
	// configured addresses does not match the cluster's node count. It is never
	// used to discover addresses (that would break NAT/proxy setups).
	ClusterName   string
	ProbeInterval time.Duration // default DefaultProbeInterval
	LagThreshold  time.Duration // default DefaultLagThreshold
	// Registerer, when set, receives per-host health gauges.
	Registerer prometheus.Registerer
	Logger     zerolog.Logger
}

type host struct {
	addr string
	conn driver.Conn

	mu        sync.RWMutex
	reachable bool
	ready     bool
}

func (h *host) state() (reachable, ready bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.reachable, h.ready
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
	c.registerMetrics()

	c.wg.Add(1)

	go c.monitor(monitorCtx) //nolint:contextcheck

	return c, nil
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
}

// Hosts returns the current health of every configured host (for introspection,
// e.g. a debug endpoint).
func (c *Conn) Hosts() []HostStatus {
	statuses := make([]HostStatus, 0, len(c.hosts))

	for _, h := range c.hosts {
		reachable, ready := h.state()
		statuses = append(statuses, HostStatus{Address: h.addr, Reachable: reachable, Ready: ready})
	}

	return statuses
}

// AnyReady reports whether at least one host holds the expected tables and is
// not lagging.
func (c *Conn) AnyReady() bool {
	for _, h := range c.hosts {
		if _, ready := h.state(); ready {
			return true
		}
	}

	return false
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
		return nil, errors.New("routing: no reachable ClickHouse host")
	}

	idx := int(c.rr.Add(1)-1) % len(pool) //nolint:gosec

	return pool[idx], nil
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

	return nil, errors.New("routing: no reachable ClickHouse host")
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
		h.set(false, false, "", c.logger)

		return
	}

	missing, err := c.missingTables(ctx, h)
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

// missingTables returns the expected tables the host does not hold. Checking
// names (rather than just a count) keeps it correct even if an operator created
// some table manually, and lets us log exactly which table is missing.
func (c *Conn) missingTables(ctx context.Context, h *host) ([]string, error) {
	if len(c.opts.ExpectedTables) == 0 {
		return nil, nil
	}

	rows, err := h.conn.Query(ctx,
		"SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN (?)",
		c.opts.ExpectedTables,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	present := make(map[string]bool, len(c.opts.ExpectedTables))

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		present[name] = true
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	var missing []string

	for _, table := range c.opts.ExpectedTables {
		if !present[table] {
			missing = append(missing, table)
		}
	}

	return missing, nil
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

func (c *Conn) Exec(ctx context.Context, query string, args ...any) error {
	h, err := c.pick()
	if err != nil {
		return err
	}

	err = h.conn.Exec(ctx, query, args...)
	c.observe(h, err)

	return err
}

func (c *Conn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	h, err := c.pick()
	if err != nil {
		return nil, err
	}

	rows, err := h.conn.Query(ctx, query, args...)
	c.observe(h, err)

	return rows, err
}

func (c *Conn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	h, err := c.pick()
	if err != nil {
		return errorRow{err: err}
	}

	row := h.conn.QueryRow(ctx, query, args...)
	c.observe(h, row.Err())

	return row
}

func (c *Conn) Select(ctx context.Context, dest any, query string, args ...any) error {
	h, err := c.pick()
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
	h, err := c.pick()
	if err != nil {
		return nil, err
	}

	batch, err := h.conn.PrepareBatch(ctx, query, opts...)
	c.observe(h, err)

	return batch, err
}

// AsyncInsert is required by driver.Conn (deprecated upstream; SquirrelDB does
// not use it, but the interface must be satisfied).
//
//nolint:staticcheck
func (c *Conn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	h, err := c.pick()
	if err != nil {
		return err
	}

	err = h.conn.AsyncInsert(ctx, query, wait, args...)
	c.observe(h, err)

	return err
}

func (c *Conn) Ping(ctx context.Context) error {
	h, err := c.pick()
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
	return &stickyConn{c: c}
}

type stickyConn struct {
	c *Conn
}

var _ driver.Conn = (*stickyConn)(nil)

func (s *stickyConn) Exec(ctx context.Context, query string, args ...any) error {
	h, err := s.c.pickSticky()
	if err != nil {
		return err
	}

	err = h.conn.Exec(ctx, query, args...)
	s.c.observe(h, err)

	return err
}

func (s *stickyConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	h, err := s.c.pickSticky()
	if err != nil {
		return nil, err
	}

	rows, err := h.conn.Query(ctx, query, args...)
	s.c.observe(h, err)

	return rows, err
}

func (s *stickyConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	h, err := s.c.pickSticky()
	if err != nil {
		return errorRow{err: err}
	}

	row := h.conn.QueryRow(ctx, query, args...)
	s.c.observe(h, row.Err())

	return row
}

func (s *stickyConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	h, err := s.c.pickSticky()
	if err != nil {
		return err
	}

	err = h.conn.Select(ctx, dest, query, args...)
	s.c.observe(h, err)

	return err
}

func (s *stickyConn) PrepareBatch(
	ctx context.Context, query string, opts ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	h, err := s.c.pickSticky()
	if err != nil {
		return nil, err
	}

	batch, err := h.conn.PrepareBatch(ctx, query, opts...)
	s.c.observe(h, err)

	return batch, err
}

//nolint:staticcheck
func (s *stickyConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	h, err := s.c.pickSticky()
	if err != nil {
		return err
	}

	err = h.conn.AsyncInsert(ctx, query, wait, args...)
	s.c.observe(h, err)

	return err
}

func (s *stickyConn) Ping(ctx context.Context) error {
	h, err := s.c.pickSticky()
	if err != nil {
		return err
	}

	return h.conn.Ping(ctx)
}

// Contributors/ServerVersion/Stats don't depend on the picking strategy.
func (s *stickyConn) Contributors() []string                        { return s.c.Contributors() }
func (s *stickyConn) ServerVersion() (*driver.ServerVersion, error) { return s.c.ServerVersion() }
func (s *stickyConn) Stats() driver.Stats                           { return s.c.Stats() }

// Close is a no-op: the underlying Conn (and its monitor) is owned by the caller
// that built it, not by this view.
func (s *stickyConn) Close() error { return nil }

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
