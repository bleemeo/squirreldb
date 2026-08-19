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

package tsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/redis/client"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
)

// newWALClient returns the walClient backing the WAL. When Redis is configured it
// returns a shared, durable, multi-node Redis client; otherwise it falls back to an
// in-process store (memWALClient) with reduced durability and no cross-node
// sharing, and logs a warning so the operator knows durability is degraded. The
// empty-Redis check matches the idiom used across the codebase (and the Cassandra
// in-memory batching fallback).
func newWALClient(redisOpts config.Redis, logger zerolog.Logger) walClient {
	if len(redisOpts.Addresses) > 0 && redisOpts.Addresses[0] != "" {
		return client.New(redisOpts)
	}

	logger.Warn().Msg(
		"No Redis configured: the ClickHouse write-ahead log is kept in memory (single node, reduced " +
			"durability — buffered points not yet flushed to ClickHouse are lost on restart or crash). " +
			"Configure Redis for a durable, highly-available WAL.")

	return newMemWALClient()
}

// The ClickHouse write path is a WAL (write-ahead log), NOT the per-metric batcher
// used by Cassandra. Each Commit appends its series (labels + points) to a single
// list; one node (the drainer, elected by a lease) periodically writes the head of
// the list to ClickHouse in a few large inserts and trims it. This deliberately
// drops read-your-writes (a just-written point is queryable in ClickHouse within
// ~the flush delay) and per-metric ownership/takeover — what ClickHouse wants is
// few large global inserts.
//
// The list and lease live in Redis by default: the WAL is then shared across nodes
// and durable (a crash loses nothing that was ACKed; only losing Redis loses data).
// When no Redis is configured the WAL falls back to an in-process store
// (memWALClient): single node, and buffered-but-not-yet-drained points are lost on
// restart/crash — same reduced-durability trade-off as the Cassandra backend
// without Redis.

// walEntry is ONE point (with its series' labels and write TTL) as stored in the
// WAL. The appender explodes each Commit into one entry per point, so the WAL list
// length is exactly the number of buffered points: backpressure and the drain
// trigger read it for free with LLEN/RPUSH — no separate byte counter to keep in
// sync. The trade-off is that labels are repeated per point in Redis (fine for
// Bleemeo, 1 point/series; heavier only for a backlog that groups points, which the
// drainer regroups back into one arrays-per-series insert). Points is kept as a
// slice so the binary layout is unchanged, but Append always stores a single point.
type walEntry struct {
	Labels labels.Labels
	TTL    int64 // per-metric write TTL, in seconds
	Points []types.MetricPoint
}

// encodeWALEntry serializes an entry with a self-describing, length-prefixed binary
// layout (no escaping needed): ttl | nLabels | (nameLen name valLen val)* |
// nPoints | (timestamp valueBits)*. All integers big-endian.
func encodeWALEntry(e walEntry) []byte {
	var buf bytes.Buffer

	var scratch [8]byte

	putU64 := func(v uint64) {
		binary.BigEndian.PutUint64(scratch[:], v)
		buf.Write(scratch[:])
	}

	putU32 := func(v uint32) {
		binary.BigEndian.PutUint32(scratch[:4], v)
		buf.Write(scratch[:4])
	}

	putStr := func(s string) {
		putU32(uint32(len(s))) //nolint:gosec
		buf.WriteString(s)
	}

	putU64(uint64(e.TTL)) //nolint:gosec

	nLabels := 0

	e.Labels.Range(func(labels.Label) { nLabels++ })
	putU32(uint32(nLabels))

	e.Labels.Range(func(l labels.Label) {
		putStr(l.Name)
		putStr(l.Value)
	})

	putU32(uint32(len(e.Points))) //nolint:gosec

	for _, p := range e.Points {
		putU64(uint64(p.Timestamp)) //nolint:gosec
		putU64(math.Float64bits(p.Value))
	}

	return buf.Bytes()
}

// pointEncodedSize is the on-wire size of one encoded point: two big-endian u64s
// (timestamp + value). labelMinEncodedSize is the smallest a label can encode to:
// two u32 length prefixes (empty name + value). Both bound slice pre-allocations
// against a corrupt count.
const (
	pointEncodedSize    = 16
	labelMinEncodedSize = 8
)

// capHint returns min(n, maxCap) as a non-negative int, comparing in uint64 so a
// corrupt n above 2^31 does not wrap negative on 32-bit builds (int(n) would, and
// a negative capacity panics make/NewScratchBuilder). maxCap is the small,
// non-negative byte-derived bound (remaining bytes / per-element size).
func capHint(n uint32, maxCap int) int {
	if uint64(n) < uint64(maxCap) { //nolint:gosec
		return int(n)
	}

	return maxCap
}

// decodeWALEntry is the inverse of encodeWALEntry.
func decodeWALEntry(b []byte) (walEntry, error) {
	r := decoder{b: b}

	ttl := r.u64()

	nLabels := r.u32()

	// Cap like points below: a corrupt nLabels can't drive a huge pre-allocation.
	// Each label is at least two u32 length prefixes (empty name + value), so
	// remaining/labelMinEncodedSize bounds it; a valid entry allocates exactly nLabels.
	builder := labels.NewScratchBuilder(capHint(nLabels, r.remaining()/labelMinEncodedSize))

	for range nLabels {
		name := r.str()
		value := r.str()

		if r.err != nil {
			break // truncated/corrupt: stop rather than add garbage labels nLabels times
		}

		builder.Add(name, value)
	}

	nPoints := r.u32()

	var points []types.MetricPoint

	if nPoints > 0 {
		// Cap the initial capacity at what the remaining bytes can actually hold (each
		// point is two u64s). A valid entry has exactly nPoints points left, so this
		// never re-allocs in the normal case; a corrupt nPoints (e.g. ~4e9 from a
		// bit-flipped length prefix) can't drive a huge up-front allocation.
		points = make([]types.MetricPoint, 0, capHint(nPoints, r.remaining()/pointEncodedSize))

		for range nPoints {
			ts := r.u64()
			value := math.Float64frombits(r.u64())

			if r.err != nil {
				break // truncated/corrupt: stop rather than append zero points nPoints times
			}

			points = append(points, types.MetricPoint{Timestamp: int64(ts), Value: value}) //nolint:gosec
		}
	}

	if r.err != nil {
		return walEntry{}, r.err
	}

	builder.Sort()

	return walEntry{Labels: builder.Labels(), TTL: int64(ttl), Points: points}, nil //nolint:gosec
}

// decoder reads the encodeWALEntry layout, latching the first error so callers can
// check it once at the end.
type decoder struct {
	b   []byte
	pos int
	err error
}

// remaining is how many undecoded bytes are left in the buffer.
func (d *decoder) remaining() int {
	return len(d.b) - d.pos
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}

	if d.pos+n > len(d.b) {
		d.err = fmt.Errorf("wal: truncated entry (want %d bytes at %d, have %d)", n, d.pos, len(d.b))

		return nil
	}

	out := d.b[d.pos : d.pos+n]
	d.pos += n

	return out
}

func (d *decoder) u64() uint64 {
	b := d.take(8)
	if b == nil {
		return 0
	}

	return binary.BigEndian.Uint64(b)
}

func (d *decoder) u32() uint32 {
	b := d.take(4)
	if b == nil {
		return 0
	}

	return binary.BigEndian.Uint32(b)
}

func (d *decoder) str() string {
	n := d.u32()

	b := d.take(int(n))
	if b == nil {
		return ""
	}

	return string(b)
}

// walClient is the subset of a Redis client the WAL and its drainer need: an
// ordered list (RPush/LLen/LRange/LTrim) plus a leader-election lease
// (LeaseAcquire/LeaseRelease). *redis/client.Client satisfies it for the shared,
// HA (multi-node) mode; memWALClient satisfies it for the in-memory, single-node
// mode used when no Redis is configured (see newWALClient). Close releases the
// connection the store owns.
type walClient interface {
	RPush(ctx context.Context, key string, values ...any) (int64, error)
	LLen(ctx context.Context, key string) (int64, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	LTrim(ctx context.Context, key string, start, stop int64) error
	LeaseAcquire(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	LeaseHeld(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	LeaseRelease(ctx context.Context, key, owner string) error
	Close()
}

var _ walClient = (*client.Client)(nil)

// walStore is the WAL: a single list appended by every node and drained by the
// elected leader (in Redis mode; a per-process list drained by the process itself
// in in-memory mode). Since every entry is one point, the list length (LLEN, or
// RPUSH's return) IS the buffered-point count — the source of truth for
// backpressure and the drain trigger, always exact and free (no side counter).
type walStore struct {
	client walClient
	key    string
}

// walPeekWindow is how many list elements (points) to fetch per LRANGE while
// assembling a drain batch. Peek accumulates across several LRANGE windows into ONE
// insert (bounded by maxPoints), so this does NOT cap the insert size — it only
// bounds how many entries a single LRANGE pulls into memory at once. Sized so a
// full maxPoints drain is a handful of round-trips, not hundreds.
const walPeekWindow = 20000

// Append pushes the entries (one point each) to the tail of the WAL and returns the
// WAL's length afterwards, i.e. the buffered-point count (used for backpressure).
func (w *walStore) Append(ctx context.Context, entries []walEntry) (int64, error) {
	if len(entries) == 0 {
		return w.client.LLen(ctx, w.key)
	}

	values := make([]any, len(entries))
	for i, e := range entries {
		values[i] = encodeWALEntry(e)
	}

	return w.client.RPush(ctx, w.key, values...)
}

// Len returns the WAL's current length: the number of buffered points (one entry
// per point). The drain trigger and backpressure work on this.
func (w *walStore) Len(ctx context.Context) (int64, error) {
	return w.client.LLen(ctx, w.key)
}

// Peek returns entries from the head of the WAL, stopping once it has maxPoints of
// them (one point each) or the WAL is exhausted. It does NOT remove them — call
// Trim(len) after the ClickHouse insert succeeds. Only the single leader calls
// Peek/Trim, so no concurrent drainer races on the head.
func (w *walStore) Peek(ctx context.Context, maxPoints int) ([]walEntry, error) {
	var (
		entries []walEntry
		start   int64
	)

	for len(entries) < maxPoints {
		raw, err := w.client.LRange(ctx, w.key, start, start+walPeekWindow-1)
		if err != nil {
			return nil, err
		}

		if len(raw) == 0 {
			break
		}

		for _, s := range raw {
			e, err := decodeWALEntry([]byte(s))
			if err != nil {
				return nil, fmt.Errorf("decode wal entry: %w", err)
			}

			entries = append(entries, e)

			if len(entries) >= maxPoints {
				return entries, nil
			}
		}

		if len(raw) < walPeekWindow {
			break // reached the end of the list
		}

		start += walPeekWindow
	}

	return entries, nil
}

// Trim removes the first n elements (the points a successful drain just wrote).
// LLEN stays the exact point count, so there is nothing else to reconcile.
func (w *walStore) Trim(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}

	return w.client.LTrim(ctx, w.key, int64(n), -1)
}

// backpressureDelay decides how a producer should slow down given the current WAL
// length and the critical/stop thresholds (both in points; 0 disables). It
// returns a sleep to apply BEFORE the next append (a linear ramp from 0 at
// critical to maxSleep at stop) and, once at/above stop, block=true meaning the
// caller must wait (polling) until the WAL drops back below stop. The sleep is
// applied before the append so a client timeout during it never leaves a
// half-written (duplicate-on-retry) entry.
func backpressureDelay(walLen int64, critical, stop int, maxSleep time.Duration) (time.Duration, bool) {
	if stop > 0 && walLen >= int64(stop) {
		return 0, true
	}

	if critical <= 0 || walLen < int64(critical) {
		return 0, false
	}

	span := stop - critical
	if span <= 0 {
		return maxSleep, false
	}

	over := walLen - int64(critical)

	delay := min(time.Duration(int64(maxSleep)*over/int64(span)), maxSleep)

	return delay, false
}
