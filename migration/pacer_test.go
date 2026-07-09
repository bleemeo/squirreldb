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

package migration

import (
	"testing"
	"time"
)

func TestPacerUnlimited(t *testing.T) {
	t.Parallel()

	clock := time.Unix(0, 0)
	p := newPacerWithClock(0, 0, func() time.Time { return clock })

	if d := p.reserve(1_000_000); d != 0 {
		t.Fatalf("unlimited pacer delay = %v, want 0", d)
	}
}

func TestPacerThrottles(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	clock := start
	// 100 points/s, burst = 100 (1s). Starts full.
	p := newPacerWithClock(100, 1, func() time.Time { return clock })

	// Spend the full burst instantly: allowed, no delay.
	if d := p.reserve(100); d != 0 {
		t.Fatalf("spending the burst delay = %v, want 0", d)
	}

	// Another 100 with no time elapsed: bucket empty → must wait 1s.
	if d := p.reserve(100); d != time.Second {
		t.Fatalf("delay with empty bucket = %v, want 1s", d)
	}

	// After 1s the deficit is repaid: back to zero, next small spend is free.
	clock = start.Add(time.Second)

	if d := p.reserve(0); d != 0 {
		t.Fatalf("delay after refill = %v, want 0", d)
	}
}

func TestPacerThrottlesLow(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	clock := start

	// 1 writes every 20s on average.
	p := newPacerWithClock(0.05, 10, func() time.Time { return clock })

	// Bucket size is 10s (so 50% of a write), so every first write need to
	// wait 10 seconds
	if d := p.reserve(1); d != 10*time.Second {
		t.Fatalf("spending the burst delay = %v, want 10s", d)
	}

	clock = clock.Add(10 * time.Second)

	// Then need to wait a full 20 seconds
	if d := p.reserve(1); d != 20*time.Second {
		t.Fatalf("delay with empty bucket = %v, want 20s", d)
	}

	// After 20s we should send a new write but need to wait one more 20s
	clock = clock.Add(20 * time.Second)

	if d := p.reserve(1); d != 20*time.Second {
		t.Fatalf("delay with empty bucket = %v, want 20s", d)
	}

	// If no write for 40s, then burst is re-fill (but burst is only 0.5, so still have
	// to wait for 10s)
	clock = clock.Add(40 * time.Second)

	if d := p.reserve(1); d != 10*time.Second {
		t.Fatalf("delay after refill = %v, want 10s", d)
	}
}

func TestPacerChunkLargerThanBurst(t *testing.T) {
	t.Parallel()

	const (
		rate         = 100
		burstSeconds = 1
	)

	clock := time.Unix(0, 0)
	p := newPacerWithClock(rate, burstSeconds, func() time.Time { return clock })

	// A single chunk of 500 points (5× the burst) → throttled proportionally:
	// 100 free (the burst) + 400 to wait for = 4s.
	if d := p.reserve(500); d != 4*time.Second {
		t.Fatalf("delay for oversized chunk = %v, want 4s", d)
	}
}

// TestPacerBurstIsBounded is the point of the token bucket: a long idle/slow
// period must NOT accumulate unbounded credit (which a cumulative running
// average would, then blow the rate on a huge burst).
func TestPacerBurstIsBounded(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	clock := start
	p := newPacerWithClock(100, 1, func() time.Time { return clock }) // rate 100, burst 100

	// One hour of idle. A running average would bank ~360 000 tokens of credit.
	clock = start.Add(time.Hour)

	// The bucket is capped at burst (100): only 100 points may go through free,
	// then the rate applies. 1100 points → 100 free + 1000 waited = 10s.
	if d := p.reserve(1100); d != 10*time.Second {
		t.Fatalf("delay after long idle = %v, want 10s (burst capped, not unbounded credit)", d)
	}
}
