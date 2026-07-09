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
	"context"
	"math"
	"time"
)

// pacer throttles the migration to a target points-per-second using a token
// bucket. Tokens (one per point) refill continuously at rate and are capped at
// burst, so — unlike a cumulative running average — a slow period cannot
// accumulate unbounded credit that would later be spent as an unbounded burst.
// A chunk larger than the burst is handled naturally: the balance goes negative
// and the next wait sleeps proportionally.
type pacer struct {
	rate   float64 // points/s; <= 0 means unlimited (no throttling)
	burst  float64 // max accumulated tokens
	now    func() time.Time
	last   time.Time
	tokens float64
}

func newPacer(rate float64) *pacer {
	// pacerBurstSeconds sizes the token-bucket burst as rate × this many seconds. It
	// bounds how many points may be sent back-to-back after an idle/slow period —
	// small on purpose so a slow first phase cannot bank a huge burst for later.
	const pacerBurstSeconds = 10.0

	return newPacerWithClock(rate, pacerBurstSeconds, time.Now)
}

func newPacerWithClock(rate float64, burstSeconds float64, now func() time.Time) *pacer {
	burst := rate * burstSeconds

	return &pacer{rate: rate, burst: burst, now: now, last: now(), tokens: burst}
}

// wait accounts n freshly-written points and sleeps for as long as the token
// bucket says is needed to hold the target rate. It returns immediately if
// unlimited or if ctx is done during the sleep.
func (p *pacer) wait(ctx context.Context, n int) {
	delay := p.reserve(n)
	if delay <= 0 {
		return
	}

	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

// reserve refills the bucket up to burst, spends n tokens, and returns how long
// to sleep to hold the rate (0 when enough tokens were available). When it
// returns a delay it leaves the balance negative on purpose: the refill over the
// (unchanged) last timestamp on the next call brings it back toward zero, so the
// slept time is neither double-counted nor turned into credit.
func (p *pacer) reserve(n int) time.Duration {
	if p.rate <= 0 {
		return 0
	}

	now := p.now()
	p.tokens = math.Min(p.burst, p.tokens+now.Sub(p.last).Seconds()*p.rate)
	p.last = now

	p.tokens -= float64(n)
	if p.tokens >= 0 {
		return 0
	}

	return time.Duration(-p.tokens / p.rate * float64(time.Second))
}
