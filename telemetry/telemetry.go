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

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/bleemeo/squirreldb/facts"
	"github.com/bleemeo/squirreldb/logger"

	"github.com/rs/zerolog"
)

// Store persists the small amount of state the telemetry needs: the shared
// cluster ID and a pool of per-node IDs with a crash-tolerant ownership lease.
//
// The node-ID contract is intentionally soft (a violation only makes Bleemeo's
// telemetry slightly inaccurate, never breaks SquirrelDB):
//   - a fixed-size cluster reuses the same pool of IDs across restarts/crashes;
//   - two live nodes should not hold the same ID at the same time;
//   - a crashed node's ID becomes free again on its own.
type Store interface {
	// ClusterID returns the shared cluster ID, creating it on first use.
	ClusterID(ctx context.Context) (string, error)
	// AcquireNodeID claims a node ID and returns it together with a release
	// function that frees it. Keeping the claim alive (lease refresh, lock
	// keep-alive, ...) is the Store's own concern; the caller only has to call
	// release once it no longer needs the ID. ctx bounds the initial claim only.
	AcquireNodeID(ctx context.Context) (id string, release func(), err error)
}

type Telemetry struct {
	opts Options

	// Common ID between all SquirrelDB in the cluster.
	clusterID string
	// ID of this SquirrelDB instance, and the function that releases it.
	id        string
	releaseID func()
}

type Options struct {
	// The telemetry URL.
	URL string
	// The way SquirrelDB was installed (Manual, Package, Docker, etc).
	InstallationFormat string
	// SquirrelDB version.
	Version       string
	ClusterSizeFn func() int
	Store         Store
	Logger        zerolog.Logger
}

func New(opts Options) *Telemetry {
	tlm := Telemetry{
		opts: opts,
	}

	return &tlm
}

func (t *Telemetry) Start(ctx context.Context) {
	go func() {
		defer logger.ProcessPanic()

		t.run(ctx)
	}()
}

func (t *Telemetry) run(ctx context.Context) {
	// The initial delay means a node only claims an ID once it has been alive
	// for a few minutes: short-lived pods (and the extra pod during a rolling
	// deploy) never grab a slot, so the pool stays at the steady-state size.
	select {
	case <-time.After(2*time.Minute + time.Duration(rand.Intn(5))*time.Minute): //nolint:gosec
	case <-ctx.Done():
		return
	}

	for {
		t.postInformation(ctx)

		select {
		case <-time.After(24 * time.Hour):
		case <-ctx.Done():
			t.stop()

			return
		}
	}
}

func (t *Telemetry) stop() {
	// Free our telemetry ID before stopping.
	if t.releaseID != nil {
		t.releaseID()
		t.releaseID = nil
	}
}

func (t *Telemetry) postInformation(ctx context.Context) {
	id, err := t.getTelemetryID(ctx)
	if err != nil {
		t.opts.Logger.Err(err).Msg("Failed to get telemetry id")

		return
	}

	clusterID, err := t.getClusterID(ctx)
	if err != nil {
		t.opts.Logger.Err(err).Msg("Failed to get cluster id")

		return
	}

	facts := facts.Facts(ctx)
	body, _ := json.Marshal(map[string]string{ //nolint:errchkjson // False positive.
		"id":                  id,
		"cluster_id":          clusterID,
		"cluster_size":        strconv.Itoa(t.opts.ClusterSizeFn()),
		"cpu_cores":           facts["cpu_cores"],
		"cpu_model":           facts["cpu_model_name"],
		"country":             facts["timezone"],
		"installation_format": t.opts.InstallationFormat,
		"kernel_version":      facts["kernel_version"],
		"memory":              facts["memory"],
		"product":             "SquirrelDB",
		"os_type":             facts["os_name"],
		"os_version":          facts["os_version"],
		"system_architecture": facts["architecture"],
		"version":             t.opts.Version,
	})

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx2, http.MethodPost, t.opts.URL, bytes.NewBuffer(body))

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.opts.Logger.Err(err).Msg("Failed to post telemetry")

		return
	}

	t.opts.Logger.Info().Msgf("Telemetry response status: %s", resp.Status)

	// Ensure we read the whole response to avoid "Connection reset by peer" on server
	// and ensure the HTTP connection can be reused
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// getTelemetryID returns this node's telemetry ID, acquiring one from the Store
// (and caching it, with its release function) on first use.
func (t *Telemetry) getTelemetryID(ctx context.Context) (string, error) {
	if t.id != "" {
		return t.id, nil
	}

	id, release, err := t.opts.Store.AcquireNodeID(ctx)
	if err != nil {
		return "", err
	}

	t.id = id
	t.releaseID = release

	return id, nil
}

func (t *Telemetry) getClusterID(ctx context.Context) (string, error) {
	if t.clusterID != "" {
		return t.clusterID, nil
	}

	clusterID, err := t.opts.Store.ClusterID(ctx)
	if err != nil {
		return "", err
	}

	t.clusterID = clusterID

	return clusterID, nil
}
