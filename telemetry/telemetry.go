// Copyright 2015-2025 Bleemeo
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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/bleemeo/squirreldb/facts"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/types"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	idLockTTL        = time.Minute
	idLockRetryDelay = 5 * time.Second
)

var errFailedLock = errors.New("failed to acquire lock")

type Telemetry struct {
	opts Options

	// Common ID between all SquirrelDB in the cluster.
	clusterID string
	// ID of this SquirrelDB instance.
	id string
	// Lock used to avoid other SquirrelDB taking the same ID.
	idLock types.TryLocker
}

type lockFactory interface {
	CreateLock(name string, timeToLive time.Duration) types.TryLocker
}

type Options struct {
	// The telemetry URL.
	URL string
	// The way SquirrelDB was installed (Manual, Package, Docker, etc).
	InstallationFormat string
	// SquirrelDB version.
	Version       string
	ClusterSizeFn func() int
	LockFactory   lockFactory
	State         types.State
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
	if t.idLock != nil {
		t.idLock.Unlock()
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

	req, _ := http.NewRequest(http.MethodPost, t.opts.URL, bytes.NewBuffer(body))

	req.Header.Set("Content-Type", "application/json")

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := http.DefaultClient.Do(req.WithContext(ctx2))
	if err != nil {
		t.opts.Logger.Err(err).Msg("Failed to post telemetry")

		return
	}

	t.opts.Logger.Info().Msgf("Telemetry response status: %s", resp.Status)

	// Ensure we read the whole response to avoid "Connection reset by peer" on server
	// and ensure the HTTP connection can be reused
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// The telemetry ID is stored both as a lock and in the state.
// The state contains the list of all known IDs.
// To get an ID, SquirrelDB checks if the IDs in the list are already taken.
// An ID is taken if the lock associated to this ID is taken.
// If it finds a free ID, it takes it, else it adds a new ID to the state and lock the new ID.
// Note that this means the IDs can be swapped between SquirrelDB nodes after a restart.
func (t *Telemetry) getTelemetryID(ctx context.Context) (string, error) {
	if t.id != "" {
		return t.id, nil
	}

	// Use a lock to read and write to the list of telemetry IDs from the state.
	lock := t.opts.LockFactory.CreateLock("create telemetry id", 5*time.Second)
	if ok := lock.TryLock(ctx, 10*time.Second); !ok {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		return "", fmt.Errorf("%w: create telemetry id", errFailedLock)
	}

	defer lock.Unlock()

	var telemetryIDs []string

	_, err := t.opts.State.Read(ctx, "telemetry-ids", &telemetryIDs)
	if err != nil {
		return "", fmt.Errorf("failed to read telemetry ID from state: %w", err)
	}

	// Find a free ID in the list.
	for _, id := range telemetryIDs {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		lock := t.opts.LockFactory.CreateLock("telemetry-id-"+id, idLockTTL)
		if ok := lock.TryLock(ctx, 0); !ok {
			// The lock is taken, another SquirrelDB already uses this ID.
			continue
		}

		// Lock successfully taken, we found a free ID.
		t.opts.Logger.Debug().Msgf("Found free telemetry ID %s", id)

		t.id = id
		t.idLock = lock

		break
	}

	if t.id != "" {
		return t.id, nil
	}

	// No free ID found, create a new one, add it to the state and lock it.
	id := uuid.New().String()
	telemetryIDs = append(telemetryIDs, id)

	err = t.opts.State.Write(ctx, "telemetry-ids", telemetryIDs)
	if err != nil {
		return "", fmt.Errorf("failed to write telemetry ID in state: %w", err)
	}

	lock = t.opts.LockFactory.CreateLock("telemetry-id-"+id, 5*time.Second)
	if ok := lock.TryLock(ctx, idLockRetryDelay); !ok {
		return "", fmt.Errorf("%w: telemetry-id-%s", errFailedLock, id)
	}

	t.opts.Logger.Debug().Msgf("Created telemetry ID %s", id)

	t.idLock = lock
	t.id = id

	return id, nil
}

func (t *Telemetry) getClusterID(ctx context.Context) (string, error) {
	if t.clusterID != "" {
		return t.clusterID, nil
	}

	var clusterID string

	lock := t.opts.LockFactory.CreateLock("create cluster id", 5*time.Second)
	if ok := lock.TryLock(ctx, 10*time.Second); !ok {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		return "", fmt.Errorf("%w: create cluster id", errFailedLock)
	}

	defer lock.Unlock()

	hasClusterID, err := t.opts.State.Read(ctx, "cluster_id", &clusterID)
	if err != nil {
		return "", fmt.Errorf("unable to read cluster id for telemetry: %w", err)
	}

	if !hasClusterID {
		// The cluster ID doesn't exist, create it.
		clusterID = uuid.New().String()

		err = t.opts.State.Write(ctx, "cluster_id", clusterID)
		if err != nil {
			return "", fmt.Errorf("unable to set cluster id for telemetry: %w", err)
		}

		t.opts.Logger.Debug().Msgf("Created telemetry cluster ID %s", clusterID)
	} else {
		t.opts.Logger.Debug().Msgf("Read telemetry cluster ID %s", clusterID)
	}

	t.clusterID = clusterID

	return clusterID, nil
}
