// Copyright 2015-2019 Bleemeo
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
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"squirreldb/facts"
	"squirreldb/logger"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type Telemetry struct {
	ID        string
	newFacts  map[string]string
	runOption map[string]string
}

type telemetryJSONID struct {
	ID string `json:"id"`
}

func (t *Telemetry) getIDFromFile() {
	filepath := t.runOption["filepath"]
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		t.setIDToFile(filepath)
	}

	file, _ := ioutil.ReadFile(filepath)

	var tlm telemetryJSONID

	_ = json.Unmarshal(file, &tlm)
	t.ID = tlm.ID

	if t.ID == "" {
		t.setIDToFile(filepath)
	}
}

func (t *Telemetry) setIDToFile(filepath string) {
	var tlm telemetryJSONID

	t.ID = uuid.New().String()
	tlm.ID = t.ID

	file, _ := json.MarshalIndent(tlm, "", " ") //nolint:errchkjson // False positive.

	_ = ioutil.WriteFile(filepath, file, 0o600)
}

func (t Telemetry) postInformation(ctx context.Context) {
	facts := facts.Facts(ctx)
	body, _ := json.Marshal(map[string]string{ //nolint:errchkjson // False positive.
		"id":                  t.ID,
		"cluster_id":          t.newFacts["cluster_id"],
		"cpu_cores":           facts["cpu_cores"],
		"cpu_model":           facts["cpu_model_name"],
		"country":             facts["timezone"],
		"installation_format": t.newFacts["installation_format"],
		"kernel_version":      facts["kernel_version"],
		"memory":              facts["memory"],
		"product":             "SquirrelDB",
		"os_type":             facts["os_name"],
		"os_version":          facts["os_version"],
		"system_architecture": facts["architecture"],
		"version":             t.newFacts["version"],
	})

	req, _ := http.NewRequest("POST", t.runOption["url"], bytes.NewBuffer(body))

	req.Header.Set("Content-Type", "application/json")

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := http.DefaultClient.Do(req.WithContext(ctx2))
	if err != nil {
		log.Err(err).Msg("Failed to post telemetry")

		return
	}

	log.Info().Msgf("Telemetry response status: %s", resp.Status)

	defer func() {
		// Ensure we read the whole response to avoid "Connection reset by peer" on server
		// and ensure HTTP connection can be resused
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
}

func (t Telemetry) run(ctx context.Context) {
	select {
	case <-time.After(2*time.Minute + time.Duration(rand.Intn(5))*time.Minute): //nolint:gosec
	case <-ctx.Done():
		return
	}

	t.getIDFromFile()

	for {
		t.postInformation(ctx)

		select {
		case <-time.After(24 * time.Hour):
		case <-ctx.Done():
			return
		}
	}
}

func New(newFacts map[string]string, runOption map[string]string) Telemetry {
	var tlm Telemetry

	tlm.newFacts = newFacts
	tlm.runOption = runOption

	return tlm
}

func (t Telemetry) Start(ctx context.Context) {
	go func() {
		defer logger.ProcessPanic()

		t.run(ctx)
	}()
}

func (t Telemetry) Stop() {
}
