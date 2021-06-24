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
	"crypto/rand"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"squirreldb/facts"
	"time"

	"github.com/google/uuid"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

type Telemetry struct {
	ID        string
	newFacts  map[string]string
	runOption map[string]string
}

type telemetryJSONID struct {
	ID string `json:"id"`
}

func (t Telemetry) getIDFromFile(filepath string) Telemetry {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		t.setIDToFile(filepath)
	}

	file, _ := ioutil.ReadFile(filepath)

	var tlm telemetryJSONID

	_ = json.Unmarshal(file, &tlm)
	t.ID = tlm.ID

	if t.ID == "" {
		return t.setIDToFile(filepath)
	}

	return t
}

func (t Telemetry) setIDToFile(filepath string) Telemetry {
	var tlm telemetryJSONID

	t.ID = uuid.New().String()
	tlm.ID = t.ID

	file, _ := json.MarshalIndent(tlm, "", " ")

	_ = ioutil.WriteFile(filepath, file, 0600)

	return t
}

func (t Telemetry) postInformation(ctx context.Context, newFacts map[string]string, url string) {
	facts := facts.Facts(ctx)
	body, _ := json.Marshal(map[string]string{
		"id":                  t.ID,
		"cluster_id":          newFacts["cluster_id"],
		"cpu_cores":           facts["cpu_cores"],
		"cpu_model":           facts["cpu_model_name"],
		"country":             facts["timezone"],
		"installation_format": newFacts["installation_format"],
		"kernel_version":      facts["kernel_version"],
		"memory":              facts["memory"],
		"product":             "SquirrelDB",
		"os_type":             facts["os_name"],
		"os_version":          facts["os_version"],
		"system_architecture": facts["architecture"],
		"version":             newFacts["version"],
	})

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))

	req.Header.Set("Content-Type", "application/json")

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := http.DefaultClient.Do(req.WithContext(ctx2))
	if err != nil {
		logger.Printf("failed when we post on telemetry: %v", err)
		return
	}

	logger.Printf("Telemetry response Satus: %s", resp.Status)

	defer func() {
		// Ensure we read the whole response to avoid "Connection reset by peer" on server
		// and ensure HTTP connection can be resused
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
}

func (t Telemetry) run(ctx context.Context) {
	n, err := rand.Int(rand.Reader, big.NewInt(5))
	if err != nil {
		logger.Printf("Waring: can't create a random int%v", err)
	}

	select {
	case <-time.After(2*time.Minute + time.Duration(n.Int64())*time.Minute):
	case <-ctx.Done():
		return
	}

	var tlm Telemetry

	tlm = tlm.getIDFromFile(t.runOption["filepath"])

	for {
		tlm.postInformation(ctx, t.newFacts, t.runOption["url"])

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
	go t.run(ctx)
}

func (t Telemetry) Stop() {
}
