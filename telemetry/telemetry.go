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
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"squirreldb/facts"
	"time"

	"github.com/google/uuid"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

type telemetry struct {
	ID string `json:"id"`
}

func (t telemetry) getIDFromFile(filepath string) {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		t.setIDToFile(filepath)
	}

	file, _ := ioutil.ReadFile(filepath)

	_ = json.Unmarshal(file, &t)

	if t.ID == "" {
		t.setIDToFile(filepath)
	}
}

func (t telemetry) setIDToFile(filepath string) {
	t.ID = uuid.New().String()

	file, _ := json.MarshalIndent(t, "", " ")

	_ = ioutil.WriteFile(filepath, file, 0600)
}

func (t telemetry) postInformation(ctx context.Context, newFacts map[string]string, url string) {
	facts := facts.Facts(ctx)
	body, _ := json.Marshal(map[string]string{
		"id":                  t.ID,
		"cluster_id":          newFacts["cluster_id"],
		"cpu_cores":           facts["cpu_cores"],
		"cpu_model":           facts["cpu_model_name"],
		"country":             facts["timezone"],
		"installation_format": newFacts["installation_format"],
		"kernel_version":      facts["kernel_major_version"],
		"memory":              facts["memory"],
		"product":             "Squirreldb",
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

	logger.Printf("telemetry response Satus: %s", resp.Status)
	defer resp.Body.Close()
}

func Run(ctx context.Context, newFacts map[string]string, runOption map[string]string) error {
	select {
	case <-time.After(2*time.Minute + time.Duration(rand.Intn(5))*time.Minute):
	case <-ctx.Done():
		return nil
	}

	var tlm telemetry

	tlm.getIDFromFile(runOption["filepath"])

	for {
		tlm.postInformation(ctx, newFacts, runOption["url"])

		select {
		case <-time.After(24 * time.Hour):
		case <-ctx.Done():
			return nil
		}
	}
}
