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
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

type Telemetry struct {
	ID string `json:"id"`
}

func (t Telemetry) GetIdFromFile() {
	if _, err := os.Stat("telemetry.json"); os.IsNotExist(err) {
		t.setIdToFile()
	}

	file, _ := ioutil.ReadFile("telemetry.json")

	_ = json.Unmarshal([]byte(file), &t)

	if t.ID == "" {
		t.setIdToFile()
	}
}

func (t Telemetry) setIdToFile() {
	t.ID = uuid.New().String()

	file, _ := json.MarshalIndent(t, "", " ")

	_ = ioutil.WriteFile("telemetry.json", file, 0644)
}

func (t Telemetry) PostInformation(ctx context.Context, url string, facts map[string]string) {
	body, _ := json.Marshal(map[string]string{
		"id":                  t.ID,
		"cpu_cores":           facts["cpu_cores"],
		"cpu_model":           facts["cpu_model_name"],
		"country":             facts["timezone"],
		"installation_format": facts["installation_format"], //TBD
		"kernel_version":      facts["kernel_major_version"],
		"memory":              facts["memory"],
		"product":             "Squirreldb",
		"os_type":             facts["os_name"],
		"os_version":          facts["os_version"],
		"system_architecture": facts["architecture"],
		"version":             facts["squirreldb_version"],
	})

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))

	req.Header.Set("Content-Type", "application/json")

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := http.DefaultClient.Do(req.WithContext(ctx2))

	if err != nil {
		logger.Printf("failed when we post on telemetry: %v", err)
	}

	if resp != nil {
		logger.Printf("telemetry response Satus: %s", resp.Status)
		defer resp.Body.Close()
	}
}
