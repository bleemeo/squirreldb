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

package facts

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

func Facts(_ context.Context) map[string]string {
	newFacts := make(map[string]string)

	for k, v := range platformFacts() {
		newFacts[k] = v
	}

	newFacts["architecture"] = runtime.GOARCH

	if v, err := os.ReadFile(filepath.Join("/", "etc/timezone")); err == nil {
		newFacts["timezone"] = strings.TrimSpace(string(v))
	}

	newFacts["fact_updated_at"] = time.Now().UTC().Format(time.RFC3339)

	cpu, err := cpu.Info()

	if err == nil && len(cpu) > 0 {
		newFacts["cpu_model_name"] = cpu[0].ModelName
		newFacts["cpu_cores"] = strconv.Itoa(len(cpu))
	}

	mem, err := mem.VirtualMemory()

	if err == nil && mem != nil {
		newFacts["memory"] = byteCountDecimal(mem.Total)
	}

	cleanFacts(newFacts)

	return newFacts
}

// cleanFacts will remove key with empty values and truncate value
// with 100 characters or more.
func cleanFacts(facts map[string]string) {
	for k, v := range facts {
		if v == "" {
			delete(facts, k)
		}

		if len(v) >= 100 {
			facts[k] = v[:97] + "..."
		}
	}
}

func byteCountDecimal(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}

	div, exp := int64(unit), 0

	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
