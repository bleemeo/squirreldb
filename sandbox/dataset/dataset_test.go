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

package dataset

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func readAll(t *testing.T, ds *Dataset, name string) string {
	t.Helper()

	r, err := ds.Open(name)
	if err != nil {
		t.Fatalf("Open(%q): %v", name, err)
	}

	defer r.Close()

	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	return string(content)
}

func TestLoadDefault(t *testing.T) {
	t.Parallel()

	ds, err := Load("")
	if err != nil {
		t.Fatal(err)
	}

	if len(ds.Config.Files.Metrics) == 0 || len(ds.Config.Files.Samples) == 0 || len(ds.Config.Promql) == 0 {
		t.Fatalf("default dataset is incomplete: %+v", ds.Config)
	}

	// Every file referenced by the default dataset must be embedded.
	for _, m := range ds.Config.Files.Metrics {
		readAll(t, ds, m.File)
	}

	for _, s := range ds.Config.Files.Samples {
		readAll(t, ds, s.File)
	}
}

// TestLoadFromDisk checks that relative paths are resolved relative to the
// directory of the dataset definition, and absolute paths are kept.
func TestLoadFromDisk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	otherDir := t.TempDir()

	writeFile := func(path, content string) {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	absoluteSamples := filepath.Join(otherDir, "samples.json")

	writeFile(filepath.Join(dir, "metrics.txt"), "relative")
	writeFile(absoluteSamples, "absolute")
	writeFile(filepath.Join(dir, "dataset.yaml"), `
files:
  metrics:
    - file: ./metrics.txt
      matching_labels: [item]
  samples:
    - file: `+absoluteSamples+`
promql:
  - name: expiry
    step_seconds: 10
    query: time()
`)

	ds, err := Load(filepath.Join(dir, "dataset.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if got := ds.Config.Files.Metrics[0].MatchingLabels; len(got) != 1 || got[0] != "item" {
		t.Errorf("matching_labels = %v, want [item]", got)
	}

	if got := readAll(t, ds, ds.Config.Files.Metrics[0].File); got != "relative" {
		t.Errorf("relative file content = %q, want %q", got, "relative")
	}

	if got := readAll(t, ds, ds.Config.Files.Samples[0].File); got != "absolute" {
		t.Errorf("absolute file content = %q, want %q", got, "absolute")
	}
}

func TestLoadErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "dataset.yaml")

	if err := os.WriteFile(path, []byte("files:\n  metric:\n    - file: a.txt\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Load(path); err == nil {
		t.Error("expected an error for an unknown field")
	}

	if _, err := Load(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Error("expected an error for a missing file")
	}
}
