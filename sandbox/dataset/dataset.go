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

// Package dataset loads the definition of a sandbox dataset: which series exist
// and where their samples come from. A default dataset is embedded in the
// binary; a dataset stored on disk can be used instead.
package dataset

import (
	"embed"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// defaultFS holds the default dataset, used when no dataset file is configured.
//
//go:embed default
var defaultFS embed.FS

const (
	defaultDir    = "default"
	defaultConfig = "default.yaml"
)

// Config is the content of a dataset definition file.
type Config struct {
	Files  Files  `yaml:"files"`
	Promql Promql `yaml:"promql"`
}

// Files defines the series replaying recorded samples.
type Files struct {
	// Metrics are files listing the series that exist, one per line.
	Metrics []MetricsFile `yaml:"metrics"`
	// Samples are files of recorded samples, as produced by squirreldb-sandbox-dump.
	Samples []SamplesFile `yaml:"samples"`
}

type MetricsFile struct {
	File string `yaml:"file"`
	// MatchingLabels are the labels (in addition to __name__) that must be equal
	// between a series and the samples it uses.
	MatchingLabels []string `yaml:"matching_labels"`
}

type SamplesFile struct {
	File string `yaml:"file"`
}

// Promql defines the series whose value is computed by a PromQL expression.
type Promql []PromqlMetric

type PromqlMetric struct {
	// Name is the series, in PromQL syntax (e.g. `metric_name{label="value"}`).
	Name        string `yaml:"name"`
	StepSeconds int    `yaml:"step_seconds"`
	// Query is evaluated at each step; it must return a scalar or a single sample.
	Query string `yaml:"query"`
}

// Dataset is a loaded dataset definition. Open gives access to the files it
// references.
type Dataset struct {
	Config Config

	open func(name string) (io.ReadCloser, error)
}

// Open opens a file referenced by the dataset definition. Relative paths are
// resolved relative to the directory of the definition file.
func (d *Dataset) Open(name string) (io.ReadCloser, error) {
	return d.open(name)
}

// Load reads the dataset definition stored at configPath. An empty configPath
// loads the default dataset embedded in SquirrelDB.
func Load(configPath string) (*Dataset, error) {
	if configPath == "" {
		return loadDefault()
	}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("open dataset config: %w", err)
	}

	defer file.Close()

	cfg, err := decode(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", configPath, err)
	}

	baseDir := filepath.Dir(configPath)

	return &Dataset{
		Config: cfg,
		open: func(name string) (io.ReadCloser, error) {
			if !filepath.IsAbs(name) {
				name = filepath.Join(baseDir, name)
			}

			return os.Open(name)
		},
	}, nil
}

func loadDefault() (*Dataset, error) {
	file, err := defaultFS.Open(path.Join(defaultDir, defaultConfig))
	if err != nil {
		return nil, fmt.Errorf("open embedded dataset config: %w", err)
	}

	defer file.Close()

	cfg, err := decode(file)
	if err != nil {
		return nil, fmt.Errorf("embedded dataset: %w", err)
	}

	return &Dataset{
		Config: cfg,
		open: func(name string) (io.ReadCloser, error) {
			return defaultFS.Open(path.Join(defaultDir, path.Clean(name)))
		},
	}, nil
}

func decode(r io.Reader) (Config, error) {
	var cfg Config

	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode dataset config: %w", err)
	}

	return cfg, nil
}
