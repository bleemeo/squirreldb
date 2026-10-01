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

// Command squirreldb-sandbox-samples applies simple modifications (like dropping
// NaN values) to a samples file of a sandbox dataset, read on stdin and written
// on stdout. See docs/sandbox.md.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"

	"github.com/bleemeo/squirreldb/sandbox/source/file"
)

//nolint:gochecknoglobals
var (
	dropNaN   = flag.Bool("drop-nan", false, "Drop NaN samples")
	dropAbove = flag.Float64("drop-above", math.NaN(), "Drop samples with value above given value.")
)

func main() {
	flag.Parse()

	if err := do(); err != nil {
		log.Println(err)

		os.Exit(1)
	}
}

func do() error {
	var row file.SamplesJSON

	dec := json.NewDecoder(os.Stdin)
	enc := json.NewEncoder(os.Stdout)

	for {
		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("can't decode row: %w", err)
		}

		n := 0

		for _, x := range row.Samples {
			v, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return fmt.Errorf("bad value: %w", err)
			}

			if keepSample(v) {
				row.Samples[n] = x
				n++
			}
		}

		row.Samples = row.Samples[:n]

		err = enc.Encode(row)
		if err != nil {
			return fmt.Errorf("can't write: %w", err)
		}
	}

	return nil
}

func keepSample(v float64) bool {
	if *dropNaN && math.IsNaN(v) {
		return false
	}

	if !math.IsNaN(*dropAbove) && v > *dropAbove {
		return false
	}

	return true
}
