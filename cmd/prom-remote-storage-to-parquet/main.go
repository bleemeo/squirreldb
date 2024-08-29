// Copyright 2015-2024 Bleemeo
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
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bleemeo/squirreldb/logger"

	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/rs/zerolog/log"
)

type operationType = string

const (
	opImport operationType = "import"
	opExport operationType = "export"
)

var errNoSeriesFound = errors.New("no series found")

func main() {
	log.Logger = logger.NewTestLogger(false)

	opts, err := parseOptions(os.Args[1:])
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse arguments")
	}

	switch opts.operation {
	case opImport:
		err = importData(opts)
	case opExport:
		err = exportData(opts)
	}

	if err != nil {
		log.Fatal().Err(err).Msgf("Failed to run parquet %s", opts.operation)
	}
}

// formatTimeRange returns the difference between the two given dates
// like time.Duration.String() rounded to seconds,
// but if the diff is more than 24h, it also displays the days.
func formatTimeRange(start, end time.Time) string {
	d := end.Sub(start).Round(time.Second)

	days := d / (24 * time.Hour)
	if days > 0 {
		remaining := d % (24 * time.Hour)
		if remaining == 0 {
			return fmt.Sprintf("%dd", days)
		}

		return fmt.Sprintf("%dd%s", days, remaining)
	}

	return d.String()
}

// tryParseErrorBody decodes the body and returns a string representation of it.
func tryParseErrorBody(body io.Reader) string {
	content, err := io.ReadAll(body)
	if err != nil {
		return "can't read response body: " + err.Error()
	}

	jsonDec := json.NewDecoder(bytes.NewReader(content))
	jsonDec.DisallowUnknownFields() // Ensure the type we get is the type we want

	var promErrorResp v1.Response
	if err = jsonDec.Decode(&promErrorResp); err == nil {
		if promErrorResp.Status == "error" {
			return fmt.Sprintf("%s: %s", promErrorResp.ErrorType, promErrorResp.Error)
		}

		return fmt.Sprintf("%+v", promErrorResp)
	}

	return string(content)
}
