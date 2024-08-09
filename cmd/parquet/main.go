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
	"os"

	"github.com/bleemeo/squirreldb/logger"

	"github.com/rs/zerolog/log"
)

type operationType = string

const (
	opImport operationType = "import"
	opExport operationType = "export"
)

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

// nolint: unparam,lll,nolintlint
func importData(opts options) error {
	log.Info().Str("tenant", opts.tenantHeader).Stringer("write-url", opts.writeURL).Str("input-file", opts.inputFile).Time("start", opts.start).Time("end", opts.end).Any("labels", opts.labelPairs).Msg("Import options")

	// TODO

	return nil
}
