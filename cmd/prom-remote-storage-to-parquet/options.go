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
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
)

var (
	errInvalidPosArgs         = errors.New("expected exactly one positional argument <import | export>")
	errInvalidOperation       = errors.New("invalid operation")
	errNoTenantHeaderProvided = errors.New("no tenant header provided")
	errIsNotAbsolute          = errors.New("is not absolute")
	errNoInputFileProvided    = errors.New("no input file provided")
	errNoOutputFileProvided   = errors.New("no output file provided")
	errNoStartTimeProvided    = errors.New("no start time provided")
	errNoEndTimeProvided      = errors.New("no end time provided")
	errStartAfterEnd          = errors.New("start time can't be after end time")
	errNoLabelPairsProvided   = errors.New("no label pairs provided")
	errInvalidLabelPairFormat = errors.New("invalid format")
)

type options struct {
	operation             operationType
	tenantHeader          string
	writeURL, readURL     *url.URL
	preAggregURL          *url.URL
	inputFile, outputFile string
	start, end            time.Time
	labelPairs            map[string]string
}

func parseOptions(args []string) (options, error) {
	var (
		opts              options
		writeURL, readURL string
		preAggregURL      string // TODO: use another endpoint than http://localhost:9201/debug/preaggregate ?
		start, end        string
		labelPairs        string
	)

	flags := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	flags.SortFlags = false
	flags.Usage = func() {
		log.Info().Msgf("Usage:")
		fmt.Fprintln(os.Stderr, os.Args[0], "import --input-file=<path> [--start=<time>] [--end=<time>] [--labels=<k=v pairs>] [--write-url=<url>] [--pre-aggreg-url=<url>]") //nolint:lll
		fmt.Fprintln(os.Stderr, os.Args[0], "export --output-file=<path | -> --start=<time> --end=<time> --labels=<k=v pairs> [--read-url=<url>]")                            //nolint:lll
		fmt.Fprintln(os.Stderr, flags.FlagUsages())
	}

	flags.StringVar(&opts.tenantHeader, "tenant", "", "SquirrelDB tenant header")
	flags.StringVar(&writeURL, "write-url", "http://localhost:9201/api/v1/write", "SquirrelDB write URL")
	flags.StringVar(&readURL, "read-url", "http://localhost:9201/api/v1/read", "SquirrelDB read URL")
	flags.StringVar(&preAggregURL, "pre-aggreg-url", "", "SquirrelDB pre-aggregation URL (will only be triggered if specified)") //nolint:lll
	flags.StringVarP(&opts.inputFile, "input-file", "i", "", "Input file")
	flags.StringVarP(&opts.outputFile, "output-file", "o", "", "Output file (can be '-' for stdout)")
	flags.StringVar(&start, "start", "", "Start time")
	flags.StringVar(&end, "end", "", "End time")
	flags.StringVar(&labelPairs, "labels", "", "Label pairs (k1=v1,k2=v2,...)")

	err := flags.Parse(args)
	if err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			os.Exit(0)
		}

		return options{}, err
	}

	if len(flags.Args()) != 1 {
		return options{}, errInvalidPosArgs
	}

	switch strings.ToLower(flags.Arg(0)) {
	case opImport:
		opts.writeURL, err = url.Parse(writeURL)
		if err != nil {
			return options{}, fmt.Errorf("invalid write URL: %w", err)
		}

		if !opts.writeURL.IsAbs() {
			return options{}, fmt.Errorf("write URL %w", errIsNotAbsolute)
		}

		if preAggregURL != "" {
			opts.preAggregURL, err = url.Parse(preAggregURL)
			if err != nil {
				return options{}, fmt.Errorf("invalid pre-aggregation URL: %w", err)
			}
		}

		if opts.inputFile == "" {
			return options{}, errNoInputFileProvided
		}

		opts.operation = opImport
	case opExport:
		opts.readURL, err = url.Parse(readURL)
		if err != nil {
			return options{}, fmt.Errorf("invalid read URL: %w", err)
		}

		if !opts.readURL.IsAbs() {
			return options{}, fmt.Errorf("read URL %w", errIsNotAbsolute)
		}

		if opts.outputFile == "" {
			return options{}, errNoOutputFileProvided
		}

		if !strings.Contains(labelPairs, "=") {
			return options{}, errNoLabelPairsProvided
		}

		opts.operation = opExport
	default:
		return options{}, fmt.Errorf("%w: %s", errInvalidOperation, flags.Arg(0))
	}

	if opts.tenantHeader == "" {
		return options{}, errNoTenantHeaderProvided
	}

	if start == "" {
		if opts.operation == opImport {
			opts.start = time.Unix(0, 0)
		} else { // opExport
			return options{}, errNoStartTimeProvided
		}
	} else {
		opts.start, err = time.Parse(time.RFC3339, start)
		if err != nil {
			return options{}, fmt.Errorf("parsing start time: %w", err)
		}
	}

	if end == "" {
		if opts.operation == opImport {
			opts.end = time.Unix(0, 0)
		} else { // opExport
			return options{}, errNoEndTimeProvided
		}
	} else {
		opts.end, err = time.Parse(time.RFC3339, end)
		if err != nil {
			return options{}, fmt.Errorf("parsing end time: %w", err)
		}
	}

	if !opts.end.IsZero() && opts.start.After(opts.end) {
		return options{}, errStartAfterEnd
	}

	opts.labelPairs = make(map[string]string)

	if labelPairs != "" {
		for _, pair := range strings.Split(labelPairs, ",") {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				return options{}, fmt.Errorf("parsing label pair %q: %w", pair, errInvalidLabelPairFormat)
			}

			opts.labelPairs[kv[0]] = kv[1]
		}
	}

	return opts, nil
}
