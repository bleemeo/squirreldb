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

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
)

var (
	errInvalidPosArgs        = errors.New("expected exactly one positional argument <import | export>")
	errInvalidOperation      = errors.New("invalid operation")
	errIsNotAbsolute         = errors.New("is not absolute")
	errNoInputFileProvided   = errors.New("no input file provided")
	errNoOutputFileProvided  = errors.New("no output file provided")
	errNoStartTimeProvided   = errors.New("no start time provided")
	errNoEndTimeProvided     = errors.New("no end time provided")
	errStartAfterEnd         = errors.New("start time can't be after end time")
	errInvalidMetricSelector = errors.New("invalid metric selector")
)

type options struct {
	operation               operationType
	tenantHeader            string
	squirrelDBMaxEvalSeries uint32
	squirrelDBMaxEvalPoints uint64
	writeURL, readURL       *url.URL
	preAggregURL            *url.URL
	inputFile, outputFile   string
	start, end              time.Time
	metricSelector          string
	labelMatchers           []*labels.Matcher
}

func parseOptions(args []string) (options, error) {
	var (
		opts              options
		writeURL, readURL string
		preAggregURL      string
		start, end        string
	)

	flags := pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	flags.SortFlags = false
	flags.Usage = func() {
		log.Info().Msgf("Usage:")
		fmt.Fprintln(os.Stderr, os.Args[0], "import --input-file=<path> [--start=<time>] [--end=<time>] [--metric-selector=<k=v pairs>] [--write-url=<url>] [--tenant=<tenant>] [--pre-aggreg-url=<url>]")                                                                        //nolint:lll
		fmt.Fprintln(os.Stderr, os.Args[0], "export --output-file=<path | -> --metric-selector=<promql selector> [--start=<time>] [--end=<time>] [--read-url=<url>] [--tenant=<tenant>] [--squirreldb-max-evaluated-series=<count>] [--squirreldb-max-evaluated-points=<count>]") //nolint:lll
		fmt.Fprintln(os.Stderr, flags.FlagUsages())
	}

	now := time.Now().Truncate(time.Second)

	flags.StringVar(&opts.tenantHeader, "tenant", "", "SquirrelDB tenant header")
	flags.Uint32Var(&opts.squirrelDBMaxEvalSeries, "squirreldb-max-evaluated-series", 10_000, "Max evaluated series on SquirrelDB (0=unlimited)")    //nolint:lll
	flags.Uint64Var(&opts.squirrelDBMaxEvalPoints, "squirreldb-max-evaluated-points", 2_000_000, "Max evaluated points on SquirrelDB (0=unlimited)") //nolint:lll
	flags.StringVar(&writeURL, "write-url", "http://localhost:9201/api/v1/write", "SquirrelDB write URL")
	flags.StringVar(&readURL, "read-url", "http://localhost:9201/api/v1/read", "SquirrelDB read URL")
	flags.StringVar(&preAggregURL, "pre-aggreg-url", "", "SquirrelDB pre-aggregation URL (if left blank, it will use the host of the write-url / to disable it, set to 'skip')") //nolint:lll
	flags.StringVarP(&opts.inputFile, "input-file", "i", "", "Input file")
	flags.StringVarP(&opts.outputFile, "output-file", "o", "", "Output file (can be '-' for stdout)")
	flags.StringVar(&start, "start", now.Add(-24*time.Hour).Format(time.RFC3339), "Start time")
	flags.StringVar(&end, "end", now.Format(time.RFC3339), "End time")
	flags.StringVar(&opts.metricSelector, "metric-selector", "", "PromQL metric selector")

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

		if preAggregURL != "skip" {
			if preAggregURL == "" {
				opts.preAggregURL = opts.writeURL.ResolveReference(&url.URL{Path: "/debug/preaggregate"})
			} else {
				opts.preAggregURL, err = url.Parse(preAggregURL)
				if err != nil {
					return options{}, fmt.Errorf("invalid pre-aggregation URL: %w", err)
				}
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

		if opts.metricSelector == "" {
			return options{}, errInvalidMetricSelector
		}

		opts.operation = opExport
	default:
		return options{}, fmt.Errorf("%w: %s", errInvalidOperation, flags.Arg(0))
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

	if opts.metricSelector != "" {
		opts.labelMatchers, err = parser.ParseMetricSelector(opts.metricSelector)
		if err != nil {
			return options{}, fmt.Errorf("%w: %w", errInvalidMetricSelector, err)
		}
	}

	return opts, nil
}
