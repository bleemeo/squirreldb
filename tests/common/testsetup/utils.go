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

package testsetup

import (
	"errors"
	"os"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ErrSkipBackend is returned by a test tool's run function when the configured
// backend does not support what the tool exercises (e.g. the lock benchmark or
// the Cassandra-index benchmarks on a ClickHouse backend). Tools treat it as a
// clean skip — no fatal error, minimal output — so run-tests.sh can launch every
// tool against any backend and let unsupported ones skip themselves.
var ErrSkipBackend = errors.New("test skipped: not supported by the configured backend")

// SetTestEnvironment configure few environment variable used in testing.
// This method MUST be called before first usage of Config().
// It will set Cassandra & Redis to use alternative keyspace to avoid confict with
// normal SquirrelDB. It will only do it if not explicitly set.
func SetTestEnvironment() {
	if _, ok := os.LookupEnv("SQUIRRELDB_CASSANDRA_KEYSPACE"); !ok {
		// If not explicitly changed, use squirreldb_test as keyspace. We do
		// not want to touch real data
		_ = os.Setenv("SQUIRRELDB_CASSANDRA_KEYSPACE", "squirreldb_test")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_REDIS_KEYSPACE"); !ok {
		// If not explicitly changed, use test: as namespace. We do
		// not want to touch real data
		_ = os.Setenv("SQUIRRELDB_REDIS_KEYSPACE", "test:")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_CLICKHOUSE_DATABASE"); !ok {
		// If not explicitly changed, use the test database (the only one DropData
		// will drop). We do not want to touch real data.
		_ = os.Setenv("SQUIRRELDB_CLICKHOUSE_DATABASE", "squirreldb_test")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_LISTEN_ADDRESS"); !ok {
		// If not explicitly set, use a dynamic port.
		_ = os.Setenv("SQUIRRELDB_LISTEN_ADDRESS", "127.0.0.1:0")
	}

	if _, ok := os.LookupEnv("SQUIRRELDB_TELEMETRY_ENABLED"); !ok {
		// If not explicitly set, disable telemetry.
		_ = os.Setenv("SQUIRRELDB_TELEMETRY_ENABLED", "false")
	}

	// Initialize the logger temporarily before loading the config.
	log.Logger = logger.NewLogger(logger.NewConsoleWriter(false), zerolog.TraceLevel)

	cfg, _, _ := LoadConfigForTest()

	log.Logger = logger.NewTestLogger(cfg.Log.DisableColor)
}

func LoadConfigForTest() (config.Config, error, error) {
	_, cfg, warnings, err := config.LoadConfigFromFlags("squirreldb-test", nil, os.Args[1:])
	if errors.Is(err, config.ErrNormalExit) {
		os.Exit(0)
	}

	return cfg, warnings, err
}
