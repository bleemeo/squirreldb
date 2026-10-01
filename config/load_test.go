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

package config

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// migrateLikeFlags mimics the command flags of a subcommand such as `migrate`.
func migrateLikeFlags() []Flag {
	return []Flag{
		{Name: "batch-size", Value: 1_000_000, Usage: "Points per insert"},
		{Name: "print-metrics", Value: false, Usage: "Print metrics on exit"},
	}
}

// TestLoadConfigFromFlags tests that every flag is applied regardless of its
// position relative to the command flags (--config, subcommand flags), and that
// unknown flags or positional arguments are rejected.
func TestLoadConfigFromFlags(t *testing.T) {
	tests := []struct {
		Name          string
		CommandFlags  []Flag
		Args          []string
		Environment   map[string]string
		WantListen    string
		WantRedis     []string
		WantCassandra []string
		WantBatchSize int
		WantErr       error
		WantErrSubstr string
	}{
		{
			Name:         "flag after --config",
			CommandFlags: DaemonConfigFlags(),
			Args:         []string{"--config", "testdata/flags.conf", "--listen_address", "127.0.0.1:19201"},
			WantListen:   "127.0.0.1:19201",
		},
		{
			Name:         "flag before --config",
			CommandFlags: DaemonConfigFlags(),
			Args:         []string{"--listen_address", "127.0.0.1:19201", "--config", "testdata/flags.conf"},
			WantListen:   "127.0.0.1:19201",
		},
		{
			Name:         "equal form",
			CommandFlags: DaemonConfigFlags(),
			Args:         []string{"--config=testdata/flags.conf", "--listen_address=127.0.0.1:19201"},
			WantListen:   "127.0.0.1:19201",
		},
		{
			Name:         "short config flag",
			CommandFlags: DaemonConfigFlags(),
			Args:         []string{"-c", "testdata/flags.conf", "--redis.addresses=10.0.0.1:6379,10.0.0.2:6379"},
			WantRedis:    []string{"10.0.0.1:6379", "10.0.0.2:6379"},
		},
		{
			Name:         "config flags around subcommand flags",
			CommandFlags: migrateLikeFlags(),
			Args: []string{
				"--batch-size", "42",
				"--config", "testdata/flags.conf",
				"--print-metrics",
				"--redis.addresses", "10.0.0.1:6379",
			},
			WantRedis:     []string{"10.0.0.1:6379"},
			WantBatchSize: 42,
		},
		{
			Name:         "precedence: flag over env over file over default",
			CommandFlags: DaemonConfigFlags(),
			Args:         []string{"--config", "testdata/flags.conf", "--redis.addresses", "10.0.0.4:6379"},
			Environment: map[string]string{
				"SQUIRRELDB_REDIS_ADDRESSES":     "10.0.0.3:6379",
				"SQUIRRELDB_CASSANDRA_ADDRESSES": "10.0.0.3:9042",
			},
			// listen_address keeps its default, cassandra comes from env (over the
			// file), redis comes from the flag (over env and file).
			WantListen:    DefaultConfig().ListenAddress,
			WantRedis:     []string{"10.0.0.4:6379"},
			WantCassandra: []string{"10.0.0.3:9042"},
		},
		{
			Name:          "unknown flag",
			CommandFlags:  DaemonConfigFlags(),
			Args:          []string{"--config", "testdata/flags.conf", "--no-such-flag", "x"},
			WantErrSubstr: "unknown flag: --no-such-flag",
		},
		{
			Name:          "daemon-only flag on a subcommand",
			CommandFlags:  migrateLikeFlags(),
			Args:          []string{"--config", "testdata/flags.conf", "--listen_address", "127.0.0.1:19201"},
			WantErrSubstr: "unknown flag: --listen_address",
		},
		{
			Name:         "subcommand after a flag",
			CommandFlags: DaemonConfigFlags(),
			Args:         []string{"--config", "testdata/flags.conf", "migrate"},
			WantErr:      ErrUnexpectedArgument,
		},
		{
			Name:         "help",
			CommandFlags: DaemonConfigFlags(),
			Args:         []string{"--config", "testdata/flags.conf", "--help"},
			WantErr:      ErrNormalExit,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			for k, v := range test.Environment {
				t.Setenv(k, v)
			}

			flagSet, cfg, warnings, err := LoadConfigFromFlags("test", test.CommandFlags, test.Args)

			switch {
			case test.WantErr != nil:
				if !errors.Is(err, test.WantErr) {
					t.Fatalf("Expected error %v, got %v", test.WantErr, err)
				}

				return
			case test.WantErrSubstr != "":
				if err == nil || !strings.Contains(err.Error(), test.WantErrSubstr) {
					t.Fatalf("Expected error containing %q, got %v", test.WantErrSubstr, err)
				}

				return
			case err != nil:
				t.Fatalf("Unexpected error: %v", err)
			}

			if warnings != nil {
				t.Fatalf("Unexpected warnings: %v", warnings)
			}

			wantListen := test.WantListen
			if wantListen == "" {
				wantListen = DefaultConfig().ListenAddress
			}

			// Values not overridden come from testdata/flags.conf.
			wantRedis := test.WantRedis
			if wantRedis == nil {
				wantRedis = []string{"192.168.0.2:6379"}
			}

			wantCassandra := test.WantCassandra
			if wantCassandra == nil {
				wantCassandra = []string{"192.168.0.2:9042"}
			}

			if cfg.ListenAddress != wantListen {
				t.Errorf("ListenAddress = %q, want %q", cfg.ListenAddress, wantListen)
			}

			if diff := cmp.Diff(wantRedis, cfg.Redis.Addresses); diff != "" {
				t.Errorf("Unexpected Redis addresses:\n%s", diff)
			}

			if diff := cmp.Diff(wantCassandra, cfg.Cassandra.Addresses); diff != "" {
				t.Errorf("Unexpected Cassandra addresses:\n%s", diff)
			}

			if !cfg.Internal.DisablePreAggregation {
				t.Errorf("DisablePreAggregation from the config file was not applied")
			}

			if test.WantBatchSize != 0 {
				if got, _ := flagSet.GetInt("batch-size"); got != test.WantBatchSize {
					t.Errorf("batch-size = %d, want %d", got, test.WantBatchSize)
				}

				if got, _ := flagSet.GetBool("print-metrics"); !got {
					t.Errorf("print-metrics = false, want true")
				}
			}
		})
	}
}
