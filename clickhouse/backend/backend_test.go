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

package backend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
	"github.com/bleemeo/squirreldb/config"

	"github.com/rs/zerolog"
)

// TestDropTablesRefusesNonTestDatabase checks the safety guard: dropData must
// refuse any database other than the test one, so it can never wipe production
// tables. The guard runs before any connection, so no ClickHouse is needed.
func TestDropTablesRefusesNonTestDatabase(t *testing.T) {
	b, err := New(context.Background(), zerolog.Nop(), Options{
		Config: config.ClickHouse{Database: "production"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = b.dropData(context.Background())
	if err == nil {
		t.Fatal("dropData should refuse a non-test database")
	}

	// The error should name the refused database (it must not have tried to connect).
	if !strings.Contains(err.Error(), "production") {
		t.Errorf("error should name the refused database, got: %v", err)
	}
}

// TestWithSchemaHint checks a METADATA_MISMATCH on tsdb_tags (a server joining a
// cluster whose tsdb_tags has the legacy sorting key) gets the hint pointing to
// the manual procedure, and that other errors are returned unchanged.
func TestWithSchemaHint(t *testing.T) {
	t.Parallel()

	cause := errors.New("Code: 342. METADATA_MISMATCH")

	tagsErr := fmt.Errorf("wrapped: %w", &ddl.MetadataMismatchError{Table: "tsdb_tags", Err: cause})

	got := withSchemaHint(tagsErr)
	if !errors.Is(got, cause) {
		t.Errorf("the original error is not wrapped: %v", got)
	}

	for _, want := range []string{"legacy", "(tenant, metric_name, id)", legacyTagsKeyDoc} {
		if !strings.Contains(got.Error(), want) {
			t.Errorf("hint should contain %q, got: %v", want, got)
		}
	}

	for _, err := range []error{
		nil,
		cause,
		&ddl.MetadataMismatchError{Table: "tsdb_data", Err: cause},
	} {
		if got := withSchemaHint(err); got != err { //nolint:errorlint // must be the same error
			t.Errorf("withSchemaHint(%v) = %v, want it unchanged", err, got)
		}
	}
}
