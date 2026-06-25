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
	"strings"
	"testing"

	"github.com/bleemeo/squirreldb/config"

	"github.com/rs/zerolog"
)

// TestDropTablesRefusesNonTestDatabase checks the safety guard: DropTables must
// refuse any database other than the test one unless forced, so it can never wipe
// production tables. The guard runs before any connection, so no ClickHouse is needed.
func TestDropTablesRefusesNonTestDatabase(t *testing.T) {
	b, err := New(context.Background(), zerolog.Nop(), Options{
		Config: config.ClickHouse{Database: "production"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = b.DropData(context.Background(), false)
	if err == nil {
		t.Fatal("DropData should refuse a non-test database without forceNonTestDatabase")
	}

	// The error should name the refused database (it must not have tried to connect).
	if !strings.Contains(err.Error(), "production") {
		t.Errorf("error should name the refused database, got: %v", err)
	}
}
