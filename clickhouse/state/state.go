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

// Package state provides a small, generic key/value store backed by ClickHouse.
//
// It is a single ReplacingMergeTree(version) table: a Set is a plain INSERT and
// the latest row (highest version) wins; reads use FINAL to collapse the
// duplicate versions left until the next background merge. It is low-volume
// bookkeeping (the first user is the Cassandra→ClickHouse migration checkpoint),
// so FINAL and lightweight DELETE are fine.
//
// For read-your-writes (e.g. a migrator reading back its own checkpoint after a
// restart) the backend hands this store a sticky connection pinned to one
// replica; across replicas a just-written value may lag until replication.
package state

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
	"github.com/bleemeo/squirreldb/types"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const stateDDL = `
	CREATE TABLE IF NOT EXISTS state{{ON_CLUSTER}} (
		name String,
		value String,
		version DateTime64(3) DEFAULT now64(3)
	)
	ENGINE = {{ENGINE}}
	ORDER BY (name)`

var errReadOnly = errors.New("backend is read-only")

// SchemaObjects returns the ClickHouse objects owned by the state store. Exposed
// so the backend can create the whole schema up front, independently of whether
// the store is ever instantiated.
func SchemaObjects(clusterName string) []ddl.Object {
	engine := ddl.ReplicatedEngine(clusterName, "state", "ReplacingMergeTree", "version")
	stmtDDL := strings.Replace(stateDDL, "{{ENGINE}}", engine, 1)

	return []ddl.Object{{Name: "state", DDL: ddl.Apply(stmtDDL, clusterName), Replicated: true}}
}

// Store is a key/value store backed by ClickHouse.
type Store struct {
	conn     driver.Conn
	readOnly bool
}

// New returns a state store over the given connection. The connection is owned
// by the caller (clickhouse/backend); the table is created up front by the
// backend, so New assumes it exists.
func New(conn driver.Conn, readOnly bool) *Store {
	return &Store{conn: conn, readOnly: readOnly}
}

// Read returns the value stored under name and whether it was found.
func (s *Store) Read(ctx context.Context, name string, value any) (found bool, err error) {
	var valueString string

	rows, err := s.conn.Query(ctx, "SELECT value FROM state FINAL WHERE name = ? LIMIT 1", name)
	if err != nil {
		return false, fmt.Errorf("select state %q: %w", name, err)
	}

	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("select state %q: %w", name, err)
		}

		return false, nil
	}

	if err := rows.Scan(&valueString); err != nil {
		return false, fmt.Errorf("scan state %q: %w", name, err)
	}

	if err := types.DecodeStateValue(valueString, value); err != nil {
		return false, err
	}

	return true, nil
}

// Write stores value under name (last write wins on the next read).
func (s *Store) Write(ctx context.Context, name string, value any) error {
	if s.readOnly {
		return errReadOnly
	}

	valueString, err := types.EncodeStateValue(value)
	if err != nil {
		return err
	}

	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO state (name, value)")
	if err != nil {
		return fmt.Errorf("prepare insert state %q: %w", name, err)
	}

	defer batch.Close()

	if err := batch.Append(name, valueString); err != nil {
		return fmt.Errorf("append state %q: %w", name, err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert state %q: %w", name, err)
	}

	return nil
}

// Delete removes the value stored under name (lightweight delete, visible to the
// next read).
func (s *Store) Delete(ctx context.Context, name string) error {
	if s.readOnly {
		return errReadOnly
	}

	if err := s.conn.Exec(ctx, "DELETE FROM state WHERE name = ?", name); err != nil {
		return fmt.Errorf("delete state %q: %w", name, err)
	}

	return nil
}
