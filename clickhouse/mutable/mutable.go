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

// Package mutable provides the ClickHouse-backed implementation of the mutable
// label store (mutable.Store). The backend-agnostic provider, cache and types
// live in the neutral github.com/bleemeo/squirreldb/mutable package.
//
// Mutable labels are low-volume, admin-managed data (set/deleted through the
// import API), so the schema favours simplicity over write throughput:
//
//   - both tables are ReplacingMergeTree(version); a set is a plain INSERT and
//     the latest row (highest version) wins. Reads use FINAL to collapse the
//     duplicate versions left until the next background merge.
//   - deletes use lightweight DELETE (DELETE FROM ... WHERE), which ClickHouse
//     applies immediately to subsequent SELECTs. This handles both the delete
//     of a single (tenant, name, value) and the delete of every value of a
//     (tenant, name) — the latter has no cheap equivalent with tombstone rows.
package mutable

import (
	"context"
	"fmt"
	"strings"

	"github.com/bleemeo/squirreldb/clickhouse/ddl"
	"github.com/bleemeo/squirreldb/mutable"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog"
)

// Options configures the ClickHouse mutable label store.
type Options struct {
	ReadOnly bool
	// ClusterName enables ClickHouse cluster mode (ON CLUSTER + Replicated with
	// an explicit, name-based Keeper path; the tables must not be RENAMEd).
	ClusterName string
	Logger      zerolog.Logger
}

type clickhouseStore struct {
	conn    driver.Conn
	options Options
}

// version is set by the DEFAULT now64(3) on the table, so a later INSERT for the
// same key always wins on read with FINAL.
const (
	namesDDL = `
		CREATE TABLE IF NOT EXISTS mutable_label_names{{ON_CLUSTER}} (
			tenant LowCardinality(String) CODEC(ZSTD(1)),
			name String,
			associated_name String,
			version DateTime64(3) DEFAULT now64(3)
		)
		ENGINE = {{ENGINE}}
		ORDER BY (tenant, name)`

	valuesDDL = `
		CREATE TABLE IF NOT EXISTS mutable_label_values{{ON_CLUSTER}} (
			tenant LowCardinality(String) CODEC(ZSTD(1)),
			name String,
			value String,
			associated_values Array(String) CODEC(ZSTD(1)),
			version DateTime64(3) DEFAULT now64(3)
		)
		ENGINE = {{ENGINE}}
		ORDER BY (tenant, name, value)`
)

// NewStore returns a new ClickHouse mutable label store. The connection is owned
// by the caller (clickhouse/backend) and shared with the other ClickHouse stores.
func NewStore(ctx context.Context, conn driver.Conn, options Options) (mutable.Store, error) {
	// Schema creation is owned by clickhouse/backend (created up front for every
	// store), so the store assumes its tables already exist.
	return &clickhouseStore{
		conn:    conn,
		options: options,
	}, nil
}

// SchemaObjects returns the ClickHouse objects owned by the mutable labels store.
// Exposed so the backend can create the whole schema up front and derive the
// expected-table set, independently of whether the store is ever instantiated.
func SchemaObjects(clusterName string) []ddl.Object {
	statements := []struct {
		table string
		ddl   string
	}{
		{"mutable_label_names", namesDDL},
		{"mutable_label_values", valuesDDL},
	}

	objects := make([]ddl.Object, 0, len(statements))

	for _, stmt := range statements {
		engine := ddl.ReplicatedEngine(clusterName, stmt.table, "ReplacingMergeTree", "version")
		stmtDDL := strings.Replace(stmt.ddl, "{{ENGINE}}", engine, 1)
		objects = append(objects, ddl.Object{Name: stmt.table, DDL: ddl.Apply(stmtDDL, clusterName), Replicated: true})
	}

	return objects
}

// AssociatedNames return the associated names map[mutable label name] -> non mutable label name.
func (s *clickhouseStore) AssociatedNames(ctx context.Context, tenant string) (map[string]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT name, associated_name FROM mutable_label_names FINAL
		WHERE tenant = ?`,
		tenant,
	)
	if err != nil {
		return nil, fmt.Errorf("select labels: %w", err)
	}

	defer rows.Close()

	associatedNames := make(map[string]string)

	for rows.Next() {
		var name, associatedName string

		if err := rows.Scan(&name, &associatedName); err != nil {
			return nil, fmt.Errorf("scan label: %w", err)
		}

		associatedNames[name] = associatedName
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select labels: %w", err)
	}

	return associatedNames, nil
}

// AssociatedValues returns the associated values for a mutable label name and tenant.
func (s *clickhouseStore) AssociatedValues(ctx context.Context, tenant, name string) (map[string][]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT value, associated_values FROM mutable_label_values FINAL
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	)
	if err != nil {
		return nil, fmt.Errorf("select values: %w", err)
	}

	defer rows.Close()

	values := make(map[string][]string)

	for rows.Next() {
		var (
			value            string
			associatedValues []string
		)

		if err := rows.Scan(&value, &associatedValues); err != nil {
			return nil, fmt.Errorf("scan value: %w", err)
		}

		values[value] = associatedValues
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select values: %w", err)
	}

	return values, nil
}

// SetAssociatedValues sets the non mutable label values associated to a mutable label.
func (s *clickhouseStore) SetAssociatedValues(ctx context.Context, label mutable.LabelWithValues) error {
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO mutable_label_values (tenant, name, value, associated_values)")
	if err != nil {
		return fmt.Errorf("prepare insert associated values: %w", err)
	}

	defer batch.Close()

	if err := batch.Append(label.Tenant, label.Name, label.Value, label.AssociatedValues); err != nil {
		return fmt.Errorf("append associated values: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert associated values: %w", err)
	}

	return nil
}

// DeleteAssociatedValues deletes the non mutable label values associated to a mutable label.
func (s *clickhouseStore) DeleteAssociatedValues(ctx context.Context, label mutable.Label) error {
	err := s.conn.Exec(ctx, `
		DELETE FROM mutable_label_values
		WHERE tenant = ? AND name = ? AND value = ?`,
		label.Tenant, label.Name, label.Value,
	)
	if err != nil {
		return fmt.Errorf("delete associated values: %w", err)
	}

	return nil
}

// SetAssociatedName sets the non mutable label name associated to a mutable label name.
func (s *clickhouseStore) SetAssociatedName(ctx context.Context, label mutable.LabelWithName) error {
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO mutable_label_names (tenant, name, associated_name)")
	if err != nil {
		return fmt.Errorf("prepare insert associated name: %w", err)
	}

	defer batch.Close()

	if err := batch.Append(label.Tenant, label.Name, label.AssociatedName); err != nil {
		return fmt.Errorf("append associated name: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert associated name: %w", err)
	}

	return nil
}

// DeleteAssociatedName deletes a mutable label name and all its values.
func (s *clickhouseStore) DeleteAssociatedName(ctx context.Context, tenant, name string) error {
	err := s.conn.Exec(ctx, `
		DELETE FROM mutable_label_names
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	)
	if err != nil {
		return fmt.Errorf("delete label name from mutable_label_names: %w", err)
	}

	err = s.conn.Exec(ctx, `
		DELETE FROM mutable_label_values
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	)
	if err != nil {
		return fmt.Errorf("delete label name from mutable_label_values: %w", err)
	}

	return nil
}

// AllMutableLabels return the list of all known mutable labels from all tenants.
// Currently this function is not optimized (it does a full scan) and should be avoided if possible.
func (s *clickhouseStore) AllMutableLabels(ctx context.Context) ([]mutable.LabelWithName, error) {
	rows, err := s.conn.Query(ctx,
		"SELECT tenant, name, associated_name FROM mutable_label_names FINAL")
	if err != nil {
		return nil, fmt.Errorf("select tenants: %w", err)
	}

	defer rows.Close()

	result := make([]mutable.LabelWithName, 0)

	for rows.Next() {
		var currentLabel mutable.LabelWithName

		if err := rows.Scan(&currentLabel.Tenant, &currentLabel.Name, &currentLabel.AssociatedName); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}

		result = append(result, currentLabel)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select tenants: %w", err)
	}

	return result, nil
}
