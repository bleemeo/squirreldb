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

// Package mutable provides the Cassandra-backed implementation of the mutable
// label store (mutable.Store). The backend-agnostic provider, cache and types
// live in the neutral github.com/bleemeo/squirreldb/mutable package.
package mutable

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/mutable"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

type Options struct {
	Connection *connection.Connection
	ReadOnly   bool
	Logger     zerolog.Logger
}

type cassandra struct {
	options Options
}

// NewCassandraStore returns a new Cassandra mutable label store.
func NewCassandraStore(ctx context.Context, options Options) (mutable.Store, error) {
	cp := &cassandra{
		options: options,
	}

	if err := cp.createTables(ctx); err != nil {
		return nil, err
	}

	return cp, nil
}

// createTables creates the mutable labels table if it doesn't exist.
func (cp *cassandra) createTables(ctx context.Context) error {
	if cp.options.ReadOnly {
		cp.options.Logger.Debug().Msg("Read-only mode is activated. Not trying to create tables and assuming they exist")

		return nil
	}

	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	// mutable_label_* tables have very low write throughput (at most a few writes per week).
	// Without memtable_flush_period_in_ms, their memtable is only flushed when the commitlog
	// reaches its maximum size, which can block commitlog purge for days.
	// 1 hour is more than enough given the actual write frequency.
	const mutableMemtableFlushPeriod = 15 * time.Minute

	queryMutableLabelValues := session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_label_values (
			tenant text,
			name text,
			value text,
			associated_values frozen<list<text>>,
			PRIMARY KEY ((tenant, name), value)
		)
		WITH memtable_flush_period_in_ms = ` + strconv.FormatInt(mutableMemtableFlushPeriod.Milliseconds(), 10),
	).WithContext(ctx)

	queryMutableLabelNames := session.Query(`
		CREATE TABLE IF NOT EXISTS mutable_label_names (
			tenant text,
			name text,
			associated_name text,
			PRIMARY KEY (tenant, name)
		)
		WITH memtable_flush_period_in_ms = ` + strconv.FormatInt(mutableMemtableFlushPeriod.Milliseconds(), 10),
	).WithContext(ctx)

	for _, query := range []*gocql.Query{queryMutableLabelValues, queryMutableLabelNames} {
		query.Consistency(gocql.All)

		if err := query.Exec(); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
	}

	// ALTER TABLE is needed for tables that already exist in production (CREATE TABLE IF NOT EXISTS
	// does not update options on existing tables).
	// We only alter if memtable_flush_period_in_ms is not already set, so that a DBA can override it.
	for _, tableName := range []string{"mutable_label_values", "mutable_label_names"} {
		alreadySet, err := memtableFlushPeriodIsSet(ctx, session, cp.options.Connection.Keyspace(), tableName)
		if err != nil {
			// memtable_flush_period_in_ms may not exist in system_schema.tables on some Cassandra versions
			// (e.g. Cassandra 5 deprecated the option). Log and skip rather than failing startup.
			cp.options.Logger.Warn().Err(err).Msgf(
				"Could not check memtable_flush_period_in_ms for %s, skipping ALTER TABLE",
				tableName,
			)

			continue
		}

		if alreadySet {
			continue
		}

		q := (`ALTER TABLE ` +
			tableName +
			` WITH memtable_flush_period_in_ms = ` +
			strconv.FormatInt(mutableMemtableFlushPeriod.Milliseconds(), 10))

		if err := session.Query(q).Consistency(gocql.All).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("alter table %s: %w", tableName, err)
		}
	}

	return nil
}

// memtableFlushPeriodIsSet reports whether memtable_flush_period_in_ms is already explicitly set
// on the given table. When it is, we skip the ALTER TABLE so that a DBA can manage this value freely.
// In Cassandra 3+, memtable_flush_period_in_ms is a direct column in system_schema.tables;
// its default value is 0 (disabled).
func memtableFlushPeriodIsSet(
	ctx context.Context,
	session *connection.SessionWrapper,
	keyspace, tableName string,
) (bool, error) {
	var flushPeriodMs int

	err := session.Query(
		`SELECT memtable_flush_period_in_ms FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`,
		keyspace, tableName,
	).WithContext(ctx).Scan(&flushPeriodMs)
	if err != nil {
		return false, fmt.Errorf("read table options for %s: %w", tableName, err)
	}

	return flushPeriodMs != 0, nil
}

// AssociatedNames return the associated names map[mutable label name] -> non mutable label name.
func (cp *cassandra) AssociatedNames(ctx context.Context, tenant string) (map[string]string, error) {
	session, err := cp.options.Connection.SessionReadOnly()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	iter := session.Query(`
		SELECT name, associated_name FROM mutable_label_names
		WHERE tenant = ?`,
		tenant,
	).WithContext(ctx).Iter()

	var (
		associatedNames = make(map[string]string)
		name            string
		associatedName  string
	)

	for iter.Scan(&name, &associatedName) {
		associatedNames[name] = associatedName
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select labels: %w", err)
	}

	return associatedNames, nil
}

// AssociatedValues returns the associated values for a mutable label name and tenant.
func (cp *cassandra) AssociatedValues(ctx context.Context, tenant, name string) (map[string][]string, error) {
	session, err := cp.options.Connection.SessionReadOnly()
	if err != nil {
		return nil, err
	}

	defer session.Close()

	iter := session.Query(`
		SELECT value, associated_values FROM mutable_label_values
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	).WithContext(ctx).Iter()

	var (
		values           = make(map[string][]string)
		value            string
		associatedValues []string
	)

	for iter.Scan(&value, &associatedValues) {
		values[value] = associatedValues
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select values: %w", err)
	}

	return values, nil
}

// SetAssociatedValues sets the non mutable label values associated to a mutable label.
func (cp *cassandra) SetAssociatedValues(ctx context.Context, label mutable.LabelWithValues) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		INSERT INTO mutable_label_values (tenant, name, value, associated_values)
		VALUES (?, ?, ?, ?)`,
		label.Tenant, label.Name, label.Value, label.AssociatedValues,
	).WithContext(ctx)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated values: %w", err)
	}

	return nil
}

// DeleteAssociatedValues deletes the non mutable label values associated to a mutable label.
func (cp *cassandra) DeleteAssociatedValues(ctx context.Context, label mutable.Label) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		DELETE FROM mutable_label_values
		WHERE tenant = ? AND name = ? AND value = ?`,
		label.Tenant, label.Name, label.Value,
	).WithContext(ctx)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("delete associated values: %w", err)
	}

	return nil
}

// SetAssociatedName sets the non mutable label name associated to a mutable label name.
func (cp *cassandra) SetAssociatedName(ctx context.Context, label mutable.LabelWithName) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		INSERT INTO mutable_label_names (tenant, name, associated_name)
		VALUES (?, ?, ?)`,
		label.Tenant, label.Name, label.AssociatedName,
	).WithContext(ctx)

	if err := query.Exec(); err != nil {
		return fmt.Errorf("insert associated name: %w", err)
	}

	return nil
}

// DeleteAssociatedName deletes a mutable label name.
func (cp *cassandra) DeleteAssociatedName(ctx context.Context, tenant, name string) error {
	session, err := cp.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	queryDeleteLabelNames := session.Query(`
		DELETE FROM mutable_label_names
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	).WithContext(ctx)

	if err := queryDeleteLabelNames.Exec(); err != nil {
		return fmt.Errorf("delete label name from mutable_label_names: %w", err)
	}

	queryDeleteLabelValues := session.Query(`
		DELETE FROM squirreldb.mutable_label_values
		WHERE tenant = ? AND name = ?`,
		tenant, name,
	).WithContext(ctx)

	if err := queryDeleteLabelValues.Exec(); err != nil {
		return fmt.Errorf("delete label name from mutable_label_values: %w", err)
	}

	return nil
}

// AllMutableLabels return the list of all known mutable labels from all tenants.
// Currently this function is not optimized (it does a Cassandra full scan) and should be avoided if possible.
func (cp *cassandra) AllMutableLabels(ctx context.Context) ([]mutable.LabelWithName, error) {
	session, err := cp.options.Connection.SessionReadOnly()
	if err != nil {
		return nil, err
	}

	result := make([]mutable.LabelWithName, 0)
	iter := session.Query("SELECT tenant, name, associated_name FROM mutable_label_names").WithContext(ctx).Iter()

	var currentLabel mutable.LabelWithName

	for iter.Scan(&currentLabel.Tenant, &currentLabel.Name, &currentLabel.AssociatedName) {
		result = append(result, currentLabel)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select tenants: %w", err)
	}

	return result, ctx.Err()
}
