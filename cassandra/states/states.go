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

package states

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/types"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

type Options struct {
	Connection *connection.Connection
	Lock       sync.Locker
	ReadOnly   bool
	Logger     zerolog.Logger
}

type CassandraStates struct {
	options Options
}

// New creates a new CassandraStates object.
func New(ctx context.Context, options Options) (*CassandraStates, error) {
	if options.ReadOnly {
		options.Logger.Debug().Msg("Read-only mode is activated. Not trying to create tables and assuming they exist")
	} else {
		options.Lock.Lock()
		defer options.Lock.Unlock()

		if err := statesTableCreate(ctx, options.Connection); err != nil {
			return nil, fmt.Errorf("create tables: %w", err)
		}
	}

	states := &CassandraStates{
		options: options,
	}

	return states, nil
}

// Read reads value of the state from the states table.
func (c *CassandraStates) Read(ctx context.Context, name string, value any) (found bool, err error) {
	valueString, err := c.statesTableSelectState(ctx, name)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("read %s: %w", name, err)
	}

	if err := types.DecodeStateValue(valueString, value); err != nil {
		return false, err
	}

	return true, nil
}

// Write updates the state in the states table.
func (c *CassandraStates) Write(ctx context.Context, name string, value any) error {
	valueString, err := types.EncodeStateValue(value)
	if err != nil {
		return err
	}

	if err := c.statesTableInsertState(ctx, name, valueString); err != nil {
		return fmt.Errorf("update Cassandra: %w", err)
	}

	return nil
}

// Returns states table insert state Query.
func (c *CassandraStates) statesTableInsertState(ctx context.Context, name string, value string) error {
	session, err := c.options.Connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		INSERT INTO states (name, value)
		VALUES (?, ?)`,
		name, value,
	).WithContext(ctx)

	return query.Exec()
}

// Returns states table select state Query.
func (c *CassandraStates) statesTableSelectState(ctx context.Context, name string) (string, error) {
	var valueString string

	session, err := c.options.Connection.SessionReadOnly()
	if err != nil {
		return "", err
	}

	defer session.Close()

	query := session.Query(`
		SELECT value FROM states
		WHERE name = ?`,
		name,
	).WithContext(ctx)

	err = query.Scan(&valueString)

	return valueString, err
}

func statesTableCreate(ctx context.Context, connection *connection.Connection) error {
	session, err := connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		CREATE TABLE IF NOT EXISTS states (
			name text,
			value text,
			PRIMARY KEY (name)
		)
		WITH memtable_flush_period_in_ms = 300000`,
	).Consistency(gocql.All).WithContext(ctx)

	return query.Exec()
}
