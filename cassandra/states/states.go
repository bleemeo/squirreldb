// Copyright 2015-2025 Bleemeo
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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/bleemeo/squirreldb/cassandra/connection"

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

	switch v := value.(type) {
	case *float64:
		valueFloat64, _ := strconv.ParseFloat(valueString, 64)
		*v = valueFloat64
	case *int:
		valueInt, _ := strconv.Atoi(valueString)
		*v = valueInt
	case *int64:
		valueInt64, _ := strconv.ParseInt(valueString, 10, 64)
		*v = valueInt64
	case *string:
		*v = valueString
	default:
		err := json.Unmarshal([]byte(valueString), value)
		if err != nil {
			return false, fmt.Errorf("failed to unmarshal value: %w", err)
		}

		return true, nil
	}

	return true, nil
}

// Write updates the state in the states table.
func (c *CassandraStates) Write(ctx context.Context, name string, value any) error {
	var valueString string

	switch value.(type) {
	case float64, int, int64, string:
		valueString = fmt.Sprint(value)
	default:
		marshalled, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}

		valueString = string(marshalled)
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
