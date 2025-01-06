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

package dummy

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
)

// States is a in-memory single-node "states", that is a map :)
// Only useful for tests.
type States struct {
	values map[string]string
	mutex  sync.Mutex
}

func (s *States) Read(_ context.Context, name string, value interface{}) (bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// We mimick behavior of cassandra/states
	valueString, ok := s.values[name]
	if !ok {
		return false, nil
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

func (s *States) Write(_ context.Context, name string, value interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.values == nil {
		s.values = make(map[string]string)
	}

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

	s.values[name] = valueString

	return nil
}
