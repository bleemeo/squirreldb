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

package types

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// EncodeStateValue serializes a State value into the string a State store
// persists. It is the write half of the State value contract; DecodeStateValue
// is the read half. Both live here so every State implementation (Cassandra,
// ClickHouse, dummy) shares one encoding instead of copying the type switch.
func EncodeStateValue(value any) (string, error) {
	switch v := value.(type) {
	case float64, int, int64, string:
		return fmt.Sprint(v), nil
	case []byte:
		return string(v), nil
	default:
		marshalled, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal value: %w", err)
		}

		return string(marshalled), nil
	}
}

// DecodeStateValue populates value (which must be a pointer) from the string a
// State store returned. It is the read half of the State value contract (see
// EncodeStateValue). Parse errors for the numeric cases are ignored (the value
// is left at its zero), matching the historical behaviour.
func DecodeStateValue(valueString string, value any) error {
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
	case *[]byte:
		*v = []byte(valueString)
	default:
		if err := json.Unmarshal([]byte(valueString), value); err != nil {
			return fmt.Errorf("failed to unmarshal value: %w", err)
		}
	}

	return nil
}
