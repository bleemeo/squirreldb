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

package dummy

import (
	"context"
	"sync"

	"github.com/bleemeo/squirreldb/types"
)

// States is a in-memory single-node "states", that is a map :)
// Only useful for tests.
type States struct {
	values map[string]string
	mutex  sync.Mutex
}

func (s *States) Read(_ context.Context, name string, value any) (bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// We mimick behavior of cassandra/states
	valueString, ok := s.values[name]
	if !ok {
		return false, nil
	}

	if err := types.DecodeStateValue(valueString, value); err != nil {
		return false, err
	}

	return true, nil
}

func (s *States) Write(_ context.Context, name string, value any) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.values == nil {
		s.values = make(map[string]string)
	}

	valueString, err := types.EncodeStateValue(value)
	if err != nil {
		return err
	}

	s.values[name] = valueString

	return nil
}
