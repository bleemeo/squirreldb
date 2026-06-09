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
	"testing"

	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/tests/testcases"
	"github.com/bleemeo/squirreldb/types"
)

// Ensure MemoryTSDB implement TSDB.
var _ types.TSDB = &MemoryTSDB{}

func TestTSDB(t *testing.T) {
	for _, tt := range testcases.GetReadTestCase() {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()

			tsdb := new(MemoryTSDB)
			tsdb.AddData(tt.StoreData, 86400)

			tt.RunTest(t, tsdb)
		})
	}
}

func TestTSDBAppender(t *testing.T) {
	for _, tt := range testcases.GetReadWriteTestCases() {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()

			tt.RunTest(t, convert.NewTSDBToPromStore(new(MemoryTSDB)))
		})
	}
}
