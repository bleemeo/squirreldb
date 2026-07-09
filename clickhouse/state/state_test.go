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

package state

import (
	"strings"
	"testing"
)

func TestSchemaObjects(t *testing.T) {
	t.Parallel()

	t.Run("single node", func(t *testing.T) {
		t.Parallel()

		objects := SchemaObjects("")
		if len(objects) != 1 {
			t.Fatalf("got %d objects, want 1", len(objects))
		}

		obj := objects[0]
		if obj.Name != "state" {
			t.Errorf("object name = %q, want state", obj.Name)
		}

		for _, want := range []string{"ReplacingMergeTree", "ORDER BY (name)", "version"} {
			if !strings.Contains(obj.DDL, want) {
				t.Errorf("DDL missing %q:\n%s", want, obj.DDL)
			}
		}

		if strings.Contains(obj.DDL, "ON CLUSTER") {
			t.Errorf("single-node DDL should not mention ON CLUSTER:\n%s", obj.DDL)
		}
	})

	t.Run("cluster", func(t *testing.T) {
		t.Parallel()

		objects := SchemaObjects("my_cluster")
		if len(objects) != 1 {
			t.Fatalf("got %d objects, want 1", len(objects))
		}

		obj := objects[0]

		for _, want := range []string{"ON CLUSTER my_cluster", "ReplicatedReplacingMergeTree"} {
			if !strings.Contains(obj.DDL, want) {
				t.Errorf("cluster DDL missing %q:\n%s", want, obj.DDL)
			}
		}
	})
}
