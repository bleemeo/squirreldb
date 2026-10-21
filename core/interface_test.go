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

package core

import (
	"github.com/bleemeo/squirreldb/api"
	"github.com/bleemeo/squirreldb/export"
	"github.com/bleemeo/squirreldb/types"

	cassandrabackend "github.com/bleemeo/squirreldb/cassandra/backend"
	clickhousebackend "github.com/bleemeo/squirreldb/clickhouse/backend"
	dummybackend "github.com/bleemeo/squirreldb/dummy/backend"
	sandboxbackend "github.com/bleemeo/squirreldb/sandbox/backend"
)

// Compile-time checks that each backend exposes, on the (never-wrapped) Backend
// itself, the extra capabilities the API and the migration reach by assertion.
// ClickHouse has no index / forced pre-aggregation, so it is not a TSDBDebugEndpoint.
var (
	_ api.TSDBDebugEndpoint   = (*cassandrabackend.Backend)(nil)
	_ types.HTTPOptionsParser = (*cassandrabackend.Backend)(nil)
	_ types.HTTPOptionsParser = (*clickhousebackend.Backend)(nil)
	_ export.RawExporter      = (*cassandrabackend.Backend)(nil)
	_ export.RawImporter      = (*clickhousebackend.Backend)(nil)
	_ Backend                 = (*cassandrabackend.Backend)(nil)
	_ Backend                 = (*clickhousebackend.Backend)(nil)
	_ Backend                 = (*dummybackend.Backend)(nil)
	_ Backend                 = (*sandboxbackend.Backend)(nil)
)
