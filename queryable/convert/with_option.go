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

package convert

import (
	"context"

	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/storage"
)

// NewTSDBToPromStore convert our TSDB interface (Appender/Querier)WithOptions to just Appender/Querier.
// Useful to run test that just need plain Prometheus storage and use default options on our TSDB interface.
func NewTSDBToPromStore(db types.TSDB) convertedTSDB {
	return NewTSDBWithFixedOption(db, types.QuerierOptions{}, types.AppenderOptions{})
}

// NewTSDBWithFixedOption convert our TSDB interface (Appender/Querier)WithOptions to just Appender/Querier.
// Unlike NewTSDBToPromStore which provide it with not option, here fixed options will be used.
func NewTSDBWithFixedOption(
	db types.TSDB,
	readOptions types.QuerierOptions,
	writeOptions types.AppenderOptions,
) convertedTSDB {
	return convertedTSDB{
		inner:        db,
		readOptions:  readOptions,
		writeOptions: writeOptions,
	}
}

type convertedTSDB struct {
	inner        types.TSDB
	readOptions  types.QuerierOptions
	writeOptions types.AppenderOptions
}

func (db convertedTSDB) Appender(ctx context.Context) storage.Appender {
	return db.inner.AppenderWithOptions(ctx, db.writeOptions)
}

func (db convertedTSDB) Querier(mint int64, maxt int64) (storage.Querier, error) {
	return db.inner.QuerierWithOptions(db.readOptions, mint, maxt)
}

func (db convertedTSDB) ChunkQuerier(mint int64, maxt int64) (storage.ChunkQuerier, error) {
	return db.inner.ChunkQuerierWithOptions(db.readOptions, mint, maxt)
}
