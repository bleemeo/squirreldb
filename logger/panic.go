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

package logger

import (
	"github.com/rs/zerolog/log"
)

// ProcessPanic logs panics to Sentry.
// It should be defered at the beginning of every new goroutines.
func ProcessPanic() {
	if res := recover(); res != nil {
		// If the panic was caused by an error, add it with the "error" field,
		// this makes the logger send a stacktrace to Sentry.
		if err, ok := res.(error); ok {
			log.Panic().Err(err).Msg("Panic")
		}

		log.Panic().Msgf("%s", res)
	}
}
