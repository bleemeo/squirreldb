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

package mutable

import (
	"context"
	"time"

	"github.com/bleemeo/squirreldb/types"
)

type notImplementedVerifier struct{}

func (v notImplementedVerifier) WithNow(_ time.Time) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithDoFix(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithLock(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithStrictExpiration(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithStrictMetricCreation(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithPedanticExpiration(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) Verify(_ context.Context) (hadIssue bool, err error) {
	return false, errNotImplemented
}
