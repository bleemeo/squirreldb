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

package compare

// MinInt64 returns the smallest number between x and y.
func MinInt64(x, y int64) int64 {
	if x < y {
		return x
	}

	return y
}

// MaxInt64 returns the largest number between x and y.
func MaxInt64(x, y int64) int64 {
	if x > y {
		return x
	}

	return y
}
