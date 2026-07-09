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

package types_test

import (
	"reflect"
	"testing"

	"github.com/bleemeo/squirreldb/types"
)

func TestStateValueRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("float64", func(t *testing.T) {
		t.Parallel()

		s, err := types.EncodeStateValue(3.5)
		if err != nil {
			t.Fatal(err)
		}

		var got float64
		if err := types.DecodeStateValue(s, &got); err != nil {
			t.Fatal(err)
		}

		if got != 3.5 {
			t.Fatalf("got %v, want 3.5", got)
		}
	})

	t.Run("int", func(t *testing.T) {
		t.Parallel()

		s, _ := types.EncodeStateValue(42)

		var got int
		if err := types.DecodeStateValue(s, &got); err != nil {
			t.Fatal(err)
		}

		if got != 42 {
			t.Fatalf("got %v, want 42", got)
		}
	})

	t.Run("int64", func(t *testing.T) {
		t.Parallel()

		s, _ := types.EncodeStateValue(int64(9000000000))

		var got int64
		if err := types.DecodeStateValue(s, &got); err != nil {
			t.Fatal(err)
		}

		if got != 9000000000 {
			t.Fatalf("got %v, want 9000000000", got)
		}
	})

	t.Run("string", func(t *testing.T) {
		t.Parallel()

		s, _ := types.EncodeStateValue("hello")

		var got string
		if err := types.DecodeStateValue(s, &got); err != nil {
			t.Fatal(err)
		}

		if got != "hello" {
			t.Fatalf("got %q, want hello", got)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		t.Parallel()

		s, _ := types.EncodeStateValue([]byte("raw bytes"))

		var got []byte
		if err := types.DecodeStateValue(s, &got); err != nil {
			t.Fatal(err)
		}

		if string(got) != "raw bytes" {
			t.Fatalf("got %q, want raw bytes", got)
		}
	})

	t.Run("struct via json", func(t *testing.T) {
		t.Parallel()

		type payload struct {
			A int
			B string
		}

		want := payload{A: 1, B: "two"}

		s, err := types.EncodeStateValue(want)
		if err != nil {
			t.Fatal(err)
		}

		var got payload
		if err := types.DecodeStateValue(s, &got); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
}
