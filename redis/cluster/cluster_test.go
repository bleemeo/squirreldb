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

package cluster

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func Test_decode(t *testing.T) {
	blob := make([]byte, 66000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(blob)

	tests := []struct {
		name    string
		topic   string
		message []byte
	}{
		{
			name:    "simple",
			topic:   "index:new-metric",
			message: []byte("test"),
		},
		{
			name:    "utf-topic",
			topic:   "emoji:😇",
			message: []byte("test"),
		},
		{
			name:    "zero",
			topic:   "",
			message: []byte(""),
		},
		{
			name:    "large",
			topic:   "this-topic-is-rather-long. Still less than 255 because we can't encode more",
			message: []byte("This bytes will include... bytes\x00\x01\x02"),
		},
		{
			name:    "huge",
			topic:   "big-binary",
			message: blob,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := encode(tt.topic, tt.message)
			if err != nil {
				t.Errorf("encode() error = %v", err)
			}

			gotTopic, gotMessage, err := decode(serialized)
			if err != nil {
				t.Errorf("decode() error = %v", err)
			}

			if gotTopic != tt.topic {
				t.Errorf("topic = %v, want %v", gotTopic, tt.topic)
			}

			if !bytes.Equal(gotMessage, tt.message) {
				if len(tt.message) > 100 {
					tt.message = append(tt.message[:97], []byte("...")...)
				}

				if len(gotMessage) > 100 {
					gotMessage = append(gotMessage[:97], []byte("...")...)
				}

				t.Errorf("message = %v, want %v", gotMessage, tt.message)
			}
		})
	}
}
