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

import (
	"testing"
)

func TestMinInt64(t *testing.T) {
	type args struct {
		x int64
		y int64
	}

	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "x_min",
			args: args{
				x: 5,
				y: 10,
			},
			want: 5,
		},
		{
			name: "y_min",
			args: args{
				x: 20,
				y: 10,
			},
			want: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MinInt64(tt.args.x, tt.args.y); got != tt.want {
				t.Errorf("Int64Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMaxInt64(t *testing.T) {
	type args struct {
		x int64
		y int64
	}

	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "x_max",
			args: args{
				x: 10,
				y: 5,
			},
			want: 10,
		},
		{
			name: "y_max",
			args: args{
				x: 10,
				y: 20,
			},
			want: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MaxInt64(tt.args.x, tt.args.y); got != tt.want {
				t.Errorf("Int64Max() = %v, want %v", got, tt.want)
			}
		})
	}
}
