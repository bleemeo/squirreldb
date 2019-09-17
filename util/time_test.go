package util

import (
	"testing"
	"time"
)

func TestTimeBetween(t *testing.T) {
	type args struct {
		time time.Time
		from time.Time
		to   time.Time
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{ // <-- time_between
			name: "time_between",
			args: args{
				time: time.Unix(50, 0),
				from: time.Unix(0, 0),
				to:   time.Unix(100, 0),
			},
			want: true,
		}, // <-- time_between
		{ // <-- time_equal_from
			name: "time_equal_from",
			args: args{
				time: time.Unix(0, 0),
				from: time.Unix(0, 0),
				to:   time.Unix(100, 0),
			},
			want: true,
		}, // <-- time_equal_from
		{ // <-- time_equal_to
			name: "time_equal_to",
			args: args{
				time: time.Unix(100, 0),
				from: time.Unix(0, 0),
				to:   time.Unix(100, 0),
			},
			want: true,
		}, // <-- time_equal_to
		{ // <-- time_outside_from
			name: "time_outside_from",
			args: args{
				time: time.Unix(0, 0),
				from: time.Unix(25, 0),
				to:   time.Unix(75, 0),
			},
			want: false,
		}, // <-- time_outside_from
		{ // <-- time_outside_to
			name: "time_outside_to",
			args: args{
				time: time.Unix(100, 0),
				from: time.Unix(25, 0),
				to:   time.Unix(75, 0),
			},
			want: false,
		}, // <-- time_outside_to
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TimeBetween(tt.args.time, tt.args.from, tt.args.to); got != tt.want {
				t.Errorf("TimeBetween() = %v, want %v", got, tt.want)
			}
		})
	}
}
