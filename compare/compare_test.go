package compare

import (
	"squirreldb/types"
	"testing"
)

func TestInt64Min(t *testing.T) {
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
			if got := Int64Min(tt.args.x, tt.args.y); got != tt.want {
				t.Errorf("Int64Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInt64Max(t *testing.T) {
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
			if got := Int64Max(tt.args.x, tt.args.y); got != tt.want {
				t.Errorf("Int64Max() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLabelsEqual(t *testing.T) {
	type args struct {
		x types.MetricLabels
		y types.MetricLabels
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "x_y_equal",
			args: args{
				x: []types.MetricLabel{
					{
						Name:  "__name__",
						Value: "testing",
						Type:  0,
					},
					{
						Name:  "job",
						Value: "testing",
						Type:  0,
					},
				},
				y: []types.MetricLabel{
					{
						Name:  "__name__",
						Value: "testing",
						Type:  0,
					},
					{
						Name:  "job",
						Value: "testing",
						Type:  0,
					},
				},
			},
			want: true,
		},
		{
			name: "x_y_same_len_not_equal",
			args: args{
				x: []types.MetricLabel{
					{
						Name:  "__name__",
						Value: "testing",
						Type:  0,
					},
					{
						Name:  "job",
						Value: "testing",
						Type:  0,
					},
				},
				y: []types.MetricLabel{
					{
						Name:  "__name__",
						Value: "testing",
						Type:  0,
					},
					{
						Name:  "monitor",
						Value: "testing",
						Type:  0,
					},
				},
			},
			want: false,
		},
		{
			name: "x_y_not_same_len_not_equal",
			args: args{
				x: []types.MetricLabel{
					{
						Name:  "__name__",
						Value: "testing",
						Type:  0,
					},
					{
						Name:  "job",
						Value: "testing",
						Type:  0,
					},
				},
				y: []types.MetricLabel{
					{
						Name:  "__name__",
						Value: "testing",
						Type:  0,
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LabelsEqual(tt.args.x, tt.args.y); got != tt.want {
				t.Errorf("LabelsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
