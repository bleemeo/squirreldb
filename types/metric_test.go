package types

import (
	"reflect"
	"testing"
	"time"
)

func TestMetric_CanonicalLabels(t *testing.T) {
	type fields struct {
		Labels map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{ // <-- label_names_ordered
			name: "label_names_ordered",
			fields: fields{Labels: map[string]string{
				"__name__": "metric_name",
				"monitor":  "monitor",
			},
			},
			want: `__name__="metric_name",monitor="monitor"`,
		}, // <-- label_names_ordered
		{ // <-- label_names_unordered
			name: "label_names_unordered",
			fields: fields{
				Labels: map[string]string{
					"monitor":  "monitor",
					"__name__": "metric_name",
				},
			},
			want: `__name__="metric_name",monitor="monitor"`,
		}, // <-- label_names_unordered
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := &Metric{
				Labels: tt.fields.Labels,
			}
			if got := metric.CanonicalLabels(); got != tt.want {
				t.Errorf("CanonicalLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetricPoints_AddPoints(t *testing.T) {
	type fields struct {
		Metric Metric
		Points []Point
	}
	type args struct {
		points []Point
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   MetricPoints
	}{
		{
			name: "basic",
			fields: fields{
				Metric: Metric{Labels: map[string]string{
					"__name__": "test",
				}},
				Points: nil,
			},
			args: args{points: []Point{
				{
					Time:  time.Unix(0, 0),
					Value: 0,
				},
				{
					Time:  time.Unix(50, 0),
					Value: 50,
				},
				{
					Time:  time.Unix(100, 0),
					Value: 100,
				},
			}},
			want: MetricPoints{
				Metric: Metric{Labels: map[string]string{
					"__name__": "test",
				}},
				Points: []Point{
					{
						Time:  time.Unix(0, 0),
						Value: 0,
					},
					{
						Time:  time.Unix(50, 0),
						Value: 50,
					},
					{
						Time:  time.Unix(100, 0),
						Value: 100,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mPoints := &MetricPoints{
				Metric: tt.fields.Metric,
				Points: tt.fields.Points,
			}
			mPoints.AddPoints(tt.args.points)
			if !reflect.DeepEqual(*mPoints, tt.want) {
				t.Errorf("AddPoints() mPoints = %v, want %v", *mPoints, tt.want)
			}
		})
	}
}

func TestMetricPoints_RemovePoints(t *testing.T) {
	type fields struct {
		Metric Metric
		Points []Point
	}
	type args struct {
		pointsTime []time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   MetricPoints
	}{
		{
			name: "basic",
			fields: fields{
				Metric: Metric{Labels: map[string]string{
					"__name__": "test",
				}},
				Points: []Point{
					{
						Time:  time.Unix(0, 0),
						Value: 0,
					},
					{
						Time:  time.Unix(50, 0),
						Value: 50,
					},
					{
						Time:  time.Unix(100, 0),
						Value: 100,
					},
				},
			},
			args: args{pointsTime: []time.Time{
				time.Unix(50, 0),
			}},
			want: MetricPoints{
				Metric: Metric{Labels: map[string]string{
					"__name__": "test",
				}},
				Points: []Point{
					{
						Time:  time.Unix(0, 0),
						Value: 0,
					},
					{
						Time:  time.Unix(100, 0),
						Value: 100,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mPoints := &MetricPoints{
				Metric: tt.fields.Metric,
				Points: tt.fields.Points,
			}
			mPoints.RemovePoints(tt.args.pointsTime)
			if !reflect.DeepEqual(*mPoints, tt.want) {
				t.Errorf("RemovePoints() mPoints = %v, want %v", *mPoints, tt.want)
			}
		})
	}
}
