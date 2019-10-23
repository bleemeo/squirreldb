package index

import (
	"github.com/gofrs/uuid"
	"reflect"
	"squirreldb/types"
	"testing"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		want *Index
	}{
		{
			name: "new",
			want: &Index{
				Pairs: make(map[types.MetricUUID]types.MetricLabels),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIndex_UUID(t *testing.T) {
	type fields struct {
		Matchers map[types.MetricUUID]types.MetricLabels
	}
	type args struct {
		labels types.MetricLabels
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   types.MetricUUID
	}{
		{
			name: "uuid",
			fields: fields{
				Matchers: make(map[types.MetricUUID]types.MetricLabels),
			},
			args: args{
				labels: []types.MetricLabel{
					{
						Name:  "__bleemeo_uuid__",
						Value: "00000000-0000-0000-0000-000000000001",
					},
				},
			},
			want: types.MetricUUID{
				UUID: uuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Index{
				Pairs: tt.fields.Matchers,
			}
			if got := m.UUID(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UUID() = %v, want %v", got, tt.want)
			}
		})
	}
}
