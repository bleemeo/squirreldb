package index

import (
	gouuid "github.com/gofrs/uuid"
	"reflect"
	"squirreldb/types"
	"testing"
)

type mockIndexerTable struct {
}

func (m *mockIndexerTable) Request() (map[types.MetricUUID]types.MetricLabels, error) {
	return nil, nil
}

func (m *mockIndexerTable) Save(uuid types.MetricUUID, labels types.MetricLabels) error {
	return nil
}

func TestNew(t *testing.T) {
	type args struct {
		storage *mockIndexerTable
	}
	tests := []struct {
		name string
		args args
		want *Index
	}{
		{
			name: "new",
			args: args{
				storage: &mockIndexerTable{},
			},
			want: &Index{
				storage: &mockIndexerTable{},
				pairs:   nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.args.storage); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIndex_UUID(t *testing.T) {
	type fields struct {
		storage mockIndexerTable
		pairs   map[types.MetricUUID]types.MetricLabels
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
				storage: mockIndexerTable{},
				pairs:   make(map[types.MetricUUID]types.MetricLabels),
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
				UUID: gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Index{
				storage: &tt.fields.storage,
				pairs:   tt.fields.pairs,
			}
			if got := m.UUID(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UUID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_labelsToUUID(t *testing.T) {
	type args struct {
		labels types.MetricLabels
	}
	tests := []struct {
		name string
		args args
		want types.MetricUUID
	}{
		{
			name: "valid_uuid",
			args: args{
				labels: types.MetricLabels{
					{
						Name:  "__bleemeo_uuid__",
						Value: "abcdef01-2345-6789-abcd-ef0123456789",
					},
				},
			},
			want: types.MetricUUID{
				UUID: gouuid.FromStringOrNil("abcdef01-2345-6789-abcd-ef0123456789"),
			},
		},
		{
			name: "invalid_uuid",
			args: args{
				labels: types.MetricLabels{
					{
						Name:  "__bleemeo_uuid__",
						Value: "i-am-an-invalid-uuid",
					},
				},
			},
			want: types.MetricUUID{
				UUID: gouuid.FromStringOrNil("00000000-0000-0000-0000-000000000000"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := labelsToUUID(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("labelsToUUID() = %v, want %v", got, tt.want)
			}
		})
	}
}
