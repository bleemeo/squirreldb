package config

import (
	"errors"
	"reflect"
	"testing"

	"github.com/knadh/koanf"
)

type fakeProvider struct {
	Values map[string]interface{}
}

func (p fakeProvider) Read() (map[string]interface{}, error) {
	return p.Values, nil
}
func (p fakeProvider) ReadBytes() ([]byte, error) {
	return nil, errors.New("env provider does not support this method")
}
func (p fakeProvider) Watch(cb func(event interface{}, err error)) error {
	return errors.New("env provider does not support this method")
}

func Test_listEnvProvider_Read(t *testing.T) {
	tests := []struct {
		name     string
		provider koanf.Provider
		want     map[string]interface{}
		wantErr  bool
	}{
		{
			name: "empty",
			provider: fakeProvider{
				Values: map[string]interface{}{},
			},
			want: map[string]interface{}{},
		},
		{
			name: "cassandra-addresses",
			provider: fakeProvider{
				Values: map[string]interface{}{
					"cassandra": map[string]interface{}{
						"addresses": "localhost,remotehost",
					},
				},
			},
			want: map[string]interface{}{
				"cassandra": map[string]interface{}{
					"addresses": []string{"localhost", "remotehost"},
				},
			},
		},
		{
			name: "non-list",
			provider: fakeProvider{
				Values: map[string]interface{}{
					"remote_storage": map[string]interface{}{
						"listen_address": "localhost:9201",
					},
				},
			},
			want: map[string]interface{}{
				"remote_storage": map[string]interface{}{
					"listen_address": "localhost:9201",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep := listEnvProvider{
				Provider: tt.provider,
			}
			got, err := ep.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("listEnvProvider.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("listEnvProvider.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}
