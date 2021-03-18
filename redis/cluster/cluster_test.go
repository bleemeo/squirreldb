package cluster

import (
	"bytes"
	"math/rand"
	"testing"
)

func Test_decode(t *testing.T) {
	blob := make([]byte, 66000)
	rand.Read(blob)

	tests := []struct {
		name    string
		topic   string
		sender  []byte
		message []byte
	}{
		{
			name:    "simple",
			topic:   "index:new-metric",
			sender:  makePrefix(),
			message: []byte("test"),
		},
		{
			name:    "utf-topic",
			topic:   "emoji:😇",
			sender:  makePrefix(),
			message: []byte("test"),
		},
		{
			name:    "zero",
			topic:   "",
			sender:  makePrefix(),
			message: []byte(""),
		},
		{
			name:    "large",
			topic:   "this-topic-is-rather-long. Still less than 255 because we can't encode more",
			sender:  makePrefix(),
			message: []byte("This bytes will include... bytes\x00\x01\x02"),
		},
		{
			name:    "huge",
			topic:   "big-binary",
			sender:  makePrefix(),
			message: blob,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := encode(tt.topic, tt.sender, tt.message)
			if err != nil {
				t.Errorf("encode() error = %v", err)
			}

			gotTopic, gotSender, gotMessage, err := decode(serialized)

			if err != nil {
				t.Errorf("decode() error = %v", err)
			}

			if gotTopic != tt.topic {
				t.Errorf("topic = %v, want %v", gotTopic, tt.topic)
			}

			if !bytes.Equal(gotSender, tt.sender) {
				t.Errorf("sender = %v, want %v", gotSender, tt.sender)
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
