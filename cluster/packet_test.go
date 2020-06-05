package cluster

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func Test_encode_decode(t *testing.T) {

	largeAddress := "dns-"
	for n := 0; n < 200; n++ {
		largeAddress += fmt.Sprintf("loop-%d-", n)
		largeAddress += "01234456789."
	}
	largeAddress += ":65535"

	rnd := rand.New(rand.NewSource(42))
	largePayload := make([]byte, 1<<24) // 16 MB

	_, err := rnd.Read(largePayload)
	if err != nil {
		t.Error(err)
		return
	}

	tests := []struct {
		name   string
		packet rpcPacket
	}{
		{
			name: "request",
			packet: rpcPacket{
				PacketType:     msgRequest,
				RequestID:      1234,
				ReplyToAddress: "10.42.13.37:8080",
				RequestType:    9,
				Payload:        []byte("some payload value"),
			},
		},
		{
			name: "response",
			packet: rpcPacket{
				PacketType: msgReply,
				RequestID:  1234,
				Payload:    []byte("some response payload"),
			},
		},
		{
			name: "errors",
			packet: rpcPacket{
				PacketType: msgError,
				RequestID:  1234,
				Payload:    []byte("test error"),
			},
		},
		{
			name: "zero",
			packet: rpcPacket{
				PacketType:     msgRequest,
				RequestID:      0,
				ReplyToAddress: "dns-name:http",
				RequestType:    0,
				Payload:        []byte(""),
			},
		},
		{
			name: "large",
			packet: rpcPacket{
				PacketType:     msgRequest,
				RequestID:      1e6,
				ReplyToAddress: largeAddress,
				RequestType:    255,
				Payload:        largePayload,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := tt.packet.Encode()
			got, err := decode(buffer)
			if err != nil {
				t.Errorf("decode() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.packet) {
				t.Errorf("decode() = %v, want %v", got, tt.packet)
			}
		})
	}
}
