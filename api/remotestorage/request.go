package remotestorage

import (
	"bytes"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
)

// Returns the encoded response.
func encodeResponse(pb proto.Marshaler) ([]byte, error) {
	marshal, err := pb.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal response: %w", err)
	}

	encodedBody := snappy.Encode(nil, marshal)

	return encodedBody, nil
}

type requestContext struct {
	buffer        bytes.Buffer
	decodedBuffer []byte
	pb            proto.Message
}

// Decodes the request.
func decodeRequest(reader io.Reader, reqCtx *requestContext) error {
	reqCtx.buffer.Reset()

	_, err := reqCtx.buffer.ReadFrom(reader)
	if err != nil {
		return fmt.Errorf("read buffer: %w", err)
	}

	reqCtx.decodedBuffer, err = snappy.Decode(reqCtx.decodedBuffer, reqCtx.buffer.Bytes())

	if err != nil {
		return fmt.Errorf("failed to decode snappy: %w", err)
	}

	if err := proto.Unmarshal(reqCtx.decodedBuffer, reqCtx.pb); err != nil {
		return fmt.Errorf("failed to unmarshal: %w", err)
	}

	return nil
}
