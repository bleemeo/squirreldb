package remotestorage

import (
	"io"
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
)

// Returns the encoded response
func encodeResponse(pb proto.Marshaler) ([]byte, error) {
	marshal, err := pb.Marshal()

	if err != nil {
		return nil, err
	}

	encodedBody := snappy.Encode(nil, marshal)

	return encodedBody, nil
}

// Decodes the request
func decodeRequest(reader io.Reader, pb proto.Message) error {
	body, err := ioutil.ReadAll(reader)

	if err != nil {
		return err
	}

	decodedBody, err := snappy.Decode(nil, body)

	if err != nil {
		return err
	}

	if err := proto.Unmarshal(decodedBody, pb); err != nil {
		return err
	}

	return nil
}
