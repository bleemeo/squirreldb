package remotestorage

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"io/ioutil"
	"net/http"
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
func decodeRequest(request *http.Request, pb proto.Message) error {
	body, err := ioutil.ReadAll(request.Body)

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
