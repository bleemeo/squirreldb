package prometheus

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"hamsterdb/types"
	"io/ioutil"
	"net/http"
)

type ReadPoints struct {
	reader types.MetricReader
}

func (r *ReadPoints) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		logger.Printf("ReadPoints: Error: Can't read the body (%v)", err)
		http.Error(writer, "Can't read the HTTP body", http.StatusBadRequest)
		return
	}

	decoded, err := snappy.Decode(nil, body)

	if err != nil {
		logger.Printf("ReadPoints: Error: Can't decode the body (%v)", err)
		http.Error(writer, "Can't decode the HTTP body", http.StatusBadRequest)
		return
	}

	var readRequest prompb.ReadRequest

	if err := proto.Unmarshal(decoded, &readRequest); err != nil {
		logger.Printf("ReadPoints: Error: Can't unmarshal the decoded body (%v)", err)
		http.Error(writer, "Can't unmarshal the decoded body", http.StatusBadRequest)
		return
	}
}
