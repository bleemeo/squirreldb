package prometheus

import (
	"hamsterdb/types"
	"log"
	"net/http"
)

type Prometheus struct {
	readPoints  ReadPoints
	writePoints WritePoints
}

func NewPrometheus(reader types.Reader, writer types.Writer) *Prometheus {
	readPoints := ReadPoints{reader: reader}
	writePoints := WritePoints{writer: writer}

	return &Prometheus{readPoints: readPoints, writePoints: writePoints}
}

func (prometheus *Prometheus) InitServer() {
	// Register read and write handlers
	http.HandleFunc("/read", prometheus.readPoints.ServeHTTP)
	http.HandleFunc("/write", prometheus.writePoints.ServeHTTP)

	// Listen and serve
	log.Fatalf("[prometheus] InitServer: Can't listen and serve (%v)",
		http.ListenAndServe(":1234", nil))
}
