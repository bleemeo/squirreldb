// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"time"
	"unsafe"

	"github.com/bleemeo/squirreldb/types"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

const appProtoContentType = "application/x-protobuf"

// patchRemoteWriteHandler replaces the given api's remoteWriteHandler field
// by a fake one that makes the ServeHTTP method backdate the timestamps
// of the points that are in the future by the given backdateOffset.
// The purpose of this operation is to avoid the points that are produced by
// bleemeo-forecast getting blocked by Prometheus' 10-minute barrier.
func patchRemoteWriteHandler(api *v1.API, backdateOffset time.Duration) {
	writeHandlerField := reflect.ValueOf(api).Elem().FieldByName("remoteWriteHandler")
	ourWriteHandler := &fakeWriteHandler{
		futurePointsBackdateOffset: backdateOffset,
		// Unsafely casting the original write handler in our own type that mimics the original one,
		// to be able to access its private fields and methods.
		originalWriteHandler: (*writeHandler)(unsafe.Pointer(writeHandlerField.Elem().Pointer())),
	}

	// Making the API's remoteWriteHandler field writable
	writableWriteHandler := reflect.NewAt(writeHandlerField.Type(), unsafe.Pointer(writeHandlerField.UnsafeAddr())).Elem()
	// Setting our own write-handler in the API's remoteWriteHandler field
	writableWriteHandler.Set(reflect.ValueOf(ourWriteHandler))
}

// writeHandler is a copy of github.com/prometheus/prometheus/storage/remote.writeHandler.
// It must be updated each time it is modified on the Prometheus side.
type writeHandler struct {
	logger     *slog.Logger
	appendable storage.Appendable

	samplesWithInvalidLabelsTotal  prometheus.Counter
	samplesAppendedWithoutMetadata prometheus.Counter

	acceptedProtoMsgs map[config.RemoteWriteProtoMsg]struct{}
}

//go:linkname parseProtoMsg github.com/prometheus/prometheus/storage/remote.(*writeHandler).parseProtoMsg
func parseProtoMsg(h *writeHandler, contentType string) (config.RemoteWriteProtoMsg, error)

//go:linkname write github.com/prometheus/prometheus/storage/remote.(*writeHandler).write
func write(h *writeHandler, ctx context.Context, req *prompb.WriteRequest) error //nolint: revive

//go:linkname writeV2 github.com/prometheus/prometheus/storage/remote.(*writeHandler).writeV2
func writeV2(h *writeHandler, ctx context.Context, req *writev2.Request) (remote.WriteResponseStats, int, error) //nolint: revive,lll

type fakeWriteHandler struct {
	futurePointsBackdateOffset time.Duration
	originalWriteHandler       *writeHandler
}

// ServeHTTP is a copy of github.com/prometheus/prometheus/storage/remote.(*writeHandler).ServeHTTP,
// but slightly modified to be able to access private fields and methods,
// and mainly to backdate points that are more than 10 minutes in the future.
func (fwh *fakeWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		// Don't break yolo 1.0 clients if not needed. This is similar to what we did before 2.0.
		// We could give http.StatusUnsupportedMediaType, but let's assume 1.0 message by default.
		contentType = appProtoContentType
	}

	msgType, err := parseProtoMsg(fwh.originalWriteHandler, contentType)
	if err != nil {
		fwh.originalWriteHandler.logger.Error("Error decoding remote write request", "err", err)
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)

		return
	}

	if _, ok := fwh.originalWriteHandler.acceptedProtoMsgs[msgType]; !ok {
		err := fmt.Errorf("%v protobuf message is not accepted by this server; accepted %v", msgType, func() (ret []string) {
			for k := range fwh.originalWriteHandler.acceptedProtoMsgs {
				ret = append(ret, string(k))
			}

			return ret
		}())

		fwh.originalWriteHandler.logger.Error("Error decoding remote write request", "err", err)
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
	}

	enc := r.Header.Get("Content-Encoding")
	if enc == "" { //nolint: revive
		// Don't break yolo 1.0 clients if not needed. This is similar to what we did before 2.0.
		// We could give http.StatusUnsupportedMediaType, but let's assume snappy by default.
	} else if enc != string(remote.SnappyBlockCompression) {
		err := fmt.Errorf(
			"%v encoding (compression) is not accepted by this server; only %v is acceptable",
			enc, remote.SnappyBlockCompression,
		)

		fwh.originalWriteHandler.logger.Error("Error decoding remote write request", "err", err)
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
	}

	// Read the request body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fwh.originalWriteHandler.logger.Error("Error decoding remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	decompressed, err := snappy.Decode(nil, body)
	if err != nil {
		fwh.originalWriteHandler.logger.Error("Error decompressing remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	// Now we have a decompressed buffer we can unmarshal it.

	if msgType == config.RemoteWriteProtoMsgV1 {
		// PRW 1.0 flow has different proto message and no partial write handling.
		var req prompb.WriteRequest
		if err := proto.Unmarshal(decompressed, &req); err != nil {
			fwh.originalWriteHandler.logger.
				Error("Error decoding v1 remote write request", "protobuf_message", msgType, "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		ctx := fwh.backdatePointsV1(r.Context(), &req)
		if err = write(fwh.originalWriteHandler, ctx, &req); err != nil {
			switch {
			case errors.Is(err, storage.ErrOutOfOrderSample),
				errors.Is(err, storage.ErrOutOfBounds),
				errors.Is(err, storage.ErrDuplicateSampleForTimestamp),
				errors.Is(err, storage.ErrTooOldSample):
				// Indicated an out-of-order sample is a bad request to prevent retries.
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			default:
				fwh.originalWriteHandler.logger.
					Error("Error while remote writing the v1 request", "err", err.Error())
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}
		}

		w.WriteHeader(http.StatusNoContent)

		return
	}

	// Remote Write 2.x proto message handling.
	var req writev2.Request
	if err := proto.Unmarshal(decompressed, &req); err != nil {
		fwh.originalWriteHandler.logger.
			Error("Error decoding v2 remote write request", "protobuf_message", msgType, "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	ctx := fwh.backdatePointsV2(r.Context(), &req)
	respStats, errHTTPCode, err := writeV2(fwh.originalWriteHandler, ctx, &req)

	// Set required X-Prometheus-Remote-Write-Written-* response headers, in all cases.
	respStats.SetHeaders(w)

	if err != nil {
		if errHTTPCode/5 == 100 { // 5xx
			fwh.originalWriteHandler.logger.
				Error("Error while remote writing the v2 request", "err", err.Error())
		}

		http.Error(w, err.Error(), errHTTPCode)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (fwh *fakeWriteHandler) backdatePointsV1(ctx context.Context, req *prompb.WriteRequest) context.Context {
	return backdateSeries(ctx, req.Timeseries, fwh.futurePointsBackdateOffset.Milliseconds())
}

func (fwh *fakeWriteHandler) backdatePointsV2(ctx context.Context, req *writev2.Request) context.Context {
	return backdateSeries(ctx, req.Timeseries, fwh.futurePointsBackdateOffset.Milliseconds())
}

// backdateSeries modifies in place the given series (only if necessary),
// and returns a context that contains the offset by which the timestamps were shifted (if so).
func backdateSeries[TimeSeries prompb.TimeSeries | writev2.TimeSeries](
	ctx context.Context,
	series []TimeSeries,
	offsetMs int64,
) context.Context {
	backdated := false

	for _, s := range series {
		refSamples := reflect.ValueOf(s).FieldByName("Samples")
		if refSamples.Len() == 0 {
			continue
		}

		// We assume the last sample is the highest one of its series.
		lastSample := refSamples.Index(refSamples.Len() - 1)
		// Prometheus' threshold is currently 10 minutes, but we use 9 to be safe.
		if lastSample.FieldByName("Timestamp").Int() < time.Now().Add(9*time.Minute).UnixMilli() {
			continue // no need to backdate this series
		}

		backdated = true

		break
	}

	if !backdated {
		return ctx
	}

	// Apply backdating to all series, because all series will be reversed in remotestorage.writeMetrics
	for t, s := range series {
		refSamples := reflect.ValueOf(s).FieldByName("Samples")
		if refSamples.Len() == 0 {
			continue
		}

		refSeries := reflect.ValueOf(series).Index(t)

		for i := range refSamples.Len() {
			newTS := refSamples.Index(i).FieldByName("Timestamp").Int() - offsetMs
			refSeries.FieldByName("Samples").Index(i).FieldByName("Timestamp").Set(reflect.ValueOf(newTS))
		}
	}

	ctx = context.WithValue(ctx, types.BackdateContextKey{}, offsetMs)

	return ctx
}
