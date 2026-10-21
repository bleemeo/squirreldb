// Copyright 2015-2026 Bleemeo
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
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"time"
	"unsafe"

	"github.com/bleemeo/squirreldb/types"

	"github.com/gogo/protobuf/proto"
	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

// setRemoteWriteHandler sets the given api's remoteWriteHandler field
// to a handler that backdates the timestamps of the points that are
// in the future by the given backdateOffset.
// The purpose of this operation is to avoid the points that are produced by
// bleemeo-forecast getting blocked by Prometheus' 10-minute barrier.
// The api must be created with remote write disabled, otherwise the
// Prometheus handler metrics would be registered twice.
func setRemoteWriteHandler(
	api *v1.API,
	logger *slog.Logger,
	reg prometheus.Registerer,
	appendable storage.Appendable,
	backdateOffset time.Duration,
) {
	store := &backdatingWriteStore{
		backdateOffset: backdateOffset,
		promHandler: &writeHandler{
			logger:     logger,
			appendable: appendable,
			samplesWithInvalidLabelsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Namespace: "prometheus",
				Subsystem: "api",
				Name:      "remote_write_invalid_labels_samples_total",
				Help:      "The total number of received remote write samples and histogram samples which were rejected due to invalid labels.", //nolint:lll
			}),
			samplesAppendedWithoutMetadata: promauto.With(reg).NewCounter(prometheus.CounterOpts{
				Namespace: "prometheus",
				Subsystem: "api",
				Name:      "remote_write_without_metadata_appended_samples_total",
				Help:      "The total number of received remote write samples (and histogram samples) which were ingested without corresponding metadata.", //nolint:lll
			}),
		},
	}

	handler := remoteapi.NewWriteHandler(
		store,
		allowedProtoMsgs,
		remoteapi.WithWriteHandlerLogger(logger),
		remoteapi.WithWriteHandlerMiddlewares(remoteapi.SnappyDecodeMiddleware(logger)),
	)

	writeHandlerField := reflect.ValueOf(api).Elem().FieldByName("remoteWriteHandler")
	// Making the API's remoteWriteHandler field writable
	writableWriteHandler := reflect.NewAt(writeHandlerField.Type(), unsafe.Pointer(writeHandlerField.UnsafeAddr())).Elem()
	writableWriteHandler.Set(reflect.ValueOf(handler))
}

// writeHandler is a copy of github.com/prometheus/prometheus/storage/remote.writeHandler,
// used to call its private methods.
// It must be updated each time it is modified on the Prometheus side.
type writeHandler struct {
	logger     *slog.Logger
	appendable storage.Appendable

	samplesWithInvalidLabelsTotal  prometheus.Counter
	samplesAppendedWithoutMetadata prometheus.Counter
}

//go:linkname write github.com/prometheus/prometheus/storage/remote.(*writeHandler).write
func write(h *writeHandler, ctx context.Context, req *prompb.WriteRequest) error

//go:linkname writeV2 github.com/prometheus/prometheus/storage/remote.(*writeHandler).writeV2
func writeV2(h *writeHandler, ctx context.Context, req *writev2.Request) (remoteapi.WriteResponseStats, int, error) //nolint: lll

type backdatingWriteStore struct {
	backdateOffset time.Duration
	promHandler    *writeHandler
}

// Store is a copy of github.com/prometheus/prometheus/storage/remote.(*writeHandler).Store,
// modified to backdate points that are more than 10 minutes in the future.
func (s *backdatingWriteStore) Store(
	r *http.Request,
	msgType remoteapi.WriteMessageType,
) (*remoteapi.WriteResponse, error) {
	logger := s.promHandler.logger

	// Store receives request with decompressed content in body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.ErrorContext(r.Context(), "Error reading remote write request body", "err", err.Error())

		return nil, err
	}

	wr := remoteapi.NewWriteResponse()

	if msgType == remoteapi.WriteV1MessageType {
		// PRW 1.0 flow has different proto message and no partial write handling.
		var req prompb.WriteRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			logger.ErrorContext(
				r.Context(),
				"Error decoding v1 remote write request",
				"protobuf_message",
				msgType,
				"err",
				err.Error(),
			)
			wr.SetStatusCode(http.StatusBadRequest)

			return wr, err
		}

		ctx := backdateSeries(r.Context(), req.Timeseries, s.backdateOffset.Milliseconds())
		if err = write(s.promHandler, ctx, &req); err != nil {
			var histogramErr histogram.Error

			switch {
			case errors.Is(err, storage.ErrOutOfOrderSample),
				errors.Is(err, storage.ErrOutOfBounds),
				errors.Is(err, storage.ErrDuplicateSampleForTimestamp),
				errors.Is(err, storage.ErrTooOldSample),
				errors.As(err, &histogramErr):
				// Indicated an out-of-order sample is a bad request to prevent retries.
				wr.SetStatusCode(http.StatusBadRequest)
			default:
				wr.SetStatusCode(http.StatusInternalServerError)
			}

			return wr, err
		}

		return wr, nil
	}

	// Remote Write 2.x proto message handling.
	var req writev2.Request
	if err := proto.Unmarshal(body, &req); err != nil {
		logger.ErrorContext(
			r.Context(),
			"Error decoding v2 remote write request",
			"protobuf_message",
			msgType,
			"err",
			err.Error(),
		)
		wr.SetStatusCode(http.StatusBadRequest)

		return wr, err
	}

	ctx := backdateSeries(r.Context(), req.Timeseries, s.backdateOffset.Milliseconds())
	respStats, errHTTPCode, err := writeV2(s.promHandler, ctx, &req)
	// Add stats required X-Prometheus-Remote-Write-Written-* response headers.
	wr.Add(respStats)

	if err != nil {
		wr.SetStatusCode(errHTTPCode)

		return wr, err
	}

	return wr, nil
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
