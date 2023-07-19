package remotestorage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"squirreldb/cassandra/tsdb"
	"squirreldb/types"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/gate"
	"github.com/rs/zerolog/log"
)

const pointInPastLogPeriod = 10 * time.Second

var (
	ErrMissingTenantHeader = errors.New("the tenant header is missing")
	ErrParseTTLHeader      = errors.New("can't parse time to live header ")
	errMissingRequest      = errors.New("HTTP request not found in context")
)

type RemoteStorage struct {
	writer               types.MetricWriter
	index                types.Index
	remoteWriteGate      *gate.Gate
	tenantLabelName      string
	mutableLabelDetector MutableLabelDetector
	// When enabled, return an error to write requests that don't provide the tenant header.
	requireTenantHeader bool
	metrics             *metrics
	// Used not to print log messages too frequently.
	lastLogPointInPastLock sync.Mutex
	lastLogPointInPastAt   time.Time
}

type MutableLabelDetector interface {
	IsMutableLabel(ctx context.Context, tenant, name string) (bool, error)
}

// New returns a new initialized appendable storage.
func New(
	writer types.MetricWriter,
	index types.Index,
	maxConcurrentRemoteWrite int,
	tenantLabelName string,
	mutableLabelDetector MutableLabelDetector,
	requireTenantHeader bool,
	reg prometheus.Registerer,
) storage.Appendable {
	remoteStorage := RemoteStorage{
		writer:               writer,
		index:                index,
		tenantLabelName:      tenantLabelName,
		mutableLabelDetector: mutableLabelDetector,
		requireTenantHeader:  requireTenantHeader,
		remoteWriteGate:      gate.New(maxConcurrentRemoteWrite),
		metrics:              newMetrics(reg),
	}

	return &remoteStorage
}

func (r *RemoteStorage) Appender(ctx context.Context) storage.Appender {
	// If the tenant header is present, the tenant label is added to all metrics written.
	request, ok := ctx.Value(types.RequestContextKey{}).(*http.Request)
	if !ok {
		return errAppender{errMissingRequest}
	}

	tenant := request.Header.Get(types.HeaderTenant)
	if tenant == "" && r.requireTenantHeader {
		return errAppender{ErrMissingTenantHeader}
	}

	// The TTL is set from the header.
	timeToLive := int64(0)

	ttlRaw := request.Header.Get(types.HeaderTimeToLive)
	if ttlRaw != "" {
		var err error

		timeToLive, err = strconv.ParseInt(ttlRaw, 10, 64)
		if err != nil {
			return errAppender{fmt.Errorf("%w '%s': %w", ErrParseTTLHeader, ttlRaw, err)}
		}
	}

	// Limit concurrent writes, block here if too many concurrent writes are running.
	// The appender must call remoteWriteGate.Done to release a slot.
	if err := r.remoteWriteGate.Start(ctx); err != nil {
		return errAppender{fmt.Errorf("too many concurrent remote write: %w", err)}
	}

	writeMetrics := &writeMetrics{
		index:                 r.index,
		writer:                r.writer,
		metrics:               r.metrics,
		tenantLabel:           labels.Label{Name: r.tenantLabelName, Value: tenant},
		mutableLabelDetector:  r.mutableLabelDetector,
		timeToLiveSeconds:     timeToLive,
		pendingTimeSeries:     make(map[uint64]timeSeries),
		metricsFromTimeSeries: r.metricsFromTimeSeries,
		done:                  r.remoteWriteGate.Done,
	}

	return writeMetrics
}

// Returns a metric list generated from a TimeSeries list.
func (r *RemoteStorage) metricsFromTimeSeries(
	ctx context.Context,
	pendingTimeSeries []timeSeries,
	index types.Index,
	timeToLiveSeconds int64,
) ([]types.MetricData, int, error) {
	if len(pendingTimeSeries) == 0 {
		return nil, 0, nil
	}

	idToIndex := make(map[types.MetricID]int, len(pendingTimeSeries))

	totalPoints := 0
	metrics := make([]types.MetricData, 0, len(pendingTimeSeries))

	requests := make([]types.LookupRequest, 0, len(pendingTimeSeries))

	for _, promSeries := range pendingTimeSeries {
		min := int64(math.MaxInt64)
		max := int64(math.MinInt64)

		for _, s := range promSeries.Samples {
			if min > s.Timestamp {
				min = s.Timestamp
			}

			if max < s.Timestamp {
				max = s.Timestamp
			}
		}

		if min < time.Now().Add(-tsdb.MaxPastDelay).Unix()*1000 {
			r.lastLogPointInPastLock.Lock()
			if time.Since(r.lastLogPointInPastAt) > pointInPastLogPeriod {
				log.Warn().Msgf("Points with timestamp %v will be ignored by pre-aggregation", time.Unix(min/1000, 0))
				r.lastLogPointInPastAt = time.Now()
			}
			r.lastLogPointInPastLock.Unlock()
		}

		requests = append(requests, types.LookupRequest{
			Labels:     promSeries.Labels,
			TTLSeconds: timeToLiveSeconds,
			End:        time.Unix(max/1000, max%1000),
			Start:      time.Unix(min/1000, min%1000),
		})
	}

	ids, ttls, err := index.LookupIDs(ctx, requests)
	if err != nil {
		return nil, totalPoints, fmt.Errorf("metric ID lookup failed: %w", err)
	}

	for i, promSeries := range pendingTimeSeries {
		data := types.MetricData{
			ID:         ids[i],
			Points:     promSeries.Samples,
			TimeToLive: ttls[i],
		}

		if idx, found := idToIndex[data.ID]; found {
			metrics[idx].Points = append(metrics[idx].Points, data.Points...)
			if metrics[idx].TimeToLive < data.TimeToLive {
				metrics[idx].TimeToLive = data.TimeToLive
			}
		} else {
			metrics = append(metrics, data)
			idToIndex[data.ID] = len(metrics) - 1
		}

		totalPoints += len(data.Points)
	}

	return metrics, totalPoints, nil
}

type errAppender struct {
	err error
}

func (a errAppender) Append(storage.SeriesRef, labels.Labels, int64, float64) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) AppendHistogram(
	storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram,
) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, a.err
}

func (a errAppender) Commit() error { return a.err }

func (a errAppender) Rollback() error { return a.err }
