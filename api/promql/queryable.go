package promql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"squirreldb/dummy"
	"squirreldb/types"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/rs/zerolog"
)

var (
	errMissingRequest      = errors.New("HTTP request not found in context")
	errMissingTenantHeader = errors.New("the tenant header is missing")
	errInvalidMatcher      = errors.New("invalid matcher")
)

// Store implement Prometheus.Queryable and read from SquirrelDB Store.
type Store struct {
	Logger          zerolog.Logger
	Index           types.Index
	Reader          types.MetricReader
	TenantLabelName string
	// When enabled, return an error to queries that don't provide the tenant header.
	RequireTenantHeader       bool
	DefaultMaxEvaluatedPoints uint64
	DefaultMaxEvaluatedSeries uint32

	metrics *metrics
}

type querier struct {
	ctx                context.Context //nolint:containedctx
	logger             zerolog.Logger
	enableDebug        bool
	index              IndexWithStats
	reader             MetricReaderWithStats
	mint               int64
	maxt               int64
	forcePreAggregated bool
	forceRaw           bool
	metrics            *metrics
}

type MetricReaderWithStats interface {
	types.MetricReader
	PointsRead() float64
	PointsCached() float64
}

type metricReaderWithCache interface {
	types.MetricReader
	PointsCached() float64
}

type IndexWithStats interface {
	types.Index
	SeriesReturned() float64
}

func NewStore(
	logger zerolog.Logger,
	index types.Index,
	reader types.MetricReader,
	tenantLabelName string,
	requireTenantHeader bool,
	promQLMaxEvaluatedSeries uint32,
	promQLMaxEvaluatedPoints uint64,
	metricRegistry prometheus.Registerer,
) storage.SampleAndChunkQueryable {
	store := Store{
		Logger:                    logger,
		Index:                     index,
		Reader:                    reader,
		TenantLabelName:           tenantLabelName,
		RequireTenantHeader:       requireTenantHeader,
		DefaultMaxEvaluatedSeries: promQLMaxEvaluatedSeries,
		DefaultMaxEvaluatedPoints: promQLMaxEvaluatedPoints,
		metrics:                   newMetrics(metricRegistry),
	}

	return store
}

// newIndexAndReaderFromHeaders builds a new index and metric reader with limits from
// the HTTP headers. Available headers are: HeaderForcedMatcher, HeaderMaxEvaluatedSeries,
// HeaderMaxEvaluatedPoints, HeaderForcePreAggregated and HeaderForceRaw. See their declaration
// for documentation.
func (s Store) newQuerierFromHeaders(ctx context.Context, mint, maxt int64) (querier, error) {
	r, ok := ctx.Value(types.RequestContextKey{}).(*http.Request)
	if !ok {
		return querier{}, errMissingRequest
	}

	var index types.Index

	index = reducedTimeRangeIndex{
		index: s.Index,
	}

	value := r.Header.Get(types.HeaderForcedMatcher)
	if value != "" {
		part := strings.SplitN(value, "=", 2)
		if len(part) != 2 {
			return querier{}, fmt.Errorf("%w: \"%s\", require labelName=labelValue", errInvalidMatcher, value)
		}

		index = filteringIndex{
			index: index,
			matcher: labels.MustNewMatcher(
				labels.MatchEqual,
				part[0],
				part[1],
			),
		}
	}

	// The tenant header makes queries match only metrics from this tenant.
	tenant := r.Header.Get(types.HeaderTenant)
	if tenant != "" {
		index = filteringIndex{
			index: index,
			matcher: labels.MustNewMatcher(
				labels.MatchEqual,
				s.TenantLabelName,
				tenant,
			),
		}
	} else if s.RequireTenantHeader {
		return querier{}, errMissingTenantHeader
	}

	maxEvaluatedSeries := s.DefaultMaxEvaluatedSeries

	maxEvaluatedSeriesText := r.Header.Get(types.HeaderMaxEvaluatedSeries)
	if maxEvaluatedSeriesText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedSeriesText, 10, 32)
		if err != nil {
			return querier{}, err
		}

		maxEvaluatedSeries = uint32(tmp)
	}

	limitIndex := &limitingIndex{
		index:          index,
		maxTotalSeries: maxEvaluatedSeries,
	}

	maxEvaluatedPoints := s.DefaultMaxEvaluatedPoints

	maxEvaluatedPointsText := r.Header.Get(types.HeaderMaxEvaluatedPoints)
	if maxEvaluatedPointsText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedPointsText, 10, 64)
		if err != nil {
			return querier{}, err
		}

		maxEvaluatedPoints = tmp
	}

	reader := &limitingReader{
		reader:         &cachingReader{reader: s.Reader},
		maxTotalPoints: maxEvaluatedPoints,
	}

	enableDebug := r.Header.Get(types.HeaderQueryDebug) != ""
	if enableDebug {
		minTime := time.UnixMilli(mint)
		maxTime := time.UnixMilli(maxt)
		s.Logger.Info().Msgf(
			"Querier started. mint=%s maxt=%s. tenant=%s maxPoints=%d maxSeries=%d",
			minTime.Format(time.RFC3339),
			maxTime.Format(time.RFC3339),
			tenant,
			maxEvaluatedPoints,
			maxEvaluatedSeries,
		)
	}

	q := querier{
		ctx:         ctx,
		logger:      s.Logger,
		enableDebug: enableDebug,
		index:       limitIndex,
		reader:      reader,
		mint:        mint,
		maxt:        maxt,
		metrics:     s.metrics,
	}

	value = r.Header.Get(types.HeaderForcePreAggregated)
	if value != "" {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return querier{}, err
		}

		q.forcePreAggregated = tmp
	}

	value = r.Header.Get(types.HeaderForceRaw)
	if value != "" {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return querier{}, err
		}

		q.forceRaw = tmp
		if q.forceRaw {
			q.forcePreAggregated = false
		}
	}

	return q, nil
}

// Querier returns a storage.Querier to read from SquirrelDB store.
func (s Store) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return s.newQuerierFromHeaders(ctx, mint, maxt)
}

// ChunkQuerier returns a storage.ChunkQuerier to read from SquirrelDB store.
func (s Store) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := s.newQuerierFromHeaders(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	return chunkquerier{q}, nil
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted.
// Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimising select,
// but it's up to implementation how this is used if used at all.
func (q querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	minT := time.Unix(q.mint/1000, q.mint%1000)
	maxT := time.Unix(q.maxt/1000, q.maxt%1000)

	if q.enableDebug {
		hintsStr := "no hints"
		if hints != nil {
			hintsStr = fmt.Sprintf(
				"hints{start=%s, end=%s, step=%s, Function=%s}",
				time.UnixMilli(hints.Start).Format(time.RFC3339),
				time.UnixMilli(hints.End).Format(time.RFC3339),
				(time.Duration(hints.Step) * time.Millisecond).String(),
				hints.Func,
			)
		}

		q.logger.Info().Msgf(
			"Select(sort=%v, %s, matchers=%v)",
			sortSeries,
			hintsStr,
			matchers,
		)
	}

	metrics, err := q.index.Search(q.ctx, minT, maxT, matchers)
	if err != nil {
		return &seriesIter{err: err}
	}

	if metrics.Count() == 0 {
		return &seriesIter{}
	}

	if sortSeries {
		metricsList := make([]types.MetricLabel, 0, metrics.Count())
		for metrics.Next() {
			metricsList = append(metricsList, metrics.At())
		}

		if err := metrics.Err(); err != nil {
			return &seriesIter{err: err}
		}

		sort.Slice(metricsList, func(i, j int) bool {
			aLabels := metricsList[i].Labels
			bLabels := metricsList[j].Labels

			return labels.Compare(aLabels, bLabels) < 0
		})

		metrics = &dummy.MetricsLabel{List: metricsList}
	}

	id2Labels := make(map[types.MetricID]labels.Labels, metrics.Count())
	ids := make([]types.MetricID, 0, metrics.Count())

	for metrics.Next() {
		m := metrics.At()
		id2Labels[m.ID] = m.Labels
		ids = append(ids, m.ID)
	}

	if err := metrics.Err(); err != nil {
		return &seriesIter{err: err}
	}

	req := types.MetricRequest{
		IDs:                ids,
		FromTimestamp:      q.mint,
		ToTimestamp:        q.maxt,
		ForcePreAggregated: q.forcePreAggregated,
		ForceRaw:           q.forceRaw,
		EnableDebug:        q.enableDebug,
	}

	if hints != nil {
		// We don't use hints.Start/End, because PromQL engine will anyway
		// set q.mint/q.maxt to good value. Actually hints.Start/End will make
		// the first point incomplet when using range (like "rate(metric[5m])")
		req.Function = hints.Func
		req.StepMs = hints.Step
	}

	result, err := q.reader.ReadIter(q.ctx, req)

	if q.enableDebug {
		q.logger.Info().Msgf(
			"Select(...) returned %d metricID. request = {ForcePreAggregated=%v, ForceRaw=%v, Function=%v, StepMs=%d}",
			len(req.IDs),
			req.ForcePreAggregated,
			req.ForceRaw,
			req.Function,
			req.StepMs,
		)
	}

	return &seriesIter{
		list:      result,
		index:     q.index,
		id2Labels: id2Labels,
		err:       err,
	}
}

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifefime of the querier.
func (q querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	minT := time.Unix(q.mint/1000, q.mint%1000)
	maxT := time.Unix(q.maxt/1000, q.maxt%1000)

	res, err := q.index.LabelValues(q.ctx, minT, maxT, name, matchers)

	return res, nil, err
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q querier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	minT := time.Unix(q.mint/1000, q.mint%1000)
	maxT := time.Unix(q.maxt/1000, q.maxt%1000)

	res, err := q.index.LabelNames(q.ctx, minT, maxT, matchers)

	return res, nil, err
}

// Close releases the resources of the Querier.
func (q querier) Close() error {
	q.metrics.RequestsSeries.Observe(q.index.SeriesReturned())
	q.metrics.RequestsPoints.Observe(q.reader.PointsRead())
	q.metrics.CachedPoints.Add(q.reader.PointsCached())

	if q.enableDebug {
		q.logger.Info().Msgf(
			"End of querier. series count=%f, points count=%f",
			q.index.SeriesReturned(),
			q.reader.PointsRead(),
		)
	}

	return nil
}

type chunkquerier struct {
	querier
}

func (q chunkquerier) Select(
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.ChunkSeriesSet {
	sset := q.querier.Select(sortSeries, hints, matchers...)

	return storage.NewSeriesSetToChunkSet(sset)
}
