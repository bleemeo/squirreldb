package promql

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/rs/zerolog"
)

var (
	errMissingTenantHeader = errors.New("the tenant header is missing")
	errInvalidMatcher      = errors.New("invalid matcher")
	errNoPerRequestData    = errors.New("no per-request data provided in request context")
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
	mint    int64
	maxt    int64
	logger  zerolog.Logger
	metrics *metrics
}

type perRequest struct {
	tenant             string
	maxEvaluatedSeries uint32
	limitIndex         *limitingIndex
	maxEvaluatedPoints uint64
	reader             *limitingReader
	enableDebug        bool
	enableVerboseDebug bool
	forcePreAggregated bool
	forceRaw           bool
}

type perRequestContextKey struct{}

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
) Store {
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

// ContextFromRequest wraps the given request into its context,
// and includes the perRequest data needed for caching, etc...
func (s Store) ContextFromRequest(r *http.Request) (context.Context, error) {
	perRequestData, err := s.makePerRequestData(r)
	if err != nil {
		return nil, err
	}

	ctx := r.Context()
	// We still include the request in the context, because the remotestorage appender needs some headers.
	return types.WrapContext(context.WithValue(ctx, perRequestContextKey{}, perRequestData), r), nil
}

func (s Store) makePerRequestData(r *http.Request) (perRequest, error) {
	index := types.Index(reducedTimeRangeIndex{index: s.Index})

	value := r.Header.Get(types.HeaderForcedMatcher)
	if value != "" {
		part := strings.SplitN(value, "=", 2)
		if len(part) != 2 {
			return perRequest{}, fmt.Errorf("%w: \"%s\", require labelName=labelValue", errInvalidMatcher, value)
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
		return perRequest{}, errMissingTenantHeader
	}

	maxEvaluatedSeries := s.DefaultMaxEvaluatedSeries

	maxEvaluatedSeriesText := r.Header.Get(types.HeaderMaxEvaluatedSeries)
	if maxEvaluatedSeriesText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedSeriesText, 10, 32)
		if err != nil {
			return perRequest{}, err
		}

		maxEvaluatedSeries = uint32(tmp) //nolint:gosec
	}

	limitIndex := &limitingIndex{
		index:          index,
		maxTotalSeries: maxEvaluatedSeries,
		returnedSeries: new(uint32),
	}

	maxEvaluatedPoints := s.DefaultMaxEvaluatedPoints

	maxEvaluatedPointsText := r.Header.Get(types.HeaderMaxEvaluatedPoints)
	if maxEvaluatedPointsText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedPointsText, 10, 64)
		if err != nil {
			return perRequest{}, err
		}

		maxEvaluatedPoints = tmp
	}

	reader := &limitingReader{
		reader:         &cachingReader{reader: s.Reader},
		maxTotalPoints: maxEvaluatedPoints,
		returnedPoints: new(uint64),
	}

	enableDebug := r.Header.Get(types.HeaderQueryDebug) != ""
	enableVerboseDebug := r.Header.Get(types.HeaderQueryVerboseDebug) != ""

	if enableVerboseDebug {
		enableDebug = true
	}

	var forcePreAggregated, forceRaw bool

	value = r.Header.Get(types.HeaderForcePreAggregated)
	if value != "" {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return perRequest{}, err
		}

		forcePreAggregated = tmp
	}

	value = r.Header.Get(types.HeaderForceRaw)
	if value != "" {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return perRequest{}, err
		}

		forceRaw = tmp
		if forceRaw {
			forcePreAggregated = false
		}
	}

	perRequestQuerier := perRequest{
		tenant:             tenant,
		maxEvaluatedSeries: maxEvaluatedSeries,
		limitIndex:         limitIndex,
		maxEvaluatedPoints: maxEvaluatedPoints,
		reader:             reader,
		enableDebug:        enableDebug,
		enableVerboseDebug: enableVerboseDebug,
		forcePreAggregated: forcePreAggregated,
		forceRaw:           forceRaw,
	}

	return perRequestQuerier, nil
}

func (s Store) newQuerier(mint, maxt int64) *querier {
	return &querier{
		mint:    mint,
		maxt:    maxt,
		logger:  s.Logger,
		metrics: s.metrics,
	}
}

// Querier returns a storage.Querier to read from SquirrelDB store.
func (s Store) Querier(mint, maxt int64) (storage.Querier, error) {
	return s.newQuerier(mint, maxt), nil
}

// ChunkQuerier returns a storage.ChunkQuerier to read from SquirrelDB store.
func (s Store) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return chunkquerier{s.newQuerier(mint, maxt)}, nil
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted.
// Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimizing select,
// but it's up to implementation how this is used if used at all.
func (q *querier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet { //nolint: lll
	minT := time.UnixMilli(q.mint)
	maxT := time.UnixMilli(q.maxt)

	perRequestData, err := getPerRequestData(ctx)
	if err != nil {
		return &seriesIter{err: err}
	}

	if perRequestData.enableDebug {
		q.logger.Info().Msgf(
			"Querier started. mint=%s maxt=%s. tenant=%s maxPoints=%d maxSeries=%d",
			minT.Format(time.RFC3339),
			maxT.Format(time.RFC3339),
			perRequestData.tenant,
			perRequestData.maxEvaluatedPoints,
			perRequestData.maxEvaluatedSeries,
		)
	}

	if perRequestData.enableDebug {
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

	metrics, err := perRequestData.limitIndex.Search(ctx, minT, maxT, matchers)
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
		FromTimestamp:      minT.UnixMilli(),
		ToTimestamp:        maxT.UnixMilli(),
		ForcePreAggregated: perRequestData.forcePreAggregated,
		ForceRaw:           perRequestData.forceRaw,
		EnableDebug:        perRequestData.enableDebug,
		EnableVerboseDebug: perRequestData.enableVerboseDebug,
	}

	if hints != nil {
		// We don't use hints.Start/End, because PromQL engine will anyway
		// set q.mint/q.maxt to good value. Actually hints.Start/End will make
		// the first point incomplet when using range (like "rate(metric[5m])")
		req.Function = hints.Func
		req.StepMs = hints.Step
	}

	result, err := perRequestData.reader.ReadIter(ctx, req)

	if perRequestData.enableDebug {
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
		index:     perRequestData.limitIndex,
		id2Labels: id2Labels,
		err:       err,
	}
}

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifefime of the querier.
func (q *querier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) { //nolint: lll
	minT := time.UnixMilli(q.mint)
	maxT := time.UnixMilli(q.maxt)

	perRequestData, err := getPerRequestData(ctx)
	if err != nil {
		return nil, nil, err
	}

	res, err := perRequestData.limitIndex.LabelValues(ctx, minT, maxT, name, matchers)

	return res, nil, err
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *querier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) { //nolint: lll
	minT := time.UnixMilli(q.mint)
	maxT := time.UnixMilli(q.maxt)

	perRequestData, err := getPerRequestData(ctx)
	if err != nil {
		return nil, nil, err
	}

	res, err := perRequestData.limitIndex.LabelNames(ctx, minT, maxT, matchers)

	return res, nil, err
}

// Close releases the resources of the Querier.
func (q *querier) Close() error {
	return nil
}

type chunkquerier struct {
	*querier
}

func (q chunkquerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.ChunkSeriesSet {
	sset := q.querier.Select(ctx, sortSeries, hints, matchers...)

	return storage.NewSeriesSetToChunkSet(sset)
}

func getPerRequestData(ctx context.Context) (perRequest, error) {
	perRequestData, ok := ctx.Value(perRequestContextKey{}).(perRequest)
	if !ok {
		return perRequest{}, errNoPerRequestData
	}

	return perRequestData, nil
}
