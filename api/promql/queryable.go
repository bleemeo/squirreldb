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

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

var errMissingRequest = errors.New("HTTP request not found in context")

// Store implement Prometheus.Queryable and read from SquirrelDB Store.
type Store struct {
	Index                     types.Index
	Reader                    types.MetricReader
	DefaultMaxEvaluatedSeries uint32
	DefaultMaxEvaluatedPoints uint64
}

type querier struct {
	ctx        context.Context
	index      types.Index
	reader     types.MetricReader
	mint, maxt int64
}

// newIndexAndReaderFromHeaders builds a new index and metric reader with limits from
// the HTTP headers. Available headers are:
// * X-PromQL-Forced-Matcher: Add one matcher to limit evaluated series. Useful for
//   filtering per-tenant.
// * X-PromQL-Max-Evaluated-Series: Limit the number of series that can be evaluated
//   by this request
// * X-PromQL-Max-Evaluated-Points: Limit the number of points that can be evaluated
//   by this request.
// A limit of 0 means unlimited.
func (s Store) newIndexAndReaderFromHeaders(ctx context.Context) (types.Index, types.MetricReader, error) {
	r, ok := ctx.Value(RequestContextKey{}).(*http.Request)
	if !ok {
		return nil, nil, errMissingRequest
	}

	index := s.Index
	reader := s.Reader

	value := r.Header.Get("X-PromQL-Forced-Matcher")
	if value != "" {
		part := strings.SplitN(value, "=", 2)
		if len(part) != 2 {
			return nil, nil, fmt.Errorf("invalid matcher \"%s\", require labelName=labelValue", value)
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

	maxEvaluatedSeries := s.DefaultMaxEvaluatedSeries

	if maxEvaluatedSeriesText := r.Header.Get("X-PromQL-Max-Evaluated-Series"); maxEvaluatedSeriesText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedSeriesText, 10, 32)
		if err != nil {
			return nil, nil, err
		}

		maxEvaluatedSeries = uint32(tmp)
	}

	index = &limitingIndex{
		index:          index,
		maxTotalSeries: maxEvaluatedSeries,
	}

	maxEvaluatedPoints := s.DefaultMaxEvaluatedPoints

	if maxEvaluatedPointsText := r.Header.Get("X-PromQL-Max-Evaluated-Points"); maxEvaluatedPointsText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedPointsText, 10, 64)
		if err != nil {
			return nil, nil, err
		}

		maxEvaluatedPoints = tmp
	}

	reader = &limitingReader{
		reader:         reader,
		maxTotalPoints: maxEvaluatedPoints,
	}

	return index, reader, nil
}

// Querier returns a storage.Querier to read from SquirrelDB store.
func (s Store) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	index, reader, err := s.newIndexAndReaderFromHeaders(ctx)
	if err != nil {
		return querier{}, err
	}

	return querier{ctx: ctx, index: index, reader: reader, mint: mint, maxt: maxt}, nil
}

// ChunkQuerier returns a storage.ChunkQuerier to read from SquirrelDB store.
func (s Store) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	index, reader, err := s.newIndexAndReaderFromHeaders(ctx)
	if err != nil {
		return nil, err
	}

	return chunkquerier{querier{ctx: ctx, index: index, reader: reader, mint: mint, maxt: maxt}}, nil
}

// func (s Store) PointsRead() float64 {
// 	v := atomic.LoadUint64(&s.reader.returnedPoints)

// 	return float64(v)
// }

// func (s Store) SeriesReturned() float64 {
// 	v := atomic.LoadUint32(&s.index.returnedSeries)

// 	return float64(v)
// }

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted.
// Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimising select,
// but it's up to implementation how this is used if used at all.
func (q querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	minT := time.Unix(q.mint/1000, q.mint%1000)
	maxT := time.Unix(q.maxt/1000, q.maxt%1000)

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
		IDs:           ids,
		FromTimestamp: q.mint,
		ToTimestamp:   q.maxt,
	}

	if hints != nil {
		// We don't use hints.Start/End, because PromQL engine will anyway
		// set q.mint/q.maxt to good value. Actually hints.Start/End will make
		// the first point incomplet when using range (like "rate(metric[5m])")
		req.Function = hints.Func
		req.StepMs = hints.Step
	}

	result, err := q.reader.ReadIter(q.ctx, req)

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
