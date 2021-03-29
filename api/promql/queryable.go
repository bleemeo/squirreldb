package promql

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"squirreldb/dummy"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// QueryableFactory build a Queryable with limit from HTTP header set.
// header are:
// * X-PromQL-Forced-Matcher: Add one matcher to limit series evaluated. Useful for
//   filtering per-tenant.
// * X-PromQL-Max-Evaluated-Series: Limit the number of series that can be evaluated
//   by this request
// * X-PromQL-Max-Evaluated-Points: Limit the number of poitns that can be evaluated
//   by this request.
// A limit of 0 means unlimited.
type QueryableFactory struct {
	Index                     types.Index
	Reader                    types.MetricReader
	DefaultMaxEvaluatedSeries uint32
	DefaultMaxEvaluatedPoints uint64
}

func (f QueryableFactory) Queryable(ctx context.Context, r *http.Request) QueryableWithStats {
	idx := f.Index
	reader := f.Reader

	st := store{}

	value := r.Header.Get("X-PromQL-Forced-Matcher")
	if value != "" {
		part := strings.SplitN(value, "=", 2)
		if len(part) != 2 {
			st.Err = fmt.Errorf("invalid matcher \"%s\", require labelName=labelValue", value)

			return st
		}

		idx = filteringIndex{
			index: idx,
			matcher: labels.MustNewMatcher(
				labels.MatchEqual,
				part[0],
				part[1],
			),
		}
	}

	maxEvaluatedSeries := f.DefaultMaxEvaluatedSeries

	if maxEvaluatedSeriesText := r.Header.Get("X-PromQL-Max-Evaluated-Series"); maxEvaluatedSeriesText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedSeriesText, 10, 32)
		if err != nil {
			st.Err = err

			return st
		}

		maxEvaluatedSeries = uint32(tmp)
	}

	st.Index = &limitingIndex{
		index:          idx,
		maxTotalSeries: maxEvaluatedSeries,
	}

	maxEvaluatedPoints := f.DefaultMaxEvaluatedPoints

	if maxEvaluatedPointsText := r.Header.Get("X-PromQL-Max-Evaluated-Points"); maxEvaluatedPointsText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedPointsText, 10, 64)
		if err != nil {
			st.Err = err

			return st
		}

		maxEvaluatedPoints = tmp
	}

	st.Reader = &limitingReader{
		reader:         reader,
		maxTotalPoints: maxEvaluatedPoints,
	}

	return st
}

// store implement Prometheus.Queryable and read from SquirrelDB store.
type store struct {
	Err    error
	Index  *limitingIndex
	Reader *limitingReader
}

type querier struct {
	ctx        context.Context
	index      types.Index
	reader     types.MetricReader
	mint, maxt int64
}

// Querier returns a storage.Querier to read from SquirrelDB store.
func (s store) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if s.Err != nil {
		return nil, s.Err
	}

	return querier{ctx: ctx, index: s.Index, reader: s.Reader, mint: mint, maxt: maxt}, nil
}

func (s store) PointsRead() float64 {
	v := atomic.LoadUint64(&s.Reader.returnedPoints)
	return float64(v)
}

func (s store) SeriesReturned() float64 {
	v := atomic.LoadUint32(&s.Index.returnedSeries)
	return float64(v)
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimising select, but it's up to implementation how this is used if used at all.
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
func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	minT := time.Unix(q.mint/1000, q.mint%1000)
	maxT := time.Unix(q.maxt/1000, q.maxt%1000)

	res, err := q.index.LabelNames(q.ctx, minT, maxT, nil)

	return res, nil, err
}

// Close releases the resources of the Querier.
func (q querier) Close() error {
	return nil
}
