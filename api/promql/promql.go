// Disable stylecheck because is complain on error message (should not be capitalized)
// but we prefer keeping the exact message used by Prometheus.

//nolint: stylecheck
package promql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"regexp"
	"squirreldb/types"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/httputil"
	"github.com/prometheus/prometheus/util/stats"
)

type errorType string

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

//nolint: gochecknoglobals
var (
	minTime = time.Unix(0, 0).UTC()
	maxTime = time.Unix(math.MaxInt32*3600, 0).UTC()
)

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

const (
	errorTimeout  errorType = "timeout"
	errorCanceled errorType = "canceled"
	errorExec     errorType = "execution"
	errorBadData  errorType = "bad_data"
	errorInternal errorType = "internal"
	errorNotFound errorType = "not_found"
)

type PromQL struct {
	CORSOrigin         *regexp.Regexp
	Index              types.Index
	Reader             types.MetricReader
	MaxEvaluatedSeries uint32
	MaxEvaluatedPoints uint64

	logger      log.Logger
	queryEngine *promql.Engine
}

type apiFunc func(r *http.Request) apiFuncResult

// Register the API's endpoints in the given router.
func (p *PromQL) Register(r *route.Router) {
	p.init()

	wrap := func(f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if p.CORSOrigin != nil {
				httputil.SetCORS(w, p.CORSOrigin, r)
			}

			result := f(r)
			if result.finalizer != nil {
				defer result.finalizer()
			}

			if result.err != nil {
				p.respondError(w, result.err, result.data)

				return
			}

			if result.data != nil {
				p.respond(w, result.data, result.warnings)

				return
			}

			w.WriteHeader(http.StatusNoContent)
		})

		return httputil.CompressionHandler{
			Handler: hf,
		}.ServeHTTP
	}

	r.Get("/query", wrap(p.query))
	r.Post("/query", wrap(p.query))
	r.Get("/query_range", wrap(p.queryRange))
	r.Post("/query_range", wrap(p.queryRange))
	r.Get("/series", wrap(p.series))
	r.Post("/series", wrap(p.series))

	r.Get("/labels", wrap(p.labelNames))
	r.Post("/labels", wrap(p.labelNames))
	r.Get("/label/:name/values", wrap(p.labelValues))
}

func (p *PromQL) init() {
	p.logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	opts := promql.EngineOpts{
		Logger:             log.With(p.logger, "component", "query engine"),
		Reg:                nil,
		MaxSamples:         50000000,
		Timeout:            2 * time.Minute,
		ActiveQueryTracker: nil,
		LookbackDelta:      5 * time.Minute,
	}
	p.queryEngine = promql.NewEngine(opts)
}

func (p *PromQL) queryable(r *http.Request) storage.Queryable {
	st := Store{
		Index:  p.Index,
		Reader: p.Reader,
	}

	value := r.Header.Get("X-PromQL-Forced-Matcher")
	if value != "" {
		part := strings.SplitN(value, "=", 2)
		if len(part) != 2 {
			st.Err = fmt.Errorf("Invalid matcher \"%s\", require labelName=labelValue", value)

			return st
		}

		st.Index = filteringIndex{
			index: st.Index,
			matcher: labels.MustNewMatcher(
				labels.MatchEqual,
				part[0],
				part[1],
			),
		}
	}

	maxEvaluatedSeries := p.MaxEvaluatedSeries

	if maxEvaluatedSeriesText := r.Header.Get("X-PromQL-Max-Evaluated-Series"); maxEvaluatedSeriesText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedSeriesText, 10, 32)
		if err != nil {
			st.Err = err

			return st
		}

		if tmp > 0 && (uint32(tmp) < maxEvaluatedSeries || maxEvaluatedSeries == 0) {
			maxEvaluatedSeries = uint32(tmp)
		}
	}

	if maxEvaluatedSeries > 0 {
		st.Index = &limitingIndex{
			index:          st.Index,
			maxTotalSeries: maxEvaluatedSeries,
		}
	}

	maxEvaluatedPoints := p.MaxEvaluatedPoints

	if maxEvaluatedPointsText := r.Header.Get("X-PromQL-Max-Evaluated-Points"); maxEvaluatedPointsText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedPointsText, 10, 64)
		if err != nil {
			st.Err = err

			return st
		}

		if tmp > 0 && (tmp < maxEvaluatedPoints || maxEvaluatedPoints == 0) {
			maxEvaluatedPoints = tmp
		}
	}

	if maxEvaluatedPoints > 0 {
		st.Reader = &limitingReader{
			reader:         st.Reader,
			maxTotalPoints: maxEvaluatedPoints,
		}
	}

	return st
}

type apiFuncResult struct {
	data      interface{}
	err       *apiError
	warnings  storage.Warnings
	finalizer func()
}

type queryData struct {
	ResultType parser.ValueType  `json:"resultType"`
	Result     parser.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

type apiError struct {
	typ errorType
	err error
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000

		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}

	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}

	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("Invalid time value for '%s': %w", paramName, err)
	}

	return result, nil
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}

		return time.Duration(ts), nil
	}

	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}

	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func returnAPIError(err error) *apiError {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, promql.ErrQueryCanceled("")):
		return &apiError{errorCanceled, err}
	case errors.Is(err, promql.ErrQueryTimeout("")):
		return &apiError{errorTimeout, err}
	case errors.Is(err, promql.ErrStorage{}):
		return &apiError{errorInternal, err}
	}

	return &apiError{errorExec, err}
}

func (p *PromQL) queryRange(r *http.Request) (result apiFuncResult) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		err = fmt.Errorf("invalid parameter 'start': %w", err)

		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		err = fmt.Errorf("invalid parameter 'end': %w", err)

		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")

		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		err = fmt.Errorf("invalid parameter 'step': %w", err)

		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")

		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")

		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx := r.Context()

	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc

		timeout, err := parseDuration(to)
		if err != nil {
			err = fmt.Errorf("invalid parameter 'timeout': %w", err)

			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := p.queryEngine.NewRangeQuery(p.queryable(r), r.FormValue("query"), start, end, step)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	var qs *stats.QueryStats
	if r.FormValue("stats") != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}

	return apiFuncResult{&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, res.Warnings, qry.Close}
}

func (p *PromQL) query(r *http.Request) (result apiFuncResult) {
	ts, err := parseTimeParam(r, "time", time.Now())
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx := r.Context()

	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc

		timeout, err := parseDuration(to)
		if err != nil {
			err = fmt.Errorf("invalid parameter 'timeout': %w", err)

			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := p.queryEngine.NewInstantQuery(p.queryable(r), r.FormValue("query"), ts)
	if err != nil {
		err = fmt.Errorf("invalid parameter 'query': %w", err)

		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	var qs *stats.QueryStats
	if r.FormValue("stats") != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}

	return apiFuncResult{&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, res.Warnings, qry.Close}
}

func (p *PromQL) series(r *http.Request) (result apiFuncResult) {
	if err := r.ParseForm(); err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("error parsing form values: %w", err)}, nil, nil}
	}

	if len(r.Form["match[]"]) == 0 {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("no match[] parameter provided")}, nil, nil}
	}

	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	matcherSets := make([][]*labels.Matcher, 0, len(r.Form["match[]"]))

	for _, s := range r.Form["match[]"] {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
		}

		matcherSets = append(matcherSets, matchers)
	}

	q, err := p.queryable(r).Querier(r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call q.Close ourselves (which is required
	// in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			q.Close()
		}
	}()

	closer := func() {
		q.Close()
	}

	var warnings storage.Warnings

	sets := make([]storage.SeriesSet, 0, len(matcherSets))

	for _, mset := range matcherSets {
		s := q.Select(false, nil, mset...)
		sets = append(sets, s)
	}

	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	metrics := []labels.Labels{}

	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}

	warnings = append(warnings, set.Warnings()...)

	if set.Err() != nil {
		return apiFuncResult{nil, &apiError{errorExec, set.Err()}, warnings, closer}
	}

	return apiFuncResult{metrics, nil, warnings, closer}
}

func (p *PromQL) labelNames(r *http.Request) apiFuncResult {
	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("invalid parameter 'start': %w", err)}, nil, nil}
	}

	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("invalid parameter 'end': %w", err)}, nil, nil}
	}

	q, err := p.queryable(r).Querier(r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	defer q.Close()

	names, warnings, err := q.LabelNames()
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, warnings, nil}
	}

	if names == nil {
		names = []string{}
	}

	return apiFuncResult{names, nil, warnings, nil}
}

func (p *PromQL) labelValues(r *http.Request) (result apiFuncResult) {
	ctx := r.Context()
	name := route.Param(ctx, "name")

	if !model.LabelNameRE.MatchString(name) {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("invalid label name: %q", name)}, nil, nil}
	}

	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("invalid parameter 'start': %w", err)}, nil, nil}
	}

	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, fmt.Errorf("invalid parameter 'end': %w", err)}, nil, nil}
	}

	q, err := p.queryable(r).Querier(r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call q.Close ourselves (which is required
	// in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			q.Close()
		}
	}()

	closer := func() {
		q.Close()
	}

	vals, warnings, err := q.LabelValues(name)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, warnings, closer}
	}

	if vals == nil {
		vals = []string{}
	}

	return apiFuncResult{vals, nil, warnings, closer}
}

func (p *PromQL) respond(w http.ResponseWriter, data interface{}, warnings storage.Warnings) {
	statusMessage := statusSuccess
	warningStrings := make([]string, 0, len(warnings))

	for _, warning := range warnings {
		warningStrings = append(warningStrings, warning.Error())
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary

	b, err := json.Marshal(&response{
		Status:   statusMessage,
		Data:     data,
		Warnings: warningStrings,
	})
	if err != nil {
		_ = p.logger.Log("msg", "error marshaling json response", "err", err)

		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(b); err != nil {
		_ = p.logger.Log("msg", "error writing response", "err", err)
	}
}

func (p *PromQL) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		_ = p.logger.Log("msg", "error marshaling json response", "err", err)

		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	var code int

	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	if _, err := w.Write(b); err != nil {
		_ = p.logger.Log("msg", "error writing response", "err", err)
	}
}
