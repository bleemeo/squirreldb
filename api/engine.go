package api

import (
	"context"
	"fmt"
	"net/http"
	"squirreldb/types"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/rs/zerolog"
)

type wrapperEngine struct {
	v1.QueryEngine
	logger zerolog.Logger
}

func (e wrapperEngine) NewInstantQuery(
	q storage.Queryable,
	opts *promql.QueryOpts,
	qs string,
	ts time.Time,
) (promql.Query, error) {
	query, err := e.QueryEngine.NewInstantQuery(q, opts, qs, ts)

	optsStr := "noOptions"
	if opts != nil {
		optsStr = fmt.Sprintf("options=%#v", *opts)
	}

	query = wrapperQuery{
		Query: query,
		logMessage: fmt.Sprintf(
			"Start query %s qs=%s ts=%s",
			optsStr,
			qs,
			ts.Format(time.RFC3339),
		),
		logger: e.logger,
	}

	return query, err
}

func (e wrapperEngine) NewRangeQuery(
	q storage.Queryable,
	opts *promql.QueryOpts,
	qs string,
	start,
	end time.Time,
	interval time.Duration,
) (promql.Query, error) {
	query, err := e.QueryEngine.NewRangeQuery(q, opts, qs, start, end, interval)

	optsStr := "noOptions"
	if opts != nil {
		optsStr = fmt.Sprintf("options=%#v", *opts)
	}

	query = wrapperQuery{
		Query: query,
		logMessage: fmt.Sprintf(
			"Start query_range %s qs=%s start/end/interval=%s to %s interval %s",
			optsStr,
			qs,
			start.Format(time.RFC3339),
			end.Format(time.RFC3339),
			interval.String(),
		),
		logger: e.logger,
	}

	return query, err
}

type wrapperQuery struct {
	promql.Query
	logMessage string
	logger     zerolog.Logger
}

func (q wrapperQuery) Exec(ctx context.Context) *promql.Result {
	r, ok := ctx.Value(types.RequestContextKey{}).(*http.Request)
	if ok {
		enableDebug := r.Header.Get(types.HeaderQueryDebug) != ""
		if enableDebug {
			q.logger.Info().Msg(q.logMessage)
		}
	}

	return q.Query.Exec(ctx)
}
