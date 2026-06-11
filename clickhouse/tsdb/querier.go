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

package tsdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// querier serves Select through the remote_read client and LabelValues/LabelNames
// through SQL on the tsdb_tags table (the remote_read protocol does not support
// label queries).
type querier struct {
	labelQuerier

	remote storage.Querier
}

func (q *querier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	return q.remote.Select(ctx, sortSeries, hints, matchers...)
}

func (q *querier) Close() error {
	return q.remote.Close()
}

// chunkQuerier is the ChunkQuerier counterpart of querier.
type chunkQuerier struct {
	labelQuerier

	remote storage.ChunkQuerier
}

func (q *chunkQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.ChunkSeriesSet {
	return q.remote.Select(ctx, sortSeries, hints, matchers...)
}

func (q *chunkQuerier) Close() error {
	return q.remote.Close()
}

// labelQuerier implements LabelValues/LabelNames against the tsdb_tags table.
type labelQuerier struct {
	conn        driver.Conn
	tenantLabel string
}

// LabelValues returns the values of a label among the series matching matchers.
func (lq *labelQuerier) LabelValues(
	ctx context.Context,
	name string,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	where, args := lq.buildWhere(matchers)

	var (
		query     string
		queryArgs []any
	)

	switch name {
	case labels.MetricName:
		query = "SELECT DISTINCT metric_name AS v FROM tsdb_tags WHERE " + where + " ORDER BY v"
		queryArgs = args
	case lq.tenantLabel:
		query = "SELECT DISTINCT tenant AS v FROM tsdb_tags WHERE (" + where + ") AND tenant != '' ORDER BY v"
		queryArgs = args
	default:
		// The label name is user input: parameterize it.
		query = "SELECT DISTINCT tags[?] AS v FROM tsdb_tags WHERE mapContains(tags, ?) AND (" + where + ") ORDER BY v"

		queryArgs = append([]any{name, name}, args...)
	}

	query, queryArgs = appendLimit(query, queryArgs, hints)

	values, err := lq.queryStrings(ctx, query, queryArgs)
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse: label values for %q: %w", name, err)
	}

	return values, nil, nil
}

// LabelNames returns the label names present among the series matching matchers.
func (lq *labelQuerier) LabelNames(
	ctx context.Context,
	hints *storage.LabelHints,
	matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	where, args := lq.buildWhere(matchers)

	// __name__ and the tenant label are columns, every other label lives in the
	// tags map. The UNION makes the three respect the matchers: when no series
	// matches, all branches return nothing.
	query := "SELECT DISTINCT name FROM (" +
		"SELECT arrayJoin(mapKeys(tags)) AS name FROM tsdb_tags WHERE " + where +
		" UNION DISTINCT SELECT '__name__' AS name FROM tsdb_tags WHERE " + where + " LIMIT 1" +
		" UNION DISTINCT SELECT " + quoteString(lq.tenantLabel) + " AS name " +
		"FROM tsdb_tags WHERE (" + where + ") AND tenant != '' LIMIT 1" +
		") ORDER BY name"

	queryArgs := make([]any, 0, len(args)*3)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, args...)

	query, queryArgs = appendLimit(query, queryArgs, hints)

	names, err := lq.queryStrings(ctx, query, queryArgs)
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse: label names: %w", err)
	}

	return names, nil, nil
}

// buildWhere turns the matchers into a SQL condition and its arguments. It
// returns "1" (always true) when there is no matcher.
func (lq *labelQuerier) buildWhere(matchers []*labels.Matcher) (string, []any) {
	if len(matchers) == 0 {
		return "1", nil
	}

	conditions := make([]string, 0, len(matchers))
	args := make([]any, 0, len(matchers))

	for _, m := range matchers {
		cond, condArgs := lq.matcherCondition(m)
		conditions = append(conditions, cond)
		args = append(args, condArgs...)
	}

	return strings.Join(conditions, " AND "), args
}

// matcherCondition turns a single matcher into a SQL condition. The column
// expression and the matched value are parameterized (user input); only the
// trusted tenant label name is embedded directly.
func (lq *labelQuerier) matcherCondition(m *labels.Matcher) (string, []any) {
	var (
		expr     string
		exprArgs []any
	)

	switch m.Name {
	case labels.MetricName:
		expr = "metric_name"
	case lq.tenantLabel:
		expr = "tenant"
	default:
		expr = "tags[?]"
		exprArgs = []any{m.Name}
	}

	switch m.Type {
	case labels.MatchEqual:
		return expr + " = ?", append(exprArgs, m.Value)
	case labels.MatchNotEqual:
		return expr + " != ?", append(exprArgs, m.Value)
	case labels.MatchRegexp:
		return "match(" + expr + ", ?)", append(exprArgs, anchorRegexp(m.Value))
	case labels.MatchNotRegexp:
		return "NOT match(" + expr + ", ?)", append(exprArgs, anchorRegexp(m.Value))
	default:
		return "1", exprArgs
	}
}

func (lq *labelQuerier) queryStrings(ctx context.Context, query string, args []any) ([]string, error) {
	rows, err := lq.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var result []string

	for rows.Next() {
		var value string

		if err := rows.Scan(&value); err != nil {
			return nil, err
		}

		result = append(result, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// appendLimit adds a LIMIT clause when the hints request one.
func appendLimit(query string, args []any, hints *storage.LabelHints) (string, []any) {
	if hints != nil && hints.Limit > 0 {
		return query + " LIMIT ?", append(args, hints.Limit)
	}

	return query, args
}

// anchorRegexp anchors a Prometheus matcher regexp, which matches the full
// string, for ClickHouse match() which searches a substring.
func anchorRegexp(value string) string {
	return "^(?:" + value + ")$"
}

// quoteString renders a Go string as a ClickHouse single-quoted string literal.
func quoteString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}
