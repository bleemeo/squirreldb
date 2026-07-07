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

package requestscoped

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/appendable/requestappender"
	"github.com/bleemeo/squirreldb/queryable/limitqueryable"
	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type contextKey struct{}

type contextValue struct {
	httpRequest        *http.Request
	operationName      string
	store              *Store
	Tenant             string
	EnableDebug        bool
	EnableVerboseDebug bool
	TimeToLive         int64
	BackendOptions     any

	l               sync.Mutex
	queryableOption *queryableOption
	queryable       types.QueryableWithOptions
	queryableErr    error
}

func (ctxValue *contextValue) fillQueryableOption() {
	ctxValue.l.Lock()
	defer ctxValue.l.Unlock()

	if ctxValue.queryableOption != nil {
		return
	}

	r := ctxValue.httpRequest
	result := queryableOption{}
	ctxValue.queryableOption = &result

	result.MaxEvaluatedSeries = ctxValue.store.DefaultMaxEvaluatedSeries

	maxEvaluatedSeriesText := r.Header.Get(types.HeaderMaxEvaluatedSeries)
	if maxEvaluatedSeriesText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedSeriesText, 10, 32)
		if err != nil {
			result.Error = err

			return
		}

		result.MaxEvaluatedSeries = uint32(tmp)
	}

	result.MaxEvaluatedPoints = ctxValue.store.DefaultMaxEvaluatedPoints

	maxEvaluatedPointsText := r.Header.Get(types.HeaderMaxEvaluatedPoints)
	if maxEvaluatedPointsText != "" {
		tmp, err := strconv.ParseUint(maxEvaluatedPointsText, 10, 64)
		if err != nil {
			result.Error = err

			return
		}

		result.MaxEvaluatedPoints = tmp
	}

	value := r.Header.Get(types.HeaderForcedMatcher)
	if value != "" {
		part := strings.SplitN(value, "=", 2)
		if len(part) != 2 {
			result.Error = fmt.Errorf("%w: \"%s\", require labelName=labelValue", requestappender.ErrInvalidLabel, value)

			return
		}

		result.ForcedMatcher, result.Error = labels.NewMatcher(
			labels.MatchEqual,
			part[0],
			part[1],
		)
	}
}

func (ctxValue *contextValue) debugLevel() types.DebugLevel {
	switch {
	case ctxValue.EnableVerboseDebug:
		return types.DebugLevelVerbose
	case ctxValue.EnableDebug:
		return types.DebugLevelSimple
	default:
		return types.DebugLevelNone
	}
}

func (ctxValue *contextValue) getAppenderOptions() types.AppenderOptions {
	return types.AppenderOptions{
		Tenant:         ctxValue.Tenant,
		RequestDebug:   ctxValue.debugLevel(),
		WriteTTL:       ctxValue.TimeToLive,
		BackendOptions: ctxValue.BackendOptions,
	}
}

func (ctxValue *contextValue) getQuerierOptions() types.QuerierOptions {
	return types.QuerierOptions{
		Tenant:         ctxValue.Tenant,
		RequestDebug:   ctxValue.debugLevel(),
		BackendOptions: ctxValue.BackendOptions,
	}
}

func (ctxValue *contextValue) makeQueryable() (types.QueryableWithOptions, error) {
	// Those min and max date must be narrowed than min/max from Cassandra index.
	var (
		minTime = time.Unix(0, 0).UTC()
		maxTime = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	)

	// fillQueryableOption must have been caller before makeQueryable() is called
	if ctxValue.queryableOption == nil {
		return nil, fmt.Errorf("%w: fillQueryableOption not called", ErrAssertion)
	}

	if ctxValue.queryableOption.Error != nil {
		return nil, ctxValue.queryableOption.Error
	}

	limitOption := limitqueryable.Options{
		MinTime:            minTime,
		MaxTime:            maxTime,
		MaxEvaluatedSeries: ctxValue.queryableOption.MaxEvaluatedSeries,
		MaxEvaluatedPoints: ctxValue.queryableOption.MaxEvaluatedPoints,
	}

	if ctxValue.queryableOption.ForcedMatcher != nil {
		limitOption.ForcedMatchers = append(limitOption.ForcedMatchers, ctxValue.queryableOption.ForcedMatcher)
	}

	if ctxValue.store.TenantLabelName != "" && ctxValue.Tenant != "" {
		limitOption.ForcedMatchers = append(limitOption.ForcedMatchers, labels.MustNewMatcher(
			labels.MatchEqual,
			ctxValue.store.TenantLabelName,
			ctxValue.Tenant,
		))
	} else if ctxValue.store.RequireTenantHeader {
		return nil, ErrMissingTenantHeader
	}

	return limitqueryable.NewLimiterQueryable(ctxValue.store.Queryable, limitOption), nil
}

func (ctxValue *contextValue) makeQuerier(mint int64, maxt int64) (storage.Querier, error) {
	ctxValue.fillQueryableOption()

	ctxValue.l.Lock()
	defer ctxValue.l.Unlock()

	if ctxValue.queryable == nil && ctxValue.queryableErr == nil {
		ctxValue.queryable, ctxValue.queryableErr = ctxValue.makeQueryable()
	}

	if ctxValue.queryableErr != nil {
		return nil, ctxValue.queryableErr
	}

	return ctxValue.queryable.QuerierWithOptions(ctxValue.getQuerierOptions(), mint, maxt)
}

func (ctxValue *contextValue) makeChunkQuerier(mint int64, maxt int64) (storage.ChunkQuerier, error) {
	ctxValue.fillQueryableOption()

	ctxValue.l.Lock()
	defer ctxValue.l.Unlock()

	if ctxValue.queryable == nil && ctxValue.queryableErr == nil {
		ctxValue.queryable, ctxValue.queryableErr = ctxValue.makeQueryable()
	}

	if ctxValue.queryableErr != nil {
		return nil, ctxValue.queryableErr
	}

	return ctxValue.queryable.ChunkQuerierWithOptions(ctxValue.getQuerierOptions(), mint, maxt)
}

type queryableOption struct {
	Error              error
	MaxEvaluatedSeries uint32
	MaxEvaluatedPoints uint64
	ForcedMatcher      *labels.Matcher
}
