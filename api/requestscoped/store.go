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

// Package requestscoped implements features that rely on HTTP request headers, like applying tenant limitation,
// per-request limit of evaluated points/series, ...
package requestscoped

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/bleemeo/squirreldb/api/auth"
	"github.com/bleemeo/squirreldb/appendable/fixedappender"
	"github.com/bleemeo/squirreldb/appendable/requestappender"
	"github.com/bleemeo/squirreldb/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// NewStore return a store (Appendable + Queryable) that handle per HTTP request configuration
// and limits (tenant labels, maximum evaluated points/series, maximum concurrency, ...).
func NewStore(options Options) *Store {
	return &Store{Options: options, metrics: newStoreMetrics(options.MetricRegistry)}
}

var (
	ErrParseTTLHeader      = errors.New("can't parse time to live header")
	ErrMissingTenantHeader = errors.New("the tenant header is missing")
	ErrInconsistentContext = errors.New("assertion failed, different context used in same Querier")
	ErrAssertion           = errors.New("assertion error")
	ErrNoContextValue      = errors.New("assertion error: missing call to ContextWithRequest")
)

type Options struct {
	DefaultMaxEvaluatedSeries uint32
	DefaultMaxEvaluatedPoints uint64
	DefaultTTL                int64
	TenantLabelName           string
	RequireTenantHeader       bool
	Queryable                 types.QueryableWithOptions
	Appendable                types.AppendableWithOptions
	MutableLabelDetector      MutableLabelDetector
	OptionsParser             types.HTTPOptionsParser
	// BackendResolver (optional) checks, when the request starts, that the
	// backend the request is routed to is valid, so an invalid backend header is
	// rejected before any read or write.
	BackendResolver BackendResolver
	MetricRegistry  prometheus.Registerer
}

// BackendResolver resolves the backend serving a request from its tenant and the
// backend it requested (HeaderBackend, empty when absent). It returns an error
// wrapping types.ErrBackendNotSelectable when the requested backend isn't enabled.
type BackendResolver interface {
	ResolveBackend(ctx context.Context, tenant, requested string) (string, error)
}

type MutableLabelDetector interface {
	IsMutableLabel(ctx context.Context, tenant, name string) (bool, error)
}

type Store struct {
	Options

	metrics  *storeMetrics
	l        sync.Mutex
	debugAll bool
}

func (store *Store) ToggleDebug() bool {
	store.l.Lock()
	defer store.l.Unlock()

	store.debugAll = !store.debugAll

	return store.debugAll
}

func (store *Store) DebugAll() bool {
	store.l.Lock()
	defer store.l.Unlock()

	return store.debugAll
}

func (store *Store) Querier(mint, maxt int64) (storage.Querier, error) {
	return &pendingQuerier{
		mint: mint,
		maxt: maxt,
		q:    nil,
	}, nil
}

func (store *Store) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return &pendingChunkQuerier{
		mint: mint,
		maxt: maxt,
		q:    nil,
	}, nil
}

func (store *Store) Appender(ctx context.Context) storage.Appender {
	ctxValue := getContextValue(ctx)
	if ctxValue == nil {
		return fixedappender.NewErrorAppender(ErrNoContextValue)
	}

	options := requestappender.Options{
		IngestedPoints: store.metrics.IngestedPoints,
		CommitSeconds:  store.metrics.WriteCommitSeconds,
	}

	if store.TenantLabelName != "" && ctxValue.Tenant != "" {
		options.TenantLabel = labels.Label{Name: store.TenantLabelName, Value: ctxValue.Tenant}
	} else if store.RequireTenantHeader {
		return fixedappender.NewErrorAppender(ErrMissingTenantHeader)
	}

	if store.MutableLabelDetector != nil {
		options.IgnoreLabelCallback = store.MutableLabelDetector.IsMutableLabel
	}

	inner := store.Appendable.AppenderWithOptions(ctx, ctxValue.getAppenderOptions())

	return requestappender.NewAppender(ctx, inner, options)
}

// ContextWithRequest add to the context the necessary value for Store to work.
// Any context provided to a function of the Store (Querier().Select(), Appender(), ...) must
// be a context from ContextWithRequest().
func (store *Store) ContextWithRequest(
	ctx context.Context,
	r *http.Request,
	operationName string,
) (context.Context, types.DebugLevel, error) {
	ctxValue, err := store.newCtxValue(r, operationName)
	if err != nil {
		return ctx, types.DebugLevelNone, err
	}

	return context.WithValue(ctx, contextKey{}, ctxValue), ctxValue.debugLevel(), nil
}

func (store *Store) newCtxValue(r *http.Request, operationName string) (*contextValue, error) {
	var err error

	result := &contextValue{
		store:         store,
		operationName: operationName,
		httpRequest:   r,
	}

	// The tenant header makes queries match only metrics from this tenant.
	result.Tenant = r.Header.Get(types.HeaderTenant)

	// A user bound to a tenant always uses its tenant. The auth middleware already
	// rejected the requests with a different tenant header.
	result.identity = auth.IdentityFromContext(r.Context())
	if result.identity != nil && result.identity.Tenant != "" {
		result.Tenant = result.identity.Tenant
	}

	// The backend header selects the backend serving the request. The routing
	// itself is done by the queryable/appendable from the querier/appender
	// options; it's only validated here to reject the request early. It's
	// resolved with the tenant of the user, so the tenant rules apply to it.
	result.Backend = r.Header.Get(types.HeaderBackend)

	if store.BackendResolver != nil {
		if _, err := store.BackendResolver.ResolveBackend(r.Context(), result.Tenant, result.Backend); err != nil {
			return result, err
		}
	}

	result.EnableDebug = r.Header.Get(types.HeaderQueryDebug) != "" || store.DebugAll()
	result.EnableVerboseDebug = r.Header.Get(types.HeaderQueryVerboseDebug) != ""

	if result.EnableVerboseDebug {
		result.EnableDebug = true
	}

	result.TimeToLive = store.DefaultTTL

	ttlRaw := r.Header.Get(types.HeaderTimeToLive)
	if ttlRaw != "" {
		result.TimeToLive, err = strconv.ParseInt(ttlRaw, 10, 64)
		if err != nil {
			return result, fmt.Errorf("%w: %s", ErrParseTTLHeader, err.Error())
		}
	}

	// optimization: don't parse option only used for query when doing a write operation
	if operationName != "write" {
		result.fillQueryableOption()

		if result.queryableOption.Error != nil {
			return result, result.queryableOption.Error
		}
	}

	if store.OptionsParser != nil {
		result.BackendOptions, err = store.OptionsParser.ParseHTTPOptions(r.Header)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func getContextValue(ctx context.Context) *contextValue {
	v, _ := ctx.Value(contextKey{}).(*contextValue)

	return v
}
