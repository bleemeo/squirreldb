package api

import (
	"context"
	"errors"
	"net/url"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

var errNotImplemented = errors.New("not implemented")

// mockExemplarQueryable implements storage.ExemplarQueryable.
type mockExemplarQueryable struct{}

func (mockExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return mockExemplarQuerier{}, errNotImplemented
}

type mockExemplarQuerier struct{}

func (mockExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	return nil, errNotImplemented
}

// mockTargetRetriever implements v1.TargetRetriever.
type mockTargetRetriever struct{}

func (mockTargetRetriever) TargetsActive() map[string][]*scrape.Target { return nil }

func (mockTargetRetriever) TargetsDropped() map[string][]*scrape.Target { return nil }

// mockAlertmanagerRetriever implements v1.AlertmanagerRetriever.
type mockAlertmanagerRetriever struct{}

func (mockAlertmanagerRetriever) Alertmanagers() []*url.URL { return nil }

func (mockAlertmanagerRetriever) DroppedAlertmanagers() []*url.URL { return nil }

// mockTSDBAdminStat implements v1.TSDBAdminStats.
type mockTSDBAdminStat struct{}

func (mockTSDBAdminStat) CleanTombstones() error { return errNotImplemented }

func (mockTSDBAdminStat) Delete(mint, maxt int64, ms ...*labels.Matcher) error {
	return errNotImplemented
}

func (mockTSDBAdminStat) Snapshot(dir string, withHead bool) error { return errNotImplemented }

func (mockTSDBAdminStat) Stats(statsByLabelName string) (*tsdb.Stats, error) {
	return nil, errNotImplemented
}

func (mockTSDBAdminStat) WALReplayStatus() (tsdb.WALReplayStatus, error) {
	return tsdb.WALReplayStatus{}, errNotImplemented
}

// mockRulesRetriever implements v1.RulesRetriever.
type mockRulesRetriever struct{}

func (mockRulesRetriever) RuleGroups() []*rules.Group { return nil }

func (mockRulesRetriever) AlertingRules() []*rules.AlertingRule { return nil }

// mockGatherer implements prometheus.Gatherer.
type mockGatherer struct{}

func (mockGatherer) Gather() ([]*dto.MetricFamily, error) { return nil, errNotImplemented }
