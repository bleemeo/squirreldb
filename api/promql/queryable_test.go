// Copyright 2015-2025 Bleemeo
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

package promql

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

const (
	metricID1  = 42
	metricID2  = 5323
	metricID3  = 858
	metricID4  = 123
	metricID4b = 456
)

//nolint:gochecknoglobals
var (
	labelsMetric1 = labels.Labels{
		{
			Name:  "__account_id",
			Value: "1234",
		},
		{
			Name:  "__name__",
			Value: "disk_used",
		},
		{
			Name:  "mountpath",
			Value: "/home",
		},
	}

	labelsMetric2 = labels.Labels{
		{
			Name:  "__name__",
			Value: "disk_used",
		},
		{
			Name:  "mountpath",
			Value: "/srv",
		},
		{
			Name:  "__account_id",
			Value: "5678",
		},
	}

	labelsMetric3 = labels.Labels{
		{
			Name:  "__name__",
			Value: "disk_free",
		},
		{
			Name:  "__account_id",
			Value: "5678",
		},
		{
			Name:  "mountpath",
			Value: "/srv",
		},
	}
)

type mockStore struct {
	l               sync.Mutex
	pointsPerSeries int
	readRequest     []types.MetricRequest
}

func (s *mockStore) ReadIter(_ context.Context, req types.MetricRequest) (types.MetricDataSet, error) {
	s.l.Lock()
	s.readRequest = append(s.readRequest, req)
	s.l.Unlock()

	fakeData := make([]types.MetricData, len(req.IDs))

	for i, id := range req.IDs {
		fakeData[i].ID = id
		fakeData[i].TimeToLive = 42
		fakeData[i].Points = make([]types.MetricPoint, s.pointsPerSeries)

		for j := range fakeData[i].Points {
			fakeData[i].Points[j].Timestamp = int64(i * j)
			fakeData[i].Points[j].Value = float64(i * j)
		}
	}

	return types.MetricIterFromList(fakeData), nil
}

func (s *mockStore) PointsRead() float64 {
	return 0
}

func (s *mockStore) PointsCached() float64 {
	return 0
}

type mockStoreFixedResponse struct {
	response []types.MetricData
}

func (s *mockStoreFixedResponse) ReadIter(_ context.Context, _ types.MetricRequest) (types.MetricDataSet, error) {
	return types.MetricIterFromList(s.response), nil
}

type mockIndex struct {
	lookupMap   map[types.MetricID]labels.Labels
	searchReply []types.MetricLabel
}

func (idx mockIndex) Search(
	_ context.Context,
	_, _ time.Time,
	_ []*labels.Matcher,
) (types.MetricsSet, error) {
	return &dummy.MetricsLabel{List: idx.searchReply}, nil
}

func (idx mockIndex) AllIDs(_ context.Context, _ time.Time, _ time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx mockIndex) LookupIDs(_ context.Context, _ []types.LookupRequest) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
}

func (idx mockIndex) LabelNames(
	_ context.Context,
	_, _ time.Time,
	_ []*labels.Matcher,
) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (idx mockIndex) LabelValues(
	_ context.Context,
	_, _ time.Time,
	_ string, _ []*labels.Matcher,
) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (idx mockIndex) SeriesReturned() float64 {
	return 0
}

type mockSeries struct {
	reply   []series
	current series
	offset  int
}

func (s *mockSeries) Next() bool {
	if s.offset >= len(s.reply) {
		return false
	}

	s.current = s.reply[s.offset]
	s.offset++

	return true
}

func (s *mockSeries) At() storage.Series {
	return s.current
}

func (s *mockSeries) Err() error {
	return nil
}

func (s *mockSeries) Warnings() annotations.Annotations {
	return nil
}

func Test_querier_Select(t *testing.T) {
	type fields struct {
		index  IndexWithStats
		reader MetricReaderWithStats
		mint   int64
		maxt   int64
	}

	type args struct {
		hints      *storage.SelectHints
		matchers   []*labels.Matcher
		sortSeries bool
	}

	tests := []struct {
		want   storage.SeriesSet
		name   string
		fields fields
		args   args
	}{
		{
			name: "no-sort",
			fields: fields{
				reader: &mockStore{},
				index: mockIndex{
					searchReply: []types.MetricLabel{
						{ID: metricID2, Labels: labelsMetric2},
						{ID: metricID1, Labels: labelsMetric1},
					},
					lookupMap: map[types.MetricID]labels.Labels{
						metricID1: labelsMetric1,
						metricID2: labelsMetric2,
					},
				},
				mint: 1000,
				maxt: 2000,
			},
			args: args{
				sortSeries: false,
				hints:      nil,
				matchers:   nil,
			},
			want: &mockSeries{
				reply: []series{
					{labels: labelsMetric2},
					{labels: labelsMetric1},
				},
			},
		},
		{
			name: "sort",
			fields: fields{
				reader: &mockStore{},
				index: mockIndex{
					searchReply: []types.MetricLabel{
						{ID: metricID2, Labels: labelsMetric2},
						{ID: metricID1, Labels: labelsMetric1},
					},
					lookupMap: map[types.MetricID]labels.Labels{
						metricID1: labelsMetric1,
						metricID2: labelsMetric2,
					},
				},
				mint: 1000,
				maxt: 2000,
			},
			args: args{
				sortSeries: true,
				hints:      nil,
				matchers:   nil,
			},
			want: &mockSeries{
				reply: []series{
					{labels: labelsMetric1},
					{labels: labelsMetric2},
				},
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Store{
				Index:  tt.fields.index,
				Reader: tt.fields.reader,
			}
			q := s.newQuerier(tt.fields.mint, tt.fields.maxt)

			ctx, err := s.ContextFromRequest(req)
			if err != nil {
				t.Fatal("Failed to parse request:", err)
			}

			got := q.Select(ctx, tt.args.sortSeries, tt.args.hints, tt.args.matchers...)
			if !seriesLabelsEquals(t, got, tt.want) {
				return
			}
		})
	}
}

func seriesLabelsEquals(t *testing.T, a, b storage.SeriesSet) bool {
	t.Helper()

	n := 0
	good := true

	for {
		n++

		aNext := a.Next()
		bNext := b.Next()

		if aNext != bNext {
			t.Errorf("at index %d: aNext = %v != %v = bNext", n, aNext, bNext)

			good = false

			break
		}

		if !aNext {
			break
		}

		aSerie := a.At()
		bSerie := b.At()

		if labels.Compare(aSerie.Labels(), bSerie.Labels()) != 0 {
			t.Errorf("at index %d: aLabels = %v != %v = bLabels", n, aSerie.Labels(), bSerie.Labels())

			good = false

			break
		}
	}

	if err := a.Err(); err != nil {
		t.Error(err)
	}

	if err := b.Err(); err != nil {
		t.Error(err)
	}

	return good
}
