package promql

import (
	"context"
	"errors"
	"squirreldb/types"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

const (
	metricID1 = 42
	metricID2 = 5323
)

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

type mockDataSet struct {
	reply   []types.MetricData
	current types.MetricData
	offset  int
}

func (d *mockDataSet) Next() bool {
	if d.offset >= len(d.reply) {
		return false
	}

	d.current = d.reply[d.offset]
	d.offset++

	return true
}

func (d *mockDataSet) At() types.MetricData {
	return d.current
}

func (d *mockDataSet) Err() error {
	return nil
}

type mockStore struct {
	pointsPerSeries int
}

func (s mockStore) ReadIter(ctx context.Context, req types.MetricRequest) (types.MetricDataSet, error) {

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

	m := &mockDataSet{
		reply: fakeData,
	}
	return m, nil
}

type mockIndex struct {
	searchReplay []types.MetricLabel
	lookupMap    map[types.MetricID]labels.Labels
}

func (idx mockIndex) Search(start time.Time, end time.Time, matchers []*labels.Matcher) ([]types.MetricLabel, error) {
	return idx.searchReplay, nil
}

func (idx mockIndex) AllIDs(start time.Time, end time.Time) ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (idx mockIndex) lookupLabels(ids []types.MetricID) ([]labels.Labels, error) {
	results := make([]labels.Labels, len(ids))

	for i, id := range ids {
		l, ok := idx.lookupMap[id]
		if ok {
			results[i] = l.Copy()
		} else {
			return nil, errors.New("not found")
		}
	}

	return results, nil
}

func (idx mockIndex) LookupIDs(ctx context.Context, request []types.LookupRequest) ([]types.MetricID, []int64, error) {
	return nil, nil, errors.New("not implemented")
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

func (s *mockSeries) Warnings() storage.Warnings {
	return nil
}

func Test_querier_Select(t *testing.T) {
	type fields struct {
		index  types.Index
		reader types.MetricReader
		mint   int64
		maxt   int64
	}
	type args struct {
		sortSeries bool
		hints      *storage.SelectHints
		matchers   []*labels.Matcher
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   storage.SeriesSet
	}{
		{
			name: "no-sort",
			fields: fields{
				reader: mockStore{},
				index: mockIndex{
					searchReplay: []types.MetricLabel{
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
				reader: mockStore{},
				index: mockIndex{
					searchReplay: []types.MetricLabel{
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := querier{
				index:  tt.fields.index,
				reader: tt.fields.reader,
				mint:   tt.fields.mint,
				maxt:   tt.fields.maxt,
			}
			got := q.Select(tt.args.sortSeries, tt.args.hints, tt.args.matchers...)
			if !seriesLabelsEquals(got, tt.want, t) {
				return
			}
		})
	}
}

func seriesLabelsEquals(a, b storage.SeriesSet, t *testing.T) bool {
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
