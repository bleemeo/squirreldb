package promql

import (
	"errors"
	"squirreldb/types"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

const (
	metricID1 = 42
	metricID2 = 5323
)

var (
	labelsMetric1 = []labels.Label{
		{
			Name:  "__name__",
			Value: "disk_used",
		},
		{
			Name:  "mountpath",
			Value: "/home",
		},
		{
			Name:  "__account_id",
			Value: "1234",
		},
	}
	labelsMetric2 = []labels.Label{
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
}

func (s mockStore) ReadIter(req types.MetricRequest) (types.MetricDataSet, error) {

	fakeData := make([]types.MetricData, len(req.IDs))

	for i, id := range req.IDs {
		fakeData[i].ID = id
	}

	m := &mockDataSet{
		reply: fakeData,
	}
	return m, nil
}

type mockIndex struct {
	searchReplay []types.MetricID
	lookupMap    map[types.MetricID][]labels.Label
}

func (i mockIndex) Search(matchers []*labels.Matcher) ([]types.MetricID, error) {
	return i.searchReplay, nil
}

func (i mockIndex) AllIDs() ([]types.MetricID, error) {
	return nil, errors.New("not implemented")
}

func (i mockIndex) LookupLabels(id types.MetricID) (labels.Labels, error) {
	l, ok := i.lookupMap[id]
	if ok {

		l2 := make(labels.Labels, len(l))

		for i, x := range l {
			l2[i] = labels.Label{
				Name:  x.Name,
				Value: x.Value,
			}
		}

		return l2, nil
	}
	return nil, errors.New("not found")
}

func (i mockIndex) LookupIDs(labelsList []labels.Labels) ([]types.MetricID, []int64, error) {
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
		name    string
		fields  fields
		args    args
		want    storage.SeriesSet
		wantErr bool
	}{
		{
			name: "no-sort",
			fields: fields{
				reader: mockStore{},
				index: mockIndex{
					searchReplay: []types.MetricID{metricID2, metricID1},
					lookupMap: map[types.MetricID][]labels.Label{
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
					searchReplay: []types.MetricID{metricID2, metricID1},
					lookupMap: map[types.MetricID][]labels.Label{
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
			got, _, err := q.Select(tt.args.sortSeries, tt.args.hints, tt.args.matchers...)
			if (err != nil) != tt.wantErr {
				t.Errorf("querier.Select() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !seriesLabelsEquals(got, tt.want, t) {
				return
			}
		})
	}
}

func seriesLabelsEquals(a, b storage.SeriesSet, t *testing.T) bool {
	n := 0
	for {
		n++
		aNext := a.Next()
		bNext := b.Next()

		if aNext != bNext {
			t.Errorf("at index %d: aNext = %v != %v = bNext", n, aNext, bNext)
			return false
		}

		if !aNext {
			break
		}

		aSerie := a.At()
		bSerie := b.At()

		if labels.Compare(aSerie.Labels(), bSerie.Labels()) != 0 {
			t.Errorf("at index %d: aLabels = %v != %v = bLabels", n, aSerie.Labels(), bSerie.Labels())
			return false
		}
	}

	return true
}
