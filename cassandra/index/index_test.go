package index

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"squirreldb/types"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	MetricIDTest1 = 1 + iota
	MetricIDTest2
	MetricIDTest3
	MetricIDTest4
	MetricIDTest5
	MetricIDTest6
	MetricIDTest7
	MetricIDTest8
	MetricIDTest9
	MetricIDTest10
	MetricIDTest11
	MetricIDTest12
	MetricIDTest13
	MetricIDTest14
	MetricIDTest15
)

type mockState struct {
	values map[string]string
}

func (m mockState) Read(name string, output interface{}) (bool, error) {
	result, ok := m.values[name]
	if !ok {
		return false, nil
	}

	outputStr, ok := output.(*string)
	if !ok {
		return false, errors.New("only string supported")
	}

	*outputStr = result
	return true, nil
}

func (m *mockState) Write(name string, value interface{}) error {
	valueStr, ok := value.(string)
	if !ok {
		return errors.New("only string supported")
	}

	if m.values == nil {
		m.values = make(map[string]string)
	}

	m.values[name] = valueStr
	return nil
}

type mockLockFactory struct {
	mutex sync.Mutex

	locks map[string]*mockLock
}

func (f *mockLockFactory) CreateLock(name string, ttl time.Duration) types.TryLocker {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.locks == nil {
		f.locks = make(map[string]*mockLock)
	}

	l, ok := f.locks[name]
	if !ok {
		l = &mockLock{}
		f.locks[name] = l
	}

	return l
}

type mockLock struct {
	mutex    sync.Mutex
	acquired bool
}

func (l *mockLock) Lock() {
	l.TryLock(context.Background(), 10*time.Second)
}

func (l *mockLock) Unlock() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.acquired {
		panic("unlock of unlocked mutex")
	}

	l.acquired = false
}

func (l *mockLock) tryLock() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.acquired {
		return false
	}

	l.acquired = true
	return true
}

func (l *mockLock) TryLock(ctx context.Context, retryDelay time.Duration) bool {
	for {
		ok := l.tryLock()
		if ok {
			return true
		}

		if retryDelay == 0 {
			return false
		}

		select {
		case <-time.After(retryDelay):
		case <-ctx.Done():
			return false
		}
	}
}

type mockStore struct {
	mutex sync.Mutex

	labels2id     map[string]types.MetricID
	postings      map[string]map[string][]byte
	id2labels     map[types.MetricID]labels.Labels
	id2expiration map[types.MetricID]time.Time
	expiration    map[time.Time][]byte
}

func (s *mockStore) Init() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.labels2id = make(map[string]types.MetricID)
	s.postings = make(map[string]map[string][]byte)
	s.id2labels = make(map[types.MetricID]labels.Labels)
	s.id2expiration = make(map[types.MetricID]time.Time)
	s.expiration = make(map[time.Time][]byte)
	return nil
}

func (s *mockStore) SelectLabels2ID(sortedLabelsString string) (types.MetricID, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	result, ok := s.labels2id[sortedLabelsString]
	if !ok {
		return 0, gocql.ErrNotFound
	}

	return result, nil
}

func (s *mockStore) SelectID2Labels(id types.MetricID) (labels.Labels, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	result, ok := s.id2labels[id]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	return result, nil
}

func (s *mockStore) SelectExpiration(day time.Time) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	result, ok := s.expiration[day]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	return result, nil
}

func (s *mockStore) SelectID2LabelsExpiration(id types.MetricID) (time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	result, ok := s.id2expiration[id]
	if !ok {
		return time.Time{}, gocql.ErrNotFound
	}

	return result, nil
}

type mockByteIter struct {
	err     error
	results [][]byte
	next    []byte
	idx     int
}

func (i *mockByteIter) HasNext() bool {
	if i.err != nil {
		return false
	}
	if i.idx < len(i.results) {
		i.next = i.results[i.idx]
		i.idx++
		return true
	}
	return false
}

func (i *mockByteIter) Next() []byte {
	if i.next == nil {
		panic("This shouldn't happen. Probably HasNext() were not called")
	}

	r := i.next

	// We do this to allow checking that HasNext()/Next() are always called thogether
	i.next = nil

	return r
}

func (i mockByteIter) Err() error {
	return i.err
}

func (s *mockStore) SelectPostingByName(name string) bytesIter {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	m, ok := s.postings[name]
	if !ok {
		return &mockByteIter{
			err: gocql.ErrNotFound,
		}
	}

	results := make([][]byte, 0, len(m))
	for _, v := range m {
		results = append(results, v)
	}

	return &mockByteIter{
		err:     nil,
		results: results,
		idx:     0,
	}
}

func (s *mockStore) SelectPostingByNameValue(name string, value string) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	m, ok := s.postings[name]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	result, ok := m[value]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	return result, nil
}

func (s *mockStore) SelectValueForName(name string) ([]string, [][]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	m, ok := s.postings[name]
	if !ok {
		return nil, nil, gocql.ErrNotFound
	}

	values := make([]string, 0, len(m))
	buffers := make([][]byte, 0, len(m))

	for k, v := range m {
		values = append(values, k)
		buffers = append(buffers, v)
	}

	return values, buffers, nil
}

func (s *mockStore) InsertPostings(name string, value string, bitset []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	m, ok := s.postings[name]
	if !ok {
		m = make(map[string][]byte)
		s.postings[name] = m
	}

	m[value] = bitset

	return nil
}

func (s *mockStore) InsertID2Labels(id types.MetricID, sortedLabels labels.Labels, expiration time.Time) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.id2labels[id] = sortedLabels
	s.id2expiration[id] = expiration

	return nil
}

func (s *mockStore) InsertLabels2ID(sortedLabelsString string, id types.MetricID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.labels2id[sortedLabelsString] = id

	return nil
}

func (s *mockStore) InsertExpiration(day time.Time, bitset []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.expiration[day] = bitset

	return nil
}

func (s *mockStore) UpdateID2LabelsExpiration(id types.MetricID, expiration time.Time) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.id2expiration[id] = expiration

	return nil
}

func (s *mockStore) DeleteLabels2ID(sortedLabelsString string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, ok := s.labels2id[sortedLabelsString]
	if !ok {
		return gocql.ErrNotFound
	}

	delete(s.labels2id, sortedLabelsString)

	return nil
}

func (s *mockStore) DeleteID2Labels(id types.MetricID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, ok := s.id2labels[id]
	_, ok2 := s.id2expiration[id]
	if !ok && !ok2 {
		return gocql.ErrNotFound
	}

	delete(s.id2labels, id)
	delete(s.id2expiration, id)

	return nil
}

func (s *mockStore) DeleteExpiration(day time.Time) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, ok := s.expiration[day]
	if !ok {
		return gocql.ErrNotFound
	}

	delete(s.expiration, day)

	return nil
}

func (s *mockStore) DeletePostings(name string, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	m, ok := s.postings[name]
	if !ok {
		return gocql.ErrNotFound
	}

	if _, ok = m[value]; !ok {
		return gocql.ErrNotFound
	}

	delete(m, value)
	if len(m) == 0 {
		delete(s.postings, name)
	}

	return nil
}

func labelsMapToList(m map[string]string, dropSpecialLabel bool) labels.Labels {
	results := make(labels.Labels, 0, len(m))

	for k, v := range m {
		if dropSpecialLabel && (k == timeToLiveLabelName || k == idLabelName) {
			continue
		}
		results = append(results, labels.Label{
			Name:  k,
			Value: v,
		})
	}

	sort.Sort(results)
	return results
}

func mockIndexFromMetrics(metrics map[types.MetricID]map[string]string) *CassandraIndex {
	index, err := new(&mockStore{}, Options{
		DefaultTimeToLive: 1 * time.Hour,
		LockFactory:       &mockLockFactory{},
	})
	if err != nil {
		panic(err)
	}

	for id, labels := range metrics {
		sortedLabels := labelsMapToList(labels, true)
		sortedLabelsString := sortedLabels.String()
		savedIDs, err := index.createMetrics([]createMetricRequest{
			{
				newID:               uint64(id),
				sortedLabelsString:  sortedLabelsString,
				sortedLabels:        sortedLabels,
				cassandraExpiration: time.Now().Add(time.Hour),
			},
		})

		if err != nil {
			panic(err)
		}

		if savedIDs[0] != id {
			panic(fmt.Sprintf("savedIDs=%v didn't match requested id=%v", savedIDs, id))
		}
	}

	return index
}

func Benchmark_keyFromLabels(b *testing.B) {
	tests := []struct {
		name   string
		labels labels.Labels
	}{
		{
			name: "simple",
			labels: labels.Labels{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: labels.Labels{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: labels.Labels{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
				{Name: "label3", Value: "value3"},
				{Name: "label4", Value: "value4"},
				{Name: "label5", Value: "value5"},
				{Name: "label6", Value: "value6"},
				{Name: "label7", Value: "value7"},
				{Name: "label8", Value: "value8"},
				{Name: "label9", Value: "value9"},
				{Name: "label0", Value: "value0"},
			},
		},
		{
			name: "five-longer-labels",
			labels: labels.Labels{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: labels.Labels{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = tt.labels.Hash()
			}
		})
	}
}

func Benchmark_labelsToID(b *testing.B) {
	tests := []struct {
		name   string
		labels labels.Labels
	}{
		{
			name: "simple",
			labels: labels.Labels{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: labels.Labels{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: labels.Labels{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
				{Name: "label3", Value: "value3"},
				{Name: "label4", Value: "value4"},
				{Name: "label5", Value: "value5"},
				{Name: "label6", Value: "value6"},
				{Name: "label7", Value: "value7"},
				{Name: "label8", Value: "value8"},
				{Name: "label9", Value: "value9"},
				{Name: "label0", Value: "value0"},
			},
		},
		{
			name: "five-longer-labels",
			labels: labels.Labels{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: labels.Labels{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		c := CassandraIndex{
			labelsToID: make(map[uint64][]idData),
		}
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				key := tt.labels.Hash()
				idData, _ := c.getIDData(key, tt.labels)
				c.setIDData(key, idData)
			}
		})
	}
}

func Test_timeToLiveFromLabels(t *testing.T) {
	tests := []struct {
		name       string
		labels     labels.Labels
		want       int64
		wantLabels labels.Labels
	}{
		{
			name: "no ttl",
			labels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
			want: 0,
			wantLabels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
				{Name: "__ttl__", Value: "3600"},
			},
			want: 3600,
			wantLabels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl2",
			labels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
			want: 3600,
			wantLabels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := timeToLiveFromLabels(&tt.labels)
			if got != tt.want {
				t.Errorf("timeToLiveFromLabels() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.labels, tt.wantLabels) {
				t.Errorf("timeToLiveFromLabels() labels = %v, want %v", tt.labels, tt.wantLabels)
			}
		})
	}
}

func Benchmark_timeToLiveFromLabels(b *testing.B) {
	tests := []struct {
		name       string
		labels     labels.Labels
		wantTTL    int64
		wantLabels labels.Labels
		wantErr    bool
	}{
		{
			name: "no ttl",
			labels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: labels.Labels{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "12 labels no ttl",
			labels: labels.Labels{
				{Name: "job", Value: "scrape"},
				{Name: "__name__", Value: "up"},
				{Name: "labels1", Value: "value1"},
				{Name: "labels2", Value: "value2"},
				{Name: "labels3", Value: "value3"},
				{Name: "labels4", Value: "value4"},
				{Name: "labels5", Value: "value5"},
				{Name: "labels6", Value: "value6"},
				{Name: "labels7", Value: "value7"},
				{Name: "labels8", Value: "value8"},
				{Name: "labels9", Value: "value9"},
				{Name: "labels10", Value: "value10"},
			},
		},
		{
			name: "12 labels ttl",
			labels: labels.Labels{
				{Name: "job", Value: "scrape"},
				{Name: "__name__", Value: "up"},
				{Name: "labels1", Value: "value1"},
				{Name: "labels2", Value: "value2"},
				{Name: "labels3", Value: "value3"},
				{Name: "labels4", Value: "value4"},
				{Name: "labels5", Value: "value5"},
				{Name: "labels6", Value: "value6"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "labels7", Value: "value7"},
				{Name: "labels8", Value: "value8"},
				{Name: "labels9", Value: "value9"},
				{Name: "labels10", Value: "value10"},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				labelsIn := make(labels.Labels, len(tt.labels))
				copy(labelsIn, tt.labels)
				_ = timeToLiveFromLabels(&labelsIn)
			}
		})
	}
}

func Test_getMatchersValue(t *testing.T) {
	type args struct {
		matchers []*labels.Matcher
		name     string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 bool
	}{
		{
			name: "contains",
			args: args{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						"__name__",
						"up",
					),
					labels.MustNewMatcher(
						labels.MatchEqual,
						"monitor",
						"codelab",
					),
				},
				name: "monitor",
			},
			want:  "codelab",
			want1: true,
		},
		{
			name: "contains_empty_value",
			args: args{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						"__name__",
						"down",
					),
					labels.MustNewMatcher(
						labels.MatchEqual,
						"job",
						"",
					),
				},
				name: "job",
			},
			want:  "",
			want1: true,
		},
		{
			name: "no_contains",
			args: args{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(
						labels.MatchEqual,
						"__name__",
						"up",
					),
				},
				name: "monitor",
			},
			want:  "",
			want1: false,
		},
		{
			name: "labels_empty",
			args: args{
				matchers: []*labels.Matcher{},
				name:     "monitor",
			},
			want:  "",
			want1: false,
		},
		{
			name: "labels_nil",
			args: args{
				matchers: nil,
				name:     "monitor",
			},
			want:  "",
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getMatchersValue(tt.args.matchers, tt.args.name)
			if got != tt.want {
				t.Errorf("GetMatchersValue() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetMatchersValue() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_sortLabels(t *testing.T) {
	type args struct {
		labels labels.Labels
	}
	tests := []struct {
		name string
		args args
		want labels.Labels
	}{
		{
			name: "sorted",
			args: args{
				labels: labels.Labels{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "monitor",
						Value: "codelab",
					},
				},
			},
			want: labels.Labels{
				{
					Name:  "__name__",
					Value: "up",
				},
				{
					Name:  "monitor",
					Value: "codelab",
				},
			},
		},
		{
			name: "no_sorted",
			args: args{
				labels: labels.Labels{
					{
						Name:  "monitor",
						Value: "codelab",
					},
					{
						Name:  "__name__",
						Value: "up",
					},
				},
			},
			want: labels.Labels{
				{
					Name:  "__name__",
					Value: "up",
				},
				{
					Name:  "monitor",
					Value: "codelab",
				},
			},
		},
		{
			name: "labels_empty",
			args: args{
				labels: labels.Labels{},
			},
			want: labels.Labels{},
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
			},
			want: labels.Labels{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sortLabels(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SortLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stringFromLabelsCollision(t *testing.T) {
	tests := []struct {
		input1 labels.Labels
		input2 labels.Labels
	}{
		{
			input1: labels.Labels{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: labels.Labels{
				{
					Name:  "label1",
					Value: "value1,label2=value2",
				},
			},
		},
		{
			input1: labels.Labels{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: labels.Labels{
				{
					Name:  "label1",
					Value: `value1",label2="value2`,
				},
			},
		},
	}
	for _, tt := range tests {
		got1 := tt.input1.String()
		got2 := tt.input2.String()
		if got1 == got2 {
			t.Errorf("StringFromLabels(%v) == StringFromLabels(%v) want not equal", tt.input1, tt.input2)
		}
	}
}

// Test_stringFromLabels is indeed testing Prometheus code which already has test,
// but this test actually serve to ensure Prometheus result don't change, because
// the result must not change.
func Test_stringFromLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels labels.Labels
		want   string
	}{
		{
			name: "simple",
			labels: labels.Labels{
				{Name: "test", Value: "value"},
			},
			want: `{test="value"}`,
		},
		{
			name: "two",
			labels: labels.Labels{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			want: `{label1="value1", label2="value2"}`,
		},
		{
			name: "two-unordered",
			labels: labels.Labels{
				{Name: "label2", Value: "value2"},
				{Name: "label1", Value: "value1"},
			},
			want: `{label2="value2", label1="value1"}`,
		},
		{
			name: "need-quoting",
			labels: labels.Labels{
				{Name: "label1", Value: `value1",label2="value2`},
			},
			want: `{label1="value1\",label2=\"value2"}`,
		},
		{
			name: "need-quoting2",
			labels: labels.Labels{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
			want: `{label1="value1\\\",label2=\\\"value2"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.labels.String(); got != tt.want {
				t.Errorf("tt.labels.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_stringFromLabels(b *testing.B) {
	tests := []struct {
		name   string
		labels labels.Labels
	}{
		{
			name: "simple",
			labels: labels.Labels{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: labels.Labels{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: labels.Labels{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
				{Name: "label3", Value: "value3"},
				{Name: "label4", Value: "value4"},
				{Name: "label5", Value: "value5"},
				{Name: "label6", Value: "value6"},
				{Name: "label7", Value: "value7"},
				{Name: "label8", Value: "value8"},
				{Name: "label9", Value: "value9"},
				{Name: "label0", Value: "value0"},
			},
		},
		{
			name: "five-longer-labels",
			labels: labels.Labels{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: labels.Labels{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = tt.labels.String()
			}
		})
	}
}

func Test_postingsForMatchers(t *testing.T) {
	metrics1 := map[types.MetricID]map[string]string{
		MetricIDTest1: {
			"__name__": "up",
			"job":      "prometheus",
			"instance": "localhost:9090",
		},
		MetricIDTest2: {
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "localhost:9100",
		},
		MetricIDTest3: {
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
		},
		MetricIDTest4: {
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "idle",
		},
		MetricIDTest5: {
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "user",
		},
		MetricIDTest6: {
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "1",
			"mode":     "user",
		},
		MetricIDTest7: {
			"__name__":   "node_filesystem_avail_bytes",
			"job":        "node_exporter",
			"instance":   "localhost:9100",
			"device":     "/dev/mapper/vg0-root",
			"fstype":     "ext4",
			"mountpoint": "/",
		},
		MetricIDTest8: {
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "localhost:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "devel",
		},
		MetricIDTest9: {
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "remote:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "production",
		},
		MetricIDTest10: {
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "remote:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "production",
			"userID":      "42",
		},
	}
	index1 := mockIndexFromMetrics(metrics1)

	metrics2 := make(map[types.MetricID]map[string]string)

	metrics3 := map[types.MetricID]map[string]string{
		MetricIDTest1: {
			"__name__": "up",
			"job":      "prometheus",
			"instance": "localhost:9090",
		},
		math.MaxUint32: {
			"__name__": "metric_id",
			"value":    "exactly-32bits",
			"instance": "localhost:900",
		},
		math.MaxInt64: {
			"__name__": "metric_id",
			"value":    "largest-id",
			"instance": "localhost:900",
		},
	}
	index3 := mockIndexFromMetrics(metrics3)

	for x := 1; x < 101; x++ {
		for y := 0; y < 100; y++ {
			id := types.MetricID(x*100 + y)
			metrics2[id] = map[string]string{
				"__name__":   fmt.Sprintf("generated_%03d", x),
				"label_x":    fmt.Sprintf("%03d", x),
				"label_y":    fmt.Sprintf("%03d", y),
				"multiple_2": fmt.Sprintf("%v", y%2 == 0),
				"multiple_3": fmt.Sprintf("%v", y%3 == 0),
				"multiple_5": fmt.Sprintf("%v", y%5 == 0),
			}
		}
	}

	index2 := mockIndexFromMetrics(metrics2)

	tests := []struct {
		name     string
		index    *CassandraIndex
		matchers []*labels.Matcher
		want     []types.MetricID
		wantLen  int
	}{
		{
			name:  "eq",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
			},
		},
		{
			name:  "eq-eq",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_cpu_seconds_total",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"mode",
					"user",
				),
			},
			want: []types.MetricID{
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "eq-neq",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_cpu_seconds_total",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"mode",
					"user",
				),
			},
			want: []types.MetricID{
				MetricIDTest4,
			},
		},
		{
			name:  "eq-nolabel",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"",
				),
			},
			want: []types.MetricID{
				MetricIDTest7,
			},
		},
		{
			name:  "eq-label",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"environment",
					"",
				),
			},
			want: []types.MetricID{
				MetricIDTest8,
				MetricIDTest9,
				MetricIDTest10,
			},
		},
		{
			name:  "re",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"u.",
				),
			},
			want: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
			},
		},
		{
			name:  "re-re",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"node_cpu_.*",
				),
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"mode",
					"^u.*",
				),
			},
			want: []types.MetricID{
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "re-nre",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"node_(cpu|disk)_seconds_total",
				),
				labels.MustNewMatcher(
					labels.MatchNotRegexp,
					"mode",
					"u\\wer",
				),
			},
			want: []types.MetricID{
				MetricIDTest4,
			},
		},
		{
			name:  "re-re_nolabel",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"environment",
					"^$",
				),
			},
			want: []types.MetricID{
				MetricIDTest7,
			},
		},
		{
			name:  "re-re_label",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"node_filesystem_avail_bytes$",
				),
				labels.MustNewMatcher(
					labels.MatchNotRegexp,
					"environment",
					"^$",
				),
			},
			want: []types.MetricID{
				MetricIDTest8,
				MetricIDTest9,
				MetricIDTest10,
			},
		},
		{
			name:  "re-re*",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"node_filesystem_avail_bytes$",
				),
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"environment",
					".*",
				),
			},
			want: []types.MetricID{
				MetricIDTest7,
				MetricIDTest8,
				MetricIDTest9,
				MetricIDTest10,
			},
		},
		{
			name:  "re-nre*",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"node_filesystem_avail_bytes$",
				),
				labels.MustNewMatcher(
					labels.MatchNotRegexp,
					"environment",
					".*",
				),
			},
			want: []types.MetricID{},
		},
		{
			name:  "eq-nre_empty_and_devel",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchNotRegexp,
					"environment",
					"(|devel)",
				),
			},
			want: []types.MetricID{
				MetricIDTest9,
				MetricIDTest10,
			},
		},
		{
			name:  "eq-nre-eq same label",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchNotRegexp,
					"environment",
					"^$",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"devel",
				),
			},
			want: []types.MetricID{
				MetricIDTest8,
			},
		},
		{
			name:  "eq-eq-no_label",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"production",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"userID",
					"",
				),
			},
			want: []types.MetricID{
				MetricIDTest9,
			},
		},
		{
			name:  "eq-eq-eq_empty",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"production",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"",
				),
			},
			want: []types.MetricID{},
		},
		{
			name:  "eq-eq-re_empty",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"node_filesystem_avail_bytes",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"production",
				),
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"environment",
					".*",
				),
			},
			want: []types.MetricID{
				MetricIDTest9,
				MetricIDTest10,
			},
		},
		{
			name:  "eq_empty",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"",
				),
			},
			want: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
				MetricIDTest4,
				MetricIDTest5,
				MetricIDTest6,
				MetricIDTest7,
			},
		},
		{
			name:  "neq_empty",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"environment",
					"production",
				),
			},
			want: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
				MetricIDTest4,
				MetricIDTest5,
				MetricIDTest6,
				MetricIDTest7,
				MetricIDTest8,
			},
		},
		{
			name:  "index2-eq",
			index: index2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"generated_042",
				),
			},
			wantLen: 100,
			want: []types.MetricID{
				types.MetricID(42*100 + 0),
				types.MetricID(42*100 + 1),
				types.MetricID(42*100 + 2),
				// [...]
			},
		},
		{
			name:  "index2-eq-eq",
			index: index2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"generated_042",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"multiple_2",
					"true",
				),
			},
			wantLen: 50,
			want: []types.MetricID{
				types.MetricID(42*100 + 0),
				types.MetricID(42*100 + 2),
				types.MetricID(42*100 + 4),
				// [...]
			},
		},
		{
			name:  "index2-eq-neq",
			index: index2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"multiple_2",
					"true",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"multiple_2",
					"false",
				),
			},
			wantLen: 5000,
			want: []types.MetricID{
				types.MetricID(1*100 + 0),
				types.MetricID(1*100 + 2),
				types.MetricID(1*100 + 4),
				// [...]
			},
		},
		{
			name:  "index2-eq-neq-2",
			index: index2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"multiple_2",
					"true",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"multiple_2",
					"true",
				),
			},
			wantLen: 0,
			want:    []types.MetricID{},
		},
		{
			name:  "index2-re-neq-eq-neq",
			index: index2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"generated_04.",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"__name__",
					"generated_042",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"multiple_2",
					"true",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"multiple_5",
					"false",
				),
			},
			wantLen: 90,
			want: []types.MetricID{
				types.MetricID(40*100 + 0),
				types.MetricID(40*100 + 10),
				types.MetricID(40*100 + 20),
				// [...]
			},
		},
		{
			name:  "index2-re-nre-eq-neq",
			index: index2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"generated_04.",
				),
				labels.MustNewMatcher(
					labels.MatchNotRegexp,
					"__name__",
					"(generated_04(0|2)|)",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"multiple_2",
					"true",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"multiple_5",
					"false",
				),
			},
			wantLen: 80,
			want: []types.MetricID{
				types.MetricID(41*100 + 0),
				types.MetricID(41*100 + 10),
				types.MetricID(41*100 + 20),
				// [...]
			},
		},
		{
			name:  "index3-exact-32-bits",
			index: index3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"value",
					"exactly-32bits",
				),
			},
			want: []types.MetricID{
				types.MetricID(math.MaxUint32),
			},
		},
		{
			name:  "index3-max-id",
			index: index3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"value",
					"largest-id",
				),
			},
			want: []types.MetricID{
				types.MetricID(math.MaxInt64),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.index.postingsForMatchers(tt.matchers)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)
				return
			}
			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}
			if len(got) != tt.wantLen {
				t.Errorf("postingsForMatchers() len()=%v, want %v", len(got), tt.wantLen)
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})
		t.Run(tt.name+" reverse", func(t *testing.T) {

			matchersReverse := make([]*labels.Matcher, len(tt.matchers))
			for i := range matchersReverse {
				matchersReverse[i] = tt.matchers[len(tt.matchers)-i-1]
			}

			got, err := tt.index.postingsForMatchers(matchersReverse)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)
				return
			}
			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}
			if len(got) != tt.wantLen {
				t.Errorf("postingsForMatchers() len()=%v, want %v", len(got), tt.wantLen)
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func idsToBitset(ids []types.MetricID) *roaring.Bitmap {
	result := roaring.NewBTreeBitmap()

	for _, id := range ids {
		result.Add(uint64(id))
	}

	return result
}

func Test_freeFreeID(t *testing.T) {
	compact := roaring.NewBTreeBitmap()
	compact = compact.Flip(1, 5000)

	spare := compact.Clone()
	spare.Remove(42)
	spare.Remove(1337)
	spare.Remove(44)

	compactLarge := roaring.NewBTreeBitmap()
	compactLarge = compactLarge.Flip(1, 1e5)

	spareLarge := compactLarge.Clone()
	spareLarge.Remove(65539)
	spareLarge.Remove(65540)
	spareLarge.Remove(70000)

	spareZone := compactLarge.Clone()
	spareZone = spareZone.Flip(200, 500)
	spareZone = spareZone.Flip(1e4, 2e4)

	startAndEnd := roaring.NewBTreeBitmap()
	startAndEnd = startAndEnd.Flip(0, 1e4)
	// We want to use, but it WAY to slow...
	// startAndEnd = startAndEnd.Flip(math.MaxUint64 - 1e4, math.MaxUint64)
	// Do Add() instead
	for n := uint64(math.MaxUint64 - 1e4); true; n++ {
		startAndEnd.Add(n)
		if n == math.MaxUint64 {
			break
		}
	}

	// test bug that occured with all IDs assigned between 1 to 36183 (included)
	// but 31436 and 31437.
	// This is actually a bug in pilosa :(
	bug1 := roaring.NewBTreeBitmap()
	bug1 = bug1.Flip(1, 36183)
	_, _ = bug1.Remove(31436, 31437)
	bug1.Optimize()

	tests := []struct {
		name   string
		bitmap *roaring.Bitmap
		want   uint64
	}{
		{
			name:   "empty",
			bitmap: roaring.NewBTreeBitmap(),
			want:   1,
		},
		{
			name:   "compact",
			bitmap: compact,
			want:   5001,
		},
		{
			name:   "spare",
			bitmap: spare,
			want:   42,
		},
		{
			name:   "compactLarge",
			bitmap: compactLarge,
			want:   1e5 + 1,
		},
		{
			name:   "spareLarge",
			bitmap: spareLarge,
			want:   65539,
		},
		{
			name:   "spareZone",
			bitmap: spareZone,
			want:   200,
		},
		{
			name:   "startAndEnd",
			bitmap: startAndEnd,
			want:   10001,
		},
		{
			name:   "bug1",
			bitmap: bug1,
			want:   31436,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := freeFreeID(tt.bitmap); got != tt.want {
				t.Errorf("freeFreeID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_freeFreeID(b *testing.B) {
	compact := roaring.NewBTreeBitmap()
	compact = compact.Flip(1, 5000)

	spare := compact.Clone()
	spare.Remove(42)
	spare.Remove(1337)
	spare.Remove(44)

	compactLarge := roaring.NewBTreeBitmap()
	compactLarge = compactLarge.Flip(1, 1e5)

	spareLarge := compactLarge.Clone()
	spareLarge.Remove(65539)
	spareLarge.Remove(65540)
	spareLarge.Remove(70000)

	spareZone := compactLarge.Clone()
	spareZone = spareZone.Flip(200, 500)
	spareZone = spareZone.Flip(1e4, 2e4)

	startAndEnd := roaring.NewBTreeBitmap()
	startAndEnd = startAndEnd.Flip(0, 1e4)
	for n := uint64(math.MaxUint64); true; n++ {
		startAndEnd.Add(n)
		if n == math.MaxUint64 {
			break
		}
	}

	tests := []struct {
		name   string
		bitmap *roaring.Bitmap
	}{
		{
			name:   "empty",
			bitmap: roaring.NewBTreeBitmap(),
		},
		{
			name:   "compact",
			bitmap: compact,
		},
		{
			name:   "spare",
			bitmap: spare,
		},
		{
			name:   "compactLarge",
			bitmap: compactLarge,
		},
		{
			name:   "spareLarge",
			bitmap: spareLarge,
		},
		{
			name:   "spareZone",
			bitmap: spareZone,
		},
		{
			name:   "startAndEnd",
			bitmap: startAndEnd,
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = freeFreeID(tt.bitmap)
			}
		})
	}
}

// Test_expiration will run a small scenario on the index to check expiration.
func Test_expiration(t *testing.T) {
	// For this test, the default TTL must be smaller than longTTL.
	// It should be big enough to be kept until t5 (months if shortTTL & update delays are days)
	defaultTTL := 365 * 24 * time.Hour
	shortTTL := 2 * 24 * time.Hour
	longTTL := 375 * 24 * time.Hour
	// current implementation only delete metrics expired the day before
	implementationDelay := 24 * time.Hour
	// updateDelay is the delay after which we are guaranted to trigger an TTL update
	updateDelay := cassandraTTLUpdateDelay + cassandraTTLUpdateJitter + time.Second

	// At t0, we will create 4 metrics: two with shortTLL, one with longTTL and without TTL (so using default TTL)
	t0 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	// At t1 nothing expired and we refresh TTL of one shortTTL entry
	t1 := t0.Add(updateDelay).Add(implementationDelay)
	// At t2, the non-refreshed entry expired and is deleted
	// At t2, also refresh the longTTL entry but with 2-days TTL. This will NOT update the expiration date
	t2 := t0.Add(updateDelay).Add(shortTTL).Add(implementationDelay)
	// At t3, the entry refreshed at t1 expired and is deleted
	// At t3, also insert expireBatchSize + 10 metrics with shortTTL
	t3 := t1.Add(updateDelay).Add(shortTTL).Add(implementationDelay)
	// At t4, check that nothing happened
	t4 := t3.Add(updateDelay).Add(shortTTL - time.Hour)
	// At t5, metrics added at t3 have expired and are deleted
	t5 := t3.Add(updateDelay).Add(shortTTL).Add(implementationDelay)
	// At t6, all metrics are expired and deleted
	t6 := t2.Add(updateDelay).Add(longTTL).Add(implementationDelay)

	metrics := []map[string]string{
		map[string]string{
			"__name__":    "up",
			"description": "The metrics without TTL that use default TTL",
		},
		map[string]string{
			"__name__":    "ttl",
			"unit":        "month",
			"value":       "13",
			"description": "The metrics with TTL set to 13 months",
		},
		map[string]string{
			"__name__":    "ttl",
			"unit":        "day",
			"value":       "2",
			"description": "The metrics with TTL set to 2 months",
		},
		map[string]string{
			"__name__":    "ttl",
			"unit":        "day",
			"value":       "2",
			"updated":     "yes",
			"description": "The metrics with TTL set to 2 months",
		},
	}
	metricsTTL := []time.Duration{
		defaultTTL,
		longTTL,
		shortTTL,
		shortTTL,
	}
	metricsID := make([]types.MetricID, len(metrics))
	for n := 1; n < len(metrics); n++ {
		secondTTL := int64(metricsTTL[n].Seconds())
		metrics[n]["__ttl__"] = strconv.FormatInt(secondTTL, 10)
	}

	store := &mockStore{}
	index, err := new(store, Options{
		DefaultTimeToLive: defaultTTL,
		LockFactory:       &mockLockFactory{},
		States:            &mockState{},
	})

	if err != nil {
		t.Error(err)
	}

	var ttls []int64

	labelsList := make([]labels.Labels, len(metrics))
	for i, m := range metrics {
		labelsList[i] = labelsMapToList(m, false)
	}

	metricsID, ttls, err = index.lookupIDs(context.Background(), labelsList, t0)
	if err != nil {
		t.Error(err)
	}

	for i, got := range ttls {
		want := int64(metricsTTL[i].Seconds())
		if got != want {
			t.Errorf("got ttl = %d, want %d", got, want)
		}
	}

	// Check in store that correct write happened
	for n := 0; n < len(metrics); n++ {
		labels := labelsMapToList(metrics[n], true)
		id := metricsID[n]
		if !reflect.DeepEqual(store.id2labels[id], labels) {
			t.Errorf("id2labels[%d] = %v, want %v", id, store.id2labels[id], labels)
		}
		wantMinExpire := t0.Add(metricsTTL[n]).Add(cassandraTTLUpdateDelay)
		wantMaxExpire := t0.Add(metricsTTL[n]).Add(updateDelay)
		if store.id2expiration[id].After(wantMaxExpire) || store.id2expiration[id].Before(wantMinExpire) {
			t.Errorf("id2expiration[%d] = %v, want between %v and %v", id, store.id2expiration[id], wantMinExpire, wantMaxExpire)
		}
		if len(store.expiration) != 3 {
			t.Errorf("len(store.expiration) = %v, want 3", len(store.expiration))
		}
	}

	index.expire(t0)
	index.cassandraExpire(t0)

	allIds, err := index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIds))
	}

	labelsList[3] = labelsMapToList(metrics[3], false)
	ids, ttls, err := index.lookupIDs(context.Background(), labelsList[3:4], t1)
	if err != nil {
		t.Error(err)
		return // can't continue, lock may be hold
	}
	if ttls[0] != int64(shortTTL.Seconds()) {
		t.Errorf("ttl = %d, want %f", ttls[0], shortTTL.Seconds())
	}
	if ids[0] != metricsID[3] {
		t.Errorf("id = %d, want %d", ids[0], metricsID[3])
	}

	index.applyExpirationUpdateRequests()
	// metrics[3] was moved to a new expiration slot
	if len(store.expiration) != 4 {
		t.Errorf("len(store.expiration) = %v, want 4", len(store.expiration))
	}
	for _, id := range []types.MetricID{metricsID[2], metricsID[3]} {
		expire := store.id2expiration[id].Truncate(24 * time.Hour)
		bitmap := roaring.NewBTreeBitmap()
		bitmap.UnmarshalBinary(store.expiration[expire])

		got := bitmap.Slice()
		want := []uint64{
			uint64(id),
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("store.expiration[%v == expiration of id %d] = %v,  %v", expire, id, got, want)
		}
	}

	index.expire(t1)
	// each call to cassandraExpire do one day, but calling multiple time
	// isn't an issue but it must be called at least once per day
	for t := t0; t.Before(t1); t = t.Add(24 * time.Hour) {
		index.cassandraExpire(t1)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIds))
	}

	metrics[0]["__ttl__"] = strconv.FormatInt(int64(shortTTL.Seconds()), 10)
	labelsList[0] = labelsMapToList(metrics[0], false)
	ids, ttls, err = index.lookupIDs(context.Background(), labelsList[0:1], t2)
	if err != nil {
		t.Error(err)
		return // can't continue, lock make be hold
	}
	if ttls[0] != int64(shortTTL.Seconds()) {
		t.Errorf("ttl = %d, want %f", ttls[0], shortTTL.Seconds())
	}
	if ids[0] != metricsID[0] {
		t.Errorf("id = %d, want %d", ids[0], metricsID[0])
	}
	// But store expiration is still using 1 year
	wantMinExpire := t0.Add(defaultTTL).Add(cassandraTTLUpdateDelay)
	if store.id2expiration[ids[0]].Before(wantMinExpire) {
		t.Errorf("id2expiration[%d] = %v, want > %v", ids[0], store.id2expiration[ids[0]], wantMinExpire)
	}

	index.expire(t2)
	for t := t1; t.Before(t2); t = t.Add(24 * time.Hour) {
		index.cassandraExpire(t2)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 3 {
		t.Errorf("len(allIds) = %d, want 3", len(allIds))
	}
	for _, id := range allIds {
		if id == metricsID[2] {
			t.Errorf("allIds = %v and contains %d, want not contains this value", allIds, metricsID[2])
		}
	}

	index.expire(t3)
	for t := t2; t.Before(t3); t = t.Add(24 * time.Hour) {
		index.cassandraExpire(t3)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 2 {
		t.Errorf("len(allIds) = %d, want 2", len(allIds))
	}

	labelsList = make([]labels.Labels, expireBatchSize+10)
	for n := 0; n < expireBatchSize+10; n++ {
		labels := map[string]string{
			"__name__": "filler",
			"id":       strconv.FormatInt(int64(n), 10),
			"__ttl__":  strconv.FormatInt(int64(shortTTL.Seconds()), 10),
		}
		labelsList[n] = labelsMapToList(labels, false)
	}

	_, _, err = index.lookupIDs(context.Background(), labelsList, t3)
	if err != nil {
		t.Error(err)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 2+expireBatchSize+10 {
		t.Errorf("len(allIds) = %d, want %d", len(allIds), 2+expireBatchSize+10)
	}

	index.expire(t4)
	for t := t3; t.Before(t4); t = t.Add(24 * time.Hour) {
		index.cassandraExpire(t4)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 2+expireBatchSize+10 {
		t.Errorf("len(allIds) = %d, want %d", len(allIds), 2+expireBatchSize+10)
	}

	index.expire(t5)
	for t := t4; t.Before(t5); t = t.Add(24 * time.Hour) {
		index.cassandraExpire(t5)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 2 {
		t.Errorf("len(allIds) = %d, want 2", len(allIds))
	}

	index.expire(t6)
	for t := t5; t.Before(t6); t = t.Add(24 * time.Hour) {
		index.cassandraExpire(t6)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 0 {
		t.Errorf("allIds = %v, want []", allIds)
	}
}
