package index

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"squirreldb/types"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/prometheus/prompb"
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
	for {
		ok := l.TryLock()
		if ok {
			return
		}

		time.Sleep(10 * time.Second)
	}
}

func (l *mockLock) Unlock() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.acquired {
		panic("unlock of unlocked mutex")
	}

	l.acquired = false
}

func (l *mockLock) TryLock() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.acquired {
		return false
	}

	l.acquired = true
	return true
}

type mockStore struct {
	mutex sync.Mutex

	labels2id     map[string]types.MetricID
	postings      map[string]map[string][]byte
	id2labels     map[types.MetricID][]*prompb.Label
	id2expiration map[types.MetricID]time.Time
	expiration    map[time.Time][]byte
}

func (s *mockStore) Init() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.labels2id = make(map[string]types.MetricID)
	s.postings = make(map[string]map[string][]byte)
	s.id2labels = make(map[types.MetricID][]*prompb.Label)
	s.id2expiration = make(map[types.MetricID]time.Time)
	s.expiration = make(map[time.Time][]byte)
	return nil
}

func (s mockStore) SelectLabels2ID(sortedLabelsString string) (types.MetricID, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	result, ok := s.labels2id[sortedLabelsString]
	if !ok {
		return 0, gocql.ErrNotFound
	}

	return result, nil
}

func (s *mockStore) SelectID2Labels(id types.MetricID) ([]*prompb.Label, error) {
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

func (s *mockStore) SelectValueForName(name string) ([]string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	m, ok := s.postings[name]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	results := make([]string, 0, len(m))

	for k := range m {
		results = append(results, k)
	}

	return results, nil
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

func (s *mockStore) InsertID2Labels(id types.MetricID, sortedLabels []*prompb.Label, expiration time.Time) error {
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

func labelsMapToList(m map[string]string, dropSpecialLabel bool) []*prompb.Label {
	results := make([]*prompb.Label, 0, len(m))

	for k, v := range m {
		if dropSpecialLabel && (k == timeToLiveLabelName || k == idLabelName) {
			continue
		}
		results = append(results, &prompb.Label{
			Name:  k,
			Value: v,
		})
	}

	results = sortLabels(results)
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
		sortedLabelsString := stringFromLabels(sortedLabels)
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
		labels []*prompb.Label
	}{
		{
			name: "simple",
			labels: []*prompb.Label{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: []*prompb.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: []*prompb.Label{
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
			labels: []*prompb.Label{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: []*prompb.Label{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = keyFromLabels(tt.labels)
			}
		})
	}
}

func Test_timeToLiveFromLabels(t *testing.T) {
	tests := []struct {
		name       string
		labels     []*prompb.Label
		want       int64
		wantLabels []*prompb.Label
	}{
		{
			name: "no ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
			want: 0,
			wantLabels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
				{Name: "__ttl__", Value: "3600"},
			},
			want: 3600,
			wantLabels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl2",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
			want: 3600,
			wantLabels: []*prompb.Label{
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
		labels     []*prompb.Label
		wantTTL    int64
		wantLabels []*prompb.Label
		wantErr    bool
	}{
		{
			name: "no ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "with ttl",
			labels: []*prompb.Label{
				{Name: "__name__", Value: "up"},
				{Name: "__ttl__", Value: "3600"},
				{Name: "job", Value: "scrape"},
			},
		},
		{
			name: "12 labels no ttl",
			labels: []*prompb.Label{
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
			labels: []*prompb.Label{
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
				labelsIn := make([]*prompb.Label, len(tt.labels))
				copy(labelsIn, tt.labels)
				_ = timeToLiveFromLabels(&labelsIn)
			}
		})
	}
}

func Test_getLabelsValue(t *testing.T) {
	type args struct {
		labels []*prompb.Label
		name   string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "contains",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "monitor",
						Value: "codelab",
					},
				},
				name: "monitor",
			},
			want: "codelab",
		},
		{
			name: "contains_empty_value",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "down",
					},
					{
						Name:  "job",
						Value: "",
					},
				},
				name: "job",
			},
			want: "",
		},
		{
			name: "no_contains",
			args: args{
				labels: []*prompb.Label{
					{
						Name:  "__name__",
						Value: "up",
					},
				},
				name: "monitor",
			},
			want: "",
		},
		{
			name: "labels_empty",
			args: args{
				labels: []*prompb.Label{},
				name:   "monitor",
			},
			want: "",
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
				name:   "monitor",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getLabelsValue(tt.args.labels, tt.args.name)
			if got != tt.want {
				t.Errorf("getLabelsValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getMatchersValue(t *testing.T) {
	type args struct {
		matchers []*prompb.LabelMatcher
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
				matchers: []*prompb.LabelMatcher{
					{
						Name:  "__name__",
						Value: "up",
					},
					{
						Name:  "monitor",
						Value: "codelab",
					},
				},
				name: "monitor",
			},
			want:  "codelab",
			want1: true,
		},
		{
			name: "contains_empty_value",
			args: args{
				matchers: []*prompb.LabelMatcher{
					{
						Name:  "__name__",
						Value: "down",
					},
					{
						Name:  "job",
						Value: "",
					},
				},
				name: "job",
			},
			want:  "",
			want1: true,
		},
		{
			name: "no_contains",
			args: args{
				matchers: []*prompb.LabelMatcher{
					{
						Name:  "__name__",
						Value: "up",
					},
				},
				name: "monitor",
			},
			want:  "",
			want1: false,
		},
		{
			name: "labels_empty",
			args: args{
				matchers: []*prompb.LabelMatcher{},
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
		labels []*prompb.Label
	}
	tests := []struct {
		name string
		args args
		want []*prompb.Label
	}{
		{
			name: "sorted",
			args: args{
				labels: []*prompb.Label{
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
			want: []*prompb.Label{
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
				labels: []*prompb.Label{
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
			want: []*prompb.Label{
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
				labels: []*prompb.Label{},
			},
			want: nil,
		},
		{
			name: "labels_nil",
			args: args{
				labels: nil,
			},
			want: nil,
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
		input1 []*prompb.Label
		input2 []*prompb.Label
	}{
		{
			input1: []*prompb.Label{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: []*prompb.Label{
				{
					Name:  "label1",
					Value: "value1,label2=value2",
				},
			},
		},
		{
			input1: []*prompb.Label{
				{
					Name:  "label1",
					Value: "value1",
				},
				{
					Name:  "label2",
					Value: "value2",
				},
			},
			input2: []*prompb.Label{
				{
					Name:  "label1",
					Value: `value1",label2="value2`,
				},
			},
		},
	}
	for _, tt := range tests {
		got1 := stringFromLabels(tt.input1)
		got2 := stringFromLabels(tt.input2)
		if got1 == got2 {
			t.Errorf("StringFromLabels(%v) == StringFromLabels(%v) want not equal", tt.input1, tt.input2)
		}
	}
}

func Test_stringFromLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels []*prompb.Label
		want   string
	}{
		{
			name: "simple",
			labels: []*prompb.Label{
				{Name: "test", Value: "value"},
			},
			want: `test="value"`,
		},
		{
			name: "two",
			labels: []*prompb.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
			want: `label1="value1",label2="value2"`,
		},
		{
			name: "two-unordered",
			labels: []*prompb.Label{
				{Name: "label2", Value: "value2"},
				{Name: "label1", Value: "value1"},
			},
			want: `label2="value2",label1="value1"`,
		},
		{
			name: "need-quoting",
			labels: []*prompb.Label{
				{Name: "label1", Value: `value1",label2="value2`},
			},
			want: `label1="value1\",label2=\"value2"`,
		},
		{
			name: "need-quoting2",
			labels: []*prompb.Label{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
			want: `label1="value1\\\",label2=\\\"value2"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringFromLabels(tt.labels); got != tt.want {
				t.Errorf("StringFromLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_stringFromLabels(b *testing.B) {
	tests := []struct {
		name   string
		labels []*prompb.Label
	}{
		{
			name: "simple",
			labels: []*prompb.Label{
				{Name: "test", Value: "value"},
			},
		},
		{
			name: "two",
			labels: []*prompb.Label{
				{Name: "label1", Value: "value1"},
				{Name: "label2", Value: "value2"},
			},
		},
		{
			name: "ten-labels",
			labels: []*prompb.Label{
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
			labels: []*prompb.Label{
				{Name: "the-label-one", Value: "the-first-value"},
				{Name: "the-second-label", Value: "another-value"},
				{Name: "the-label-after-two", Value: "all-value-are-different"},
				{Name: "the-label-four", Value: "sort"},
				{Name: "the-last-label", Value: "but-most-of-the-time-value-is-long"},
			},
		},
		{
			name: "need-quoting2",
			labels: []*prompb.Label{
				{Name: "label1", Value: `value1\",label2=\"value2`},
			},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = stringFromLabels(tt.labels)
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
		matchers []*prompb.LabelMatcher
		want     []types.MetricID
		wantLen  int
	}{
		{
			name:  "eq",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "up",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "mode",
					Value: "user",
				},
			},
			want: []types.MetricID{
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "eq-neq",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "mode",
					Value: "user",
				},
			},
			want: []types.MetricID{
				MetricIDTest4,
			},
		},
		{
			name:  "eq-nolabel",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "",
				},
			},
			want: []types.MetricID{
				MetricIDTest7,
			},
		},
		{
			name:  "eq-label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "environment",
					Value: "",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "u.",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_cpu_.*",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "mode",
					Value: "^u.*",
				},
			},
			want: []types.MetricID{
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "re-nre",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_(cpu|disk)_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "mode",
					Value: "u\\wer",
				},
			},
			want: []types.MetricID{
				MetricIDTest4,
			},
		},
		{
			name:  "re-re_nolabel",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: "^$",
				},
			},
			want: []types.MetricID{
				MetricIDTest7,
			},
		},
		{
			name:  "re-re_label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: ".*",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: ".*",
				},
			},
			want: []types.MetricID{},
		},
		{
			name:  "eq-nre_empty_and_devel",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "(|devel)",
				},
			},
			want: []types.MetricID{
				MetricIDTest9,
				MetricIDTest10,
			},
		},
		{
			name:  "eq-nre-eq same label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "devel",
				},
			},
			want: []types.MetricID{
				MetricIDTest8,
			},
		},
		{
			name:  "eq-eq-no_label",
			index: index1,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "production",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "userID",
					Value: "",
				},
			},
			want: []types.MetricID{
				MetricIDTest9,
			},
		},
		{
			name:  "index2-eq",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "generated_042",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "generated_042",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_2",
					Value: "false",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_2",
					Value: "true",
				},
			},
			wantLen: 0,
			want:    []types.MetricID{},
		},
		{
			name:  "index2-re-neq-eq-neq",
			index: index2,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "generated_04.",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "__name__",
					Value: "generated_042",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_5",
					Value: "false",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "generated_04.",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "__name__",
					Value: "(generated_04(0|2)|)",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "multiple_2",
					Value: "true",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "multiple_5",
					Value: "false",
				},
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
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "value",
					Value: "exactly-32bits",
				},
			},
			want: []types.MetricID{
				types.MetricID(math.MaxUint32),
			},
		},
		{
			name:  "index3-max-id",
			index: index3,
			matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "value",
					Value: "largest-id",
				},
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
	}
}

func Test_substractResult(t *testing.T) {
	tests := []struct {
		name  string
		main  []types.MetricID
		lists [][]types.MetricID
		want  []types.MetricID
	}{
		{
			name: "same-list",
			main: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
				MetricIDTest4,
			},
			lists: [][]types.MetricID{
				{
					MetricIDTest1,
					MetricIDTest2,
					MetricIDTest3,
					MetricIDTest4,
				},
			},
			want: []types.MetricID{},
		},
		{
			name: "two-list",
			main: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
				MetricIDTest4,
			},
			lists: [][]types.MetricID{
				{
					MetricIDTest3,
					MetricIDTest4,
				},
			},
			want: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
			},
		},
		{
			name: "two-list-2",
			main: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
				MetricIDTest4,
			},
			lists: [][]types.MetricID{
				{
					MetricIDTest1,
					MetricIDTest2,
				},
			},
			want: []types.MetricID{
				MetricIDTest3,
				MetricIDTest4,
			},
		},
		{
			name: "two-list-3",
			main: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest4,
				MetricIDTest6,
				MetricIDTest8,
				MetricIDTest10,
				MetricIDTest12,
			},
			lists: [][]types.MetricID{
				{
					MetricIDTest1,
					MetricIDTest3,
					MetricIDTest6,
					MetricIDTest12,
				},
			},
			want: []types.MetricID{
				MetricIDTest2,
				MetricIDTest4,
				MetricIDTest8,
				MetricIDTest10,
			},
		},
		{
			name: "three-list",
			main: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest4,
				MetricIDTest6,
				MetricIDTest8,
				MetricIDTest10,
				MetricIDTest12,
				MetricIDTest14,
			},
			lists: [][]types.MetricID{
				{
					MetricIDTest1,
					MetricIDTest3,
					MetricIDTest6,
					MetricIDTest12,
					MetricIDTest15,
				},
				{
					MetricIDTest1,
					MetricIDTest5,
					MetricIDTest10,
					MetricIDTest15,
				},
			},
			want: []types.MetricID{
				MetricIDTest2,
				MetricIDTest4,
				MetricIDTest8,
				MetricIDTest14,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lists := make([]*roaring.Bitmap, len(tt.lists))

			for i, l := range tt.lists {
				lists[i] = idsToBitset(l)
			}

			got := substractResult(idsToBitset(tt.main), lists...)
			if eq, err := got.BitwiseEqual(idsToBitset(tt.want)); err != nil || !eq {
				t.Errorf("substractResult() = %v, want %v", got, tt.want)
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
// It will:
// * at Day 1 register 4 metrics. One without TTL (default of 1 year),
//   one with 13 months TTL, two with 2 day TTL
// * at Day 2, redo lookup for one of the 2-day TTL (refresh the entry)
// * at Day 3 check that none are deleted (because we check for metrics that expire the previous day)
//   also redo lookup for the 1 year TTL but with a 2 days TTL. check that store TTL didn't change
// * at day 4 check that one metrics get deleted.
// * at day 5 check that the other metrics get deleted.
//   also insert expireBatchSize + 10 metrics with 2 days TTL
// * at day 7, nothing happend
// * at day 8, check that all metrics are deleted (excepted the 1 year TTL)
// * at day 400, everything is deleted :)
//
func Test_expiration(t *testing.T) {
	day1 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	day3 := day1.Add(2 * 24 * time.Hour)
	day4 := day1.Add(3 * 24 * time.Hour)
	day5 := day1.Add(4 * 24 * time.Hour)
	day7 := day1.Add(6 * 24 * time.Hour)
	day8 := day1.Add(7 * 24 * time.Hour)
	day400 := day1.Add(399 * 24 * time.Hour)

	defaultTTL := 365 * 24 * time.Hour
	thirtyMonthTTL := 375 * 24 * time.Hour
	twoDayTTL := 2 * 24 * time.Hour

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
		thirtyMonthTTL,
		twoDayTTL,
		twoDayTTL,
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

	labelsList := make([][]*prompb.Label, len(metrics))
	for i, m := range metrics {
		labelsList[i] = labelsMapToList(m, false)
	}

	metricsID, ttls, err = index.lookupIDs(labelsList, day1)
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
		wantExpire := day1.Add(metricsTTL[n]).Add(cassandraTTLUpdateDelay)
		if !store.id2expiration[id].Equal(wantExpire) {
			t.Errorf("id2expiration[%d] = %v, want %v", id, store.id2expiration[id], wantExpire)
		}
		if len(store.expiration) != 3 {
			t.Errorf("len(store.expiration) = %v, want 3", len(store.expiration))
		}
	}

	index.expire(day1)
	index.cassandraExpire(day1)

	allIds, err := index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIds))
	}

	labelsList[3] = labelsMapToList(metrics[3], false)
	ids, ttls, err := index.lookupIDs(labelsList[3:4], day2)
	if err != nil {
		t.Error(err)
		return // can't continue, lock may be hold
	}
	if ttls[0] != int64(twoDayTTL.Seconds()) {
		t.Errorf("ttl = %d, want %f", ttls[0], twoDayTTL.Seconds())
	}
	if ids[0] != metricsID[3] {
		t.Errorf("id = %d, want %d", ids[0], metricsID[3])
	}

	index.applyExpirationUpdateRequests()
	// metrics[3] was moved to a new expiration slot
	if len(store.expiration) != 4 {
		t.Errorf("len(store.expiration) = %v, want 4", len(store.expiration))
	}
	wantExpire := day1.Add(metricsTTL[2]).Add(cassandraTTLUpdateDelay).Truncate(24 * time.Hour)
	bitmap := roaring.NewBTreeBitmap()
	bitmap.UnmarshalBinary(store.expiration[wantExpire])
	got := bitmap.Slice()
	want := []uint64{
		uint64(metricsID[2]),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("store.expiration[%v] = %v,  %v", wantExpire, got, want)
	}

	index.expire(day2)
	index.cassandraExpire(day2)

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIds))
	}

	index.expire(day3)
	index.cassandraExpire(day3)

	metrics[0]["__ttl__"] = strconv.FormatInt(int64(twoDayTTL.Seconds()), 10)
	labelsList[0] = labelsMapToList(metrics[0], false)
	ids, ttls, err = index.lookupIDs(labelsList[0:1], day3)
	if err != nil {
		t.Error(err)
		return // can't continue, lock make be hold
	}
	if ttls[0] != int64(twoDayTTL.Seconds()) {
		t.Errorf("ttl = %d, want %f", ttls[0], twoDayTTL.Seconds())
	}
	if ids[0] != metricsID[0] {
		t.Errorf("id = %d, want %d", ids[0], metricsID[0])
	}
	// But store expiration is still using 1 year
	wantExpire = day1.Add(defaultTTL).Add(cassandraTTLUpdateDelay)
	if !store.id2expiration[ids[0]].Equal(wantExpire) {
		t.Errorf("id2expiration[%d] = %v, want %v", ids[0], store.id2expiration[ids[0]], wantExpire)
	}

	// BTW, each call to cassandraExpire do one day, but calling multiple time
	// isn't an issue
	index.cassandraExpire(day3)
	index.cassandraExpire(day3)

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIds))
	}

	index.expire(day4)
	index.cassandraExpire(day4)

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

	index.expire(day5)
	index.cassandraExpire(day5)

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 2 {
		t.Errorf("len(allIds) = %d, want 2", len(allIds))
	}

	labelsList = make([][]*prompb.Label, expireBatchSize+10)
	for n := 0; n < expireBatchSize+10; n++ {
		labels := map[string]string{
			"__name__": "filler",
			"id":       strconv.FormatInt(int64(n), 10),
			"__ttl__":  strconv.FormatInt(int64(twoDayTTL.Seconds()), 10),
		}
		labelsList[n] = labelsMapToList(labels, false)
	}

	_, _, err = index.lookupIDs(labelsList, day5)
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

	// call at least once per day
	for n := 5; n <= 7; n++ {
		index.cassandraExpire(day7)
	}

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 2+expireBatchSize+10 {
		t.Errorf("len(allIds) = %d, want %d", len(allIds), 2+expireBatchSize+10)
	}

	index.cassandraExpire(day8)

	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 2 {
		t.Errorf("len(allIds) = %d, want 2", len(allIds))
	}

	// call at least once per day
	for n := 8; n <= 400; n++ {
		index.cassandraExpire(day400)
	}
	allIds, err = index.AllIDs()
	if err != nil {
		t.Error(err)
	}
	if len(allIds) != 0 {
		t.Errorf("allIds = %v, want []", allIds)
	}
}
