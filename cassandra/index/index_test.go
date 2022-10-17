package index

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"squirreldb/dummy"
	"squirreldb/types"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog/log"
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
	locks map[string]*mockLock
	mutex sync.Mutex
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
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return false
		}
	}
}

type mockStore struct {
	expiration    map[time.Time][]byte
	labels2id     map[string]types.MetricID
	postings      map[int32]map[string]map[string][]byte
	id2labels     map[types.MetricID]labels.Labels
	id2expiration map[types.MetricID]time.Time
	queryCount    int
	mutex         sync.Mutex
}

func (s *mockStore) Init(ctx context.Context) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.labels2id != nil {
		return nil
	}

	s.labels2id = make(map[string]types.MetricID)
	s.postings = make(map[int32]map[string]map[string][]byte)
	s.id2labels = make(map[types.MetricID]labels.Labels)
	s.id2expiration = make(map[types.MetricID]time.Time)
	s.expiration = make(map[time.Time][]byte)

	return ctx.Err()
}

func (s *mockStore) SelectLabelsList2ID(ctx context.Context, input []string) (map[string]types.MetricID, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	results := make(map[string]types.MetricID, len(input))

	for _, sortedLabelsString := range input {
		result, ok := s.labels2id[sortedLabelsString]
		if ok {
			results[sortedLabelsString] = result
		}
	}

	return results, ctx.Err()
}

func (s *mockStore) SelectIDS2LabelsAndExpiration(
	ctx context.Context,
	ids []types.MetricID,
) (map[types.MetricID]labels.Labels, map[types.MetricID]time.Time, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	results := make(map[types.MetricID]labels.Labels, len(ids))
	results2 := make(map[types.MetricID]time.Time, len(ids))

	for _, id := range ids {
		tmp, ok := s.id2labels[id]
		if ok {
			results[id] = tmp
		}

		tmp2, ok := s.id2expiration[id]
		if ok {
			results2[id] = tmp2
		}
	}

	return results, results2, ctx.Err()
}

func (s *mockStore) SelectExpiration(ctx context.Context, day time.Time) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	result, ok := s.expiration[day]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	return result, ctx.Err()
}

type valueAndByte struct {
	value  string
	buffer []byte
}

type mockPostingIter struct {
	err     error
	results []valueAndByte
	next    valueAndByte
	idx     int
}

func (i *mockPostingIter) HasNext() bool {
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

func (i *mockPostingIter) Next() (string, []byte) {
	if i.next.buffer == nil {
		panic("This shouldn't happen. Probably HasNext() were not called")
	}

	r := i.next

	// We do this to allow checking that HasNext()/Next() are always called thogether
	i.next.buffer = nil

	return r.value, r.buffer
}

func (i mockPostingIter) Err() error {
	return i.err
}

func (s *mockStore) SelectPostingByName(ctx context.Context, shard int32, name string) postingIter {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	if shard == 0 {
		panic("uninitialized shard value")
	}

	postings, ok := s.postings[shard]
	if !ok {
		return &mockPostingIter{}
	}

	m, ok := postings[name]
	if !ok {
		return &mockPostingIter{}
	}

	results := make([]valueAndByte, 0, len(m))
	for k, v := range m {
		results = append(results, valueAndByte{value: k, buffer: v})
	}

	sort.Slice(results, func(i, j int) bool {
		return strings.Compare(results[i].value, results[j].value) < 0
	})

	return &mockPostingIter{
		err:     ctx.Err(),
		results: results,
		idx:     0,
	}
}

func (s *mockStore) SelectPostingByNameValue(
	ctx context.Context,
	shard int32,
	name string,
	value string,
) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	if shard == 0 {
		panic("uninitialized shard value")
	}

	postings, ok := s.postings[shard]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	m, ok := postings[name]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	result, ok := m[value]
	if !ok {
		return nil, gocql.ErrNotFound
	}

	return result, ctx.Err()
}

func (s *mockStore) SelectValueForName(ctx context.Context, shard int32, name string) ([]string, [][]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	if shard == 0 {
		panic("uninitialized shard value")
	}

	postings, ok := s.postings[shard]
	if !ok {
		return nil, nil, gocql.ErrNotFound
	}

	m, ok := postings[name]
	if !ok {
		return nil, nil, gocql.ErrNotFound
	}

	values := make([]string, 0, len(m))
	buffers := make([][]byte, 0, len(m))

	for k, v := range m {
		values = append(values, k)
		buffers = append(buffers, v)
	}

	return values, buffers, ctx.Err()
}

func (s *mockStore) InsertPostings(ctx context.Context, shard int32, name string, value string, bitset []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	if shard == 0 {
		panic("uninitialized shard value")
	}

	postings, ok := s.postings[shard]
	if !ok {
		postings = make(map[string]map[string][]byte)
		s.postings[shard] = postings
	}

	m, ok := postings[name]
	if !ok {
		m = make(map[string][]byte)
		postings[name] = m
	}

	m[value] = bitset

	return ctx.Err()
}

func (s *mockStore) InsertID2Labels(
	ctx context.Context,
	id types.MetricID,
	sortedLabels labels.Labels,
	expiration time.Time,
) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	s.id2labels[id] = sortedLabels
	s.id2expiration[id] = expiration

	return ctx.Err()
}

func (s *mockStore) InsertLabels2ID(ctx context.Context, sortedLabelsString string, id types.MetricID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	s.labels2id[sortedLabelsString] = id

	return ctx.Err()
}

func (s *mockStore) InsertExpiration(ctx context.Context, day time.Time, bitset []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	s.expiration[day] = bitset

	return ctx.Err()
}

func (s *mockStore) UpdateID2LabelsExpiration(ctx context.Context, id types.MetricID, expiration time.Time) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	s.id2expiration[id] = expiration

	return ctx.Err()
}

func (s *mockStore) DeleteLabels2ID(ctx context.Context, sortedLabelsString string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	_, ok := s.labels2id[sortedLabelsString]
	if !ok {
		return gocql.ErrNotFound
	}

	delete(s.labels2id, sortedLabelsString)

	return ctx.Err()
}

func (s *mockStore) DeleteID2Labels(ctx context.Context, id types.MetricID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	_, ok := s.id2labels[id]
	_, ok2 := s.id2expiration[id]

	if !ok && !ok2 {
		return gocql.ErrNotFound
	}

	delete(s.id2labels, id)
	delete(s.id2expiration, id)

	return ctx.Err()
}

func (s *mockStore) DeleteExpiration(ctx context.Context, day time.Time) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	if _, ok := s.expiration[day]; !ok {
		return gocql.ErrNotFound
	}

	delete(s.expiration, day)

	return ctx.Err()
}

func (s *mockStore) DeletePostings(ctx context.Context, shard int32, name string, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++

	if shard == 0 {
		panic("uninitialized shard value")
	}

	postings, ok := s.postings[shard]
	if !ok {
		return gocql.ErrNotFound
	}

	m, ok := postings[name]
	if !ok {
		return gocql.ErrNotFound
	}

	if _, ok = m[value]; !ok {
		return gocql.ErrNotFound
	}

	delete(m, value)

	if len(m) == 0 {
		delete(postings, name)

		if len(postings) == 0 {
			delete(s.postings, shard)
		}
	}

	return ctx.Err()
}

func toLookupRequests(list []labels.Labels, now time.Time) []types.LookupRequest {
	results := make([]types.LookupRequest, len(list))

	for i, l := range list {
		results[i] = types.LookupRequest{
			Labels: l.Copy(),
			Start:  now,
			End:    now,
		}
	}

	return results
}

func labelsMapToList(m map[string]string, dropSpecialLabel bool) labels.Labels {
	results := make(labels.Labels, 0, len(m))

	for k, v := range m {
		if dropSpecialLabel && (k == timeToLiveLabelName) {
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

func mockIndexFromMetrics(
	start, end time.Time,
	metrics map[types.MetricID]map[string]string,
) *CassandraIndex {
	index, err := initialize(
		context.Background(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 1 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index").Logger(),
	)
	if err != nil {
		panic(err)
	}

	metricsList := make([]labels.Labels, 0, len(metrics))
	ids := make([]types.MetricID, 0, len(metrics))
	expirations := make([]time.Time, 0, len(metrics))
	now := time.Now()

	for id, lbls := range metrics {
		lbls := labels.FromMap(lbls)
		metricsList = append(metricsList, lbls)
		ids = append(ids, id)
		expirations = append(expirations, now.Add(time.Hour))
	}

	_, err = index.InternalCreateMetric(context.Background(), start, end, metricsList, ids, expirations, false)
	if err != nil {
		panic(err)
	}

	return index
}

func bufferToStringTruncated(b []byte) string {
	if len(b) > 5000 {
		b = b[:5000]
	}

	return string(b)
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
		wantLabels labels.Labels
		want       int64
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
		wantLabels labels.Labels
		wantTTL    int64
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
		want   string
		labels labels.Labels
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

func Test_postingsForMatchers(t *testing.T) { //nolint:maintidx
	now := time.Now()
	shards := []int32{ShardForTime(now.Unix())}
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
	index1 := mockIndexFromMetrics(now, now, metrics1)

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
	index3 := mockIndexFromMetrics(now, now, metrics3)

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

	index2 := mockIndexFromMetrics(now, now, metrics2)

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
			name:  "eq-label-noexists",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"does-not-exists",
					"",
				),
			},
			want: []types.MetricID{},
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
			name:  "re-simple",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"up|node_cpu_seconds_total",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"instance",
					"remotehost:9100",
				),
			},
			want: []types.MetricID{
				MetricIDTest3,
				MetricIDTest4,
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "re-simple-capture",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"(up|node_cpu_seconds_total)",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"instance",
					"remotehost:9100",
				),
			},
			want: []types.MetricID{
				MetricIDTest3,
				MetricIDTest4,
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "re-simple-with-unknown-matcher",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"up|does_not_exist|node_cpu_seconds_total",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"instance",
					"remotehost:9100",
				),
			},
			want: []types.MetricID{
				MetricIDTest3,
				MetricIDTest4,
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "re-complex",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"(up|node_cpu_seconds_tota\\w)",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"instance",
					"remotehost:9100",
				),
			},
			want: []types.MetricID{
				MetricIDTest3,
				MetricIDTest4,
				MetricIDTest5,
				MetricIDTest6,
			},
		},
		{
			name:  "re-simple-mountpoint-missing",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"(up|node_filesystem_avail_bytes)",
				),
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"mountpoint",
					"(/|)",
				),
			},
			want: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
				MetricIDTest7,
			},
		},
		{
			name:  "re-complex-mountpoint-missing",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"(up|node_filesystem_avail_bytes)",
				),
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"mountpoint",
					"(/+|)",
				),
			},
			want: []types.MetricID{
				MetricIDTest1,
				MetricIDTest2,
				MetricIDTest3,
				MetricIDTest7,
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
			got, _, err := tt.index.idsForMatchers(context.Background(), shards, tt.matchers, 0)
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

				return
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})

		t.Run(tt.name+" direct", func(t *testing.T) {
			got, _, err := tt.index.idsForMatchers(context.Background(), shards, tt.matchers, 1000)
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

				return
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

			got, _, err := tt.index.idsForMatchers(context.Background(), shards, matchersReverse, 0)
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

				return
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sharded_postingsForMatchers(t *testing.T) { //nolint:maintidx
	t0 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	t1 := t0.Add(8 * 24 * time.Hour)
	t2 := t1.Add(8 * 24 * time.Hour)
	t3 := t2.Add(8 * 24 * time.Hour)
	t4 := t3.Add(8 * 24 * time.Hour)
	t5 := t4.Add(8 * 24 * time.Hour)
	now := t5.Add(8 * 24 * time.Hour)

	index1, err := initialize(
		context.Background(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index1").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics1IDs, _, err := index1.lookupIDs(
		context.Background(),
		[]types.LookupRequest{
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "prometheus",
					"instance": "localhost:9090",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "node_exporter",
					"instance": "localhost:9100",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "node_exporter",
					"instance": "remotehost:9100",
				}),
				Start: t1,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "node_cpu_seconds_total",
					"job":      "node_exporter",
					"instance": "remotehost:9100",
					"cpu":      "0",
					"mode":     "idle",
				}),
				Start: t0,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "node_cpu_seconds_total",
					"job":      "node_exporter",
					"instance": "remotehost:9100",
					"cpu":      "0",
					"mode":     "user",
				}),
				Start: t1,
				End:   t2,
			},
			{ // index = 5
				Labels: labels.FromMap(map[string]string{
					"__name__": "node_cpu_seconds_total",
					"job":      "node_exporter",
					"instance": "remotehost:9100",
					"cpu":      "1",
					"mode":     "user",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":   "node_filesystem_avail_bytes",
					"job":        "node_exporter",
					"instance":   "localhost:9100",
					"device":     "/dev/mapper/vg0-root",
					"fstype":     "ext4",
					"mountpoint": "/",
				}),
				Start: t1,
				End:   t5,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":    "node_filesystem_avail_bytes",
					"job":         "node_exporter",
					"instance":    "localhost:9100",
					"device":      "/dev/mapper/vg0-data",
					"fstype":      "ext4",
					"mountpoint":  "/srv/data",
					"environment": "devel",
				}),
				Start: t0,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":    "node_filesystem_avail_bytes",
					"job":         "node_exporter",
					"instance":    "remote:9100",
					"device":      "/dev/mapper/vg0-data",
					"fstype":      "ext4",
					"mountpoint":  "/srv/data",
					"environment": "production",
				}),
				Start: t0,
				End:   t5,
			},
			{ // index == 9
				Labels: labels.FromMap(map[string]string{
					"__name__":    "node_filesystem_avail_bytes",
					"job":         "node_exporter",
					"instance":    "remote:9100",
					"device":      "/dev/mapper/vg0-data",
					"fstype":      "ext4",
					"mountpoint":  "/srv/data",
					"environment": "production",
					"userID":      "42",
				}),
				Start: t2,
				End:   t4,
			},
		},
		now,
	)
	if err != nil {
		t.Error(err)
	}

	requests := make([]types.LookupRequest, 0)

	for x := 0; x < 100; x++ {
		var start, end time.Time

		for y := 0; y < 100; y++ {
			switch x % 5 {
			case 0:
				start = t0
				end = t0
			case 1:
				start = t1
				end = t3
			case 2:
				start = t2
				end = t2
			case 3:
				start = t4
				end = t4
			case 4:
				start = t3
				end = t4
			}

			requests = append(requests, types.LookupRequest{
				Labels: labels.FromMap(map[string]string{
					"__name__":   fmt.Sprintf("generated_%03d", x),
					"label_x":    fmt.Sprintf("%03d", x),
					"label_y":    fmt.Sprintf("%03d", y),
					"multiple_2": fmt.Sprintf("%v", y%2 == 0),
					"multiple_3": fmt.Sprintf("%v", y%3 == 0),
					"multiple_5": fmt.Sprintf("%v", y%5 == 0),
				}),
				Start: start,
				End:   end,
			})
		}
	}

	index2, err := initialize(
		context.Background(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index2").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics2IDs, _, err := index2.lookupIDs(context.Background(), requests, now)
	if err != nil {
		t.Fatal(err)
	}

	index3, err := initialize(
		context.Background(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index3").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics3IDs1, _, err := index3.lookupIDs(
		context.Background(),
		[]types.LookupRequest{
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "prometheus",
					"instance": "localhost:9090",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up2",
					"job":      "prometheus",
					"instance": "localhost:9090",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up2",
					"job":      "prometheus2",
					"instance": "localhost:9090",
				}),
				Start: t0,
				End:   t0,
			},
		},
		t0,
	)
	if err != nil {
		t.Error(err)
	}

	metrics3IDs2, _, err := index3.lookupIDs(
		context.Background(),
		[]types.LookupRequest{
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "prometheus",
					"instance": "localhost:9090",
				}),
				Start: t0,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up2",
					"job":      "prometheus",
					"instance": "localhost:9090",
				}),
				Start: t1,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up3",
					"job":      "prometheus2",
					"instance": "localhost:9090",
				}),
				Start: t1,
				End:   t1,
			},
		},
		t2,
	)
	if err != nil {
		t.Error(err)
	}

	metrics3IDs3, _, err := index3.lookupIDs(
		context.Background(),
		[]types.LookupRequest{
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up",
					"job":      "prometheus",
					"instance": "localhost:9090",
				}),
				Start: t3,
				End:   t3,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up2",
					"job":      "prometheus",
					"instance": "localhost:9090",
				}),
				Start: t3,
				End:   t3,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__": "up3",
					"job":      "prometheus2",
					"instance": "localhost:9090",
				}),
				Start: t4,
				End:   t5,
			},
		},
		now,
	)
	if err != nil {
		t.Error(err)
	}

	if metrics3IDs2[0] != metrics3IDs1[0] {
		t.Errorf("metrics3IDs2[0] = %d, want %d", metrics3IDs2[0], metrics3IDs1[0])
	}

	if metrics3IDs2[1] != metrics3IDs1[1] {
		t.Errorf("metrics3IDs2[1] = %d, want %d", metrics3IDs2[1], metrics3IDs1[1])
	}

	if metrics3IDs3[0] != metrics3IDs1[0] {
		t.Errorf("metrics3IDs3[0] = %d, want %d", metrics3IDs3[0], metrics3IDs1[0])
	}

	if metrics3IDs3[1] != metrics3IDs1[1] {
		t.Errorf("metrics3IDs3[1] = %d, want %d", metrics3IDs3[1], metrics3IDs1[1])
	}

	if metrics3IDs3[2] != metrics3IDs2[2] {
		t.Errorf("metrics3IDs3[2] = %d, want %d", metrics3IDs3[2], metrics3IDs1[2])
	}

	tests := []struct {
		name       string
		index      *CassandraIndex
		queryStart time.Time
		queryEnd   time.Time
		matchers   []*labels.Matcher
		want       []types.MetricID
		wantLen    int
	}{
		{
			name:       "eq-t0",
			index:      index1,
			queryStart: t0,
			queryEnd:   t0,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				metrics1IDs[0],
				metrics1IDs[1],
			},
		},
		{
			name:       "eq-t0t1",
			index:      index1,
			queryStart: t0,
			queryEnd:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				metrics1IDs[0],
				metrics1IDs[1],
				metrics1IDs[2],
			},
		},
		{
			name:       "eq-t1t5",
			index:      index1,
			queryStart: t1,
			queryEnd:   t5,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				metrics1IDs[2],
			},
		},
		{
			name:       "eq-t2now",
			index:      index1,
			queryStart: t2,
			queryEnd:   now,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{},
		},
		{
			name:       "eq-eq",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[4],
				metrics1IDs[5],
			},
		},
		{
			name:       "eq-eq-t2",
			index:      index1,
			queryStart: t2,
			queryEnd:   t2,
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
				metrics1IDs[4],
			},
		},
		{
			name:       "eq-eq-t0+",
			index:      index1,
			queryStart: t0,
			queryEnd:   t0.Add(time.Hour),
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
				metrics1IDs[5],
			},
		},
		{
			name:       "eq-neq",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[3],
			},
		},
		{
			name:       "eq-nolabel",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[6],
			},
		},
		{
			name:       "eq-nolabel-t3",
			index:      index1,
			queryStart: t3,
			queryEnd:   t3,
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
				metrics1IDs[6],
			},
		},
		{
			name:       "eq-nolabel-t0",
			index:      index1,
			queryStart: t0,
			queryEnd:   t0,
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
			want: []types.MetricID{},
		},
		{
			name:       "eq-label",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[7],
				metrics1IDs[8],
				metrics1IDs[9],
			},
		},
		{
			name:       "eq-label-t0t1",
			index:      index1,
			queryStart: t0,
			queryEnd:   t1,
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
				metrics1IDs[7],
				metrics1IDs[8],
			},
		},
		{
			name:       "eq-label-t1t2",
			index:      index1,
			queryStart: t1,
			queryEnd:   t2,
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
				metrics1IDs[7],
				metrics1IDs[8],
				metrics1IDs[9],
			},
		},
		{
			name:       "eq-label-t2t5",
			index:      index1,
			queryStart: t2,
			queryEnd:   t5,
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
				metrics1IDs[8],
				metrics1IDs[9],
			},
		},
		{
			name:       "re",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"u.",
				),
			},
			want: []types.MetricID{
				metrics1IDs[0],
				metrics1IDs[1],
				metrics1IDs[2],
			},
		},
		{
			name:       "re-re",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[4],
				metrics1IDs[5],
			},
		},
		{
			name:       "re-nre",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[3],
			},
		},
		{
			name:       "re-re_nolabel",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[6],
			},
		},
		{
			name:       "re-re_label",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[7],
				metrics1IDs[8],
				metrics1IDs[9],
			},
		},
		{
			name:       "re-re*",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[6],
				metrics1IDs[7],
				metrics1IDs[8],
				metrics1IDs[9],
			},
		},
		{
			name:       "re-nre*",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
			name:       "eq-nre_empty_and_devel",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[8],
				metrics1IDs[9],
			},
		},
		{
			name:       "eq-nre-eq same label",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[7],
			},
		},
		{
			name:       "eq-eq-no_label",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[8],
			},
		},
		{
			name:       "eq-eq-eq_empty",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
			name:       "eq-eq-re_empty",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
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
				metrics1IDs[8],
				metrics1IDs[9],
			},
		},
		{
			name:       "eq_empty",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"environment",
					"",
				),
			},
			want: []types.MetricID{
				metrics1IDs[0],
				metrics1IDs[1],
				metrics1IDs[2],
				metrics1IDs[3],
				metrics1IDs[4],
				metrics1IDs[5],
				metrics1IDs[6],
			},
		},
		{
			name:       "neq_empty",
			index:      index1,
			queryStart: t0,
			queryEnd:   now,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"environment",
					"production",
				),
			},
			want: []types.MetricID{
				metrics1IDs[0],
				metrics1IDs[1],
				metrics1IDs[2],
				metrics1IDs[3],
				metrics1IDs[4],
				metrics1IDs[5],
				metrics1IDs[6],
				metrics1IDs[7],
			},
		},
		{
			name:       "index2-eq",
			index:      index2,
			queryStart: t0,
			queryEnd:   now,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"generated_042",
				),
			},
			wantLen: 100,
			want: []types.MetricID{
				metrics2IDs[42*100+0],
				metrics2IDs[42*100+1],
				metrics2IDs[42*100+2],
				// [...]
			},
		},
		{
			name:       "index2-eq-t2",
			index:      index2,
			queryStart: t2,
			queryEnd:   t2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"generated_042",
				),
			},
			wantLen: 100,
			want: []types.MetricID{
				metrics2IDs[42*100+0],
				metrics2IDs[42*100+1],
				metrics2IDs[42*100+2],
				// [...]
			},
		},
		{
			name:       "index2-eq-t1",
			index:      index2,
			queryStart: t1,
			queryEnd:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"generated_042",
				),
			},
			wantLen: 0,
			want:    []types.MetricID{},
		},
		{
			name:       "index2-eq-eq-t0t2",
			index:      index2,
			queryStart: t0,
			queryEnd:   t2,
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
				metrics2IDs[42*100+0],
				metrics2IDs[42*100+2],
				metrics2IDs[42*100+4],
				// [...]
			},
		},
		{
			name:       "index2-eq-neq-t1t2",
			index:      index2,
			queryStart: t1,
			queryEnd:   t2,
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
			wantLen: 2000,
			want: []types.MetricID{
				metrics2IDs[1*100+0],
				metrics2IDs[1*100+2],
				metrics2IDs[1*100+4],
				// [...]
			},
		},
		{
			name:       "index2-eq-neq-t4t5",
			index:      index2,
			queryStart: t4,
			queryEnd:   t5,
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
			wantLen: 2000,
			want: []types.MetricID{
				metrics2IDs[3*100+0],
				metrics2IDs[3*100+2],
				metrics2IDs[3*100+4],
				// [...]
			},
		},
		{
			name:       "index2-eq-neq-2",
			index:      index2,
			queryStart: t0,
			queryEnd:   now,
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
			name:       "index2-eq-neq-2-t2",
			index:      index2,
			queryStart: t2,
			queryEnd:   t2,
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
			name:       "index2-re-neq-eq-neq-t2",
			index:      index2,
			queryStart: t2,
			queryEnd:   t2,
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
			wantLen: 30,
			want: []types.MetricID{
				metrics2IDs[41*100+0],
				metrics2IDs[41*100+10],
				metrics2IDs[41*100+20],
				// [...]
			},
		},
		{
			name:       "index2-re-neq-eq-neq-t4",
			index:      index2,
			queryStart: t4,
			queryEnd:   t4,
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
			wantLen: 40,
			want: []types.MetricID{
				metrics2IDs[43*100+0],
				metrics2IDs[43*100+10],
				metrics2IDs[43*100+20],
				// [...]
			},
		},
		{
			name:       "index2-re-nre-eq-neq-t0",
			index:      index2,
			queryStart: t0,
			queryEnd:   t0,
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
			wantLen: 10,
			want: []types.MetricID{
				metrics2IDs[45*100+0],
				metrics2IDs[45*100+10],
				metrics2IDs[45*100+20],
				// [...]
			},
		},
		{
			name:       "index2-re-nre-eq-neq-t1t2",
			index:      index2,
			queryStart: t1,
			queryEnd:   t2,
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
			wantLen: 30,
			want: []types.MetricID{
				metrics2IDs[41*100+0],
				metrics2IDs[41*100+10],
				metrics2IDs[41*100+20],
				// [...]
			},
		},
		{
			name:       "index3-metric1-t0",
			index:      index3,
			queryStart: t0,
			queryEnd:   t0,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				metrics3IDs1[0],
			},
		},
		{
			name:       "index3-metric1-t1",
			index:      index3,
			queryStart: t1,
			queryEnd:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				metrics3IDs1[0],
			},
		},
		{
			name:       "index3-metric1-t2",
			index:      index3,
			queryStart: t2,
			queryEnd:   t2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{},
		},
		{
			name:       "index3-metric1-t3",
			index:      index3,
			queryStart: t3,
			queryEnd:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				metrics3IDs1[0],
			},
		},
		{
			name:       "index3-metric1-t0t3",
			index:      index3,
			queryStart: t0,
			queryEnd:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up",
				),
			},
			want: []types.MetricID{
				metrics3IDs1[0],
			},
		},
		{
			name:       "index3-metric2-t1",
			index:      index3,
			queryStart: t1,
			queryEnd:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up2",
				),
			},
			want: []types.MetricID{
				metrics3IDs1[1],
			},
		},
		{
			name:       "index3-metric2-t3",
			index:      index3,
			queryStart: t3,
			queryEnd:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up2",
				),
			},
			want: []types.MetricID{
				metrics3IDs1[1],
			},
		},
		{
			name:       "index3-metric3-t1",
			index:      index3,
			queryStart: t1,
			queryEnd:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up3",
				),
			},
			want: []types.MetricID{
				metrics3IDs2[2],
			},
		},
		{
			name:       "index3-metric3-t4",
			index:      index3,
			queryStart: t4,
			queryEnd:   t4,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up3",
				),
			},
			want: []types.MetricID{
				metrics3IDs2[2],
			},
		},
		{
			name:       "index3-metric3-t5",
			index:      index3,
			queryStart: t5,
			queryEnd:   t5,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"up3",
				),
			},
			want: []types.MetricID{
				metrics3IDs2[2],
			},
		},
	}

	for _, tt := range tests {
		shards, err := tt.index.getTimeShards(context.Background(), tt.queryStart, tt.queryEnd, false)
		if err != nil {
			t.Errorf("getTimeShards() error = %v", err)
		}

		shardsAll, err := tt.index.getTimeShards(context.Background(), tt.queryStart, tt.queryEnd, true)
		if err != nil {
			t.Errorf("getTimeShards(returnAll=true) error = %v", err)
		}

		shards = shardsAll

		t.Run(tt.name+" shardsAll", func(t *testing.T) {
			got, _, err := tt.index.idsForMatchers(context.Background(), shardsAll, tt.matchers, 0)
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

				return
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})

		t.Run(tt.name, func(t *testing.T) {
			got, _, err := tt.index.idsForMatchers(context.Background(), shards, tt.matchers, 0)
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

				return
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})

		t.Run(tt.name, func(t *testing.T) {
			got, _, err := tt.index.idsForMatchers(context.Background(), shards, tt.matchers, 0)
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

				return
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})

		t.Run(tt.name+" direct", func(t *testing.T) {
			got, _, err := tt.index.idsForMatchers(context.Background(), shards, tt.matchers, 1000)
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

				return
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

			got, _, err := tt.index.idsForMatchers(context.Background(), shards, matchersReverse, 0)
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

				return
			}
			got = got[:len(tt.want)]

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("postingsForMatchers() = %v, want %v", got, tt.want)
			}
		})
	}

	buffer := bytes.NewBuffer(nil)

	hadIssue, err := index1.verify(context.Background(), now, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	hadIssue, err = index2.verify(context.Background(), now, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	hadIssue, err = index3.verify(context.Background(), now, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}
}

func Test_PilosaBugs(t *testing.T) {
	b1 := roaring.NewBTreeBitmap()
	if _, err := b1.AddN(1, 2); err != nil {
		t.Fatal(err)
	}

	b2 := roaring.NewBTreeBitmap()
	if _, err := b2.AddN(3); err != nil {
		t.Fatal(err)
	}

	// Clone() is broken.
	// b1Bis := b1.Clone()
	// b2Bis := b2.Clone()
	// This is workaround clone
	b1Bis := roaring.NewBTreeBitmap()
	b1Bis.UnionInPlace(b1)

	b2Bis := roaring.NewBTreeBitmap()
	b2Bis.UnionInPlace(b2)

	b1.UnionInPlace(b2)

	if b1.Count() != 3 {
		t.Errorf("b1.Count() = %d, want 3", b1.Count())
	}

	if b1Bis.Count() != 2 {
		t.Errorf("b1.Count() = %d, want 2", b1Bis.Count())
	}

	b1Bis.UnionInPlace(b2Bis)

	if b1Bis.Count() != 3 {
		t.Errorf("b1.Count() = %d, want 3", b1Bis.Count())
	}
}

func Test_PilosaBugs2(t *testing.T) {
	b1 := roaring.NewBTreeBitmap()
	b2 := roaring.NewBTreeBitmap()

	// Flip is broken... this is likely because Flip use NewBitmap which is
	// known to be broken.
	b1 = b1.Flip(1, 3)

	// serialization/deserialization fix it
	buffer := bytes.NewBuffer(nil)
	if _, err := b1.WriteTo(buffer); err != nil {
		t.Fatal(err)
	}

	b1 = roaring.NewBTreeBitmap()
	if err := b1.UnmarshalBinary(buffer.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Alternative to Flip
	if _, err := b2.AddN(1, 2, 3); err != nil {
		t.Fatal(err)
	}

	for _, bitmap := range []*roaring.Bitmap{b1, b2} {
		tmp := roaring.NewBTreeBitmap()

		_ = tmp.Xor(bitmap)

		if _, err := bitmap.AddN(5); err != nil {
			t.Fatal(err)
		}

		if _, err := bitmap.AddN(6); err != nil {
			t.Fatal(err)
		}

		if !bitmap.Contains(5) {
			t.Error("pilosa bug...")
		}
	}
}

func Test_PilosaSerialization(t *testing.T) {
	want := roaring.NewBTreeBitmap()
	want = want.Flip(1, 36183)
	_, _ = want.RemoveN(31436, 31437)

	/*
		use the following to get gotBinary value

		buffer := bytes.NewBuffer(nil)
		want.WriteTo(buffer)
		fmt.Println(buffer.Bytes())
	*/

	gotBinary := []byte{
		60, 48, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 84, 141, 24, 0, 0, 0, 2, 0, 1, 0, 203, 122, 206, 122, 87, 141,
	}
	got := roaring.NewBTreeBitmap()

	err := got.UnmarshalBinary(gotBinary)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(want.Slice(), got.Slice()); diff != "" {
		t.Errorf("got != want: %s", diff)
	}
}

func loadBitmap(filename string) (*roaring.Bitmap, error) {
	tmp := roaring.NewBTreeBitmap()

	bufferHex, err := os.ReadFile(filename)
	if err != nil {
		return tmp, err
	}

	// remove all new-line
	bufferHex = bytes.ReplaceAll(bufferHex, []byte("\n"), nil)

	buffer := make([]byte, hex.DecodedLen(len(bufferHex)))

	_, err = hex.Decode(buffer, bufferHex)
	if err != nil {
		return tmp, err
	}

	err = tmp.UnmarshalBinary(buffer)
	if err != nil {
		return tmp, err
	}

	return tmp, nil
}

func Test_freeFreeID(t *testing.T) { //nolint:maintidx
	compact := roaring.NewBTreeBitmap()
	compact = compact.Flip(1, 5000)

	spare := compact.Clone()
	if _, err := spare.RemoveN(42); err != nil {
		t.Fatal(err)
	}

	if _, err := spare.RemoveN(1337); err != nil {
		t.Fatal(err)
	}

	if _, err := spare.RemoveN(44); err != nil {
		t.Fatal(err)
	}

	compactLarge := roaring.NewBTreeBitmap()
	compactLarge = compactLarge.Flip(1, 1e5)

	spareLarge := compactLarge.Clone()
	if _, err := spareLarge.RemoveN(65539); err != nil {
		t.Fatal(err)
	}

	if _, err := spareLarge.RemoveN(65540); err != nil {
		t.Fatal(err)
	}

	if _, err := spareLarge.RemoveN(70000); err != nil {
		t.Fatal(err)
	}

	spareZone := compactLarge.Clone()
	spareZone = spareZone.Flip(200, 500)
	spareZone = spareZone.Flip(1e4, 2e4)

	startAndEnd := roaring.NewBTreeBitmap()
	startAndEnd = startAndEnd.Flip(0, 1e4)
	// We want to use, but it WAY to slow...
	// startAndEnd = startAndEnd.Flip(math.MaxUint64 - 1e4, math.MaxUint64)
	// Do Add() instead
	for n := uint64(math.MaxUint64 - 1e4); true; n++ {
		if _, err := startAndEnd.AddN(n); err != nil {
			t.Fatal(err)
		}

		if n == math.MaxUint64 {
			break
		}
	}

	freeAtSplit1 := compactLarge.Clone()
	if _, err := freeAtSplit1.RemoveN(50000); err != nil {
		t.Fatal(err)
	}

	freeAtSplit2 := compactLarge.Clone()
	if _, err := freeAtSplit2.RemoveN(50001); err != nil {
		t.Fatal(err)
	}

	freeAtSplit3 := compactLarge.Clone()
	if _, err := freeAtSplit3.RemoveN(49999); err != nil {
		t.Fatal(err)
	}

	// test bug that occurred with all IDs assigned between 1 to 36183 (included)
	// but 31436 and 31437.
	// This is actually a bug in pilosa :(
	bug1 := roaring.NewBTreeBitmap()
	bug1 = bug1.Flip(1, 36183)
	_, _ = bug1.RemoveN(31436, 31437)
	bug1.Optimize()

	bug2, err := loadBitmap("testdata/large.hex")
	if err != nil {
		t.Error(err)

		return
	}

	compactFile, err := loadBitmap("testdata/compact-small.hex")
	if err != nil {
		t.Error(err)

		return
	}

	type testStruct struct {
		name       string
		bitmap     *roaring.Bitmap
		wants      []uint64
		numberHole int // number of hole == free slot before max+1
		maxIter    int
	}

	tests := []testStruct{
		{
			name:       "empty",
			bitmap:     roaring.NewBTreeBitmap(),
			wants:      []uint64{1, 2, 3, 4},
			numberHole: 0,
		},
		{
			name:       "compact",
			bitmap:     compact,
			wants:      []uint64{5001, 5002, 5003, 5004},
			numberHole: 0,
		},
		{
			name:       "spare",
			bitmap:     spare,
			wants:      []uint64{42, 44, 1337, 5001},
			numberHole: 3,
		},
		{
			name:       "compactLarge",
			bitmap:     compactLarge,
			wants:      []uint64{1e5 + 1, 1e5 + 2, 1e5 + 3},
			numberHole: 0,
		},
		{
			name:       "spareLarge",
			bitmap:     spareLarge,
			wants:      []uint64{65539, 65540, 70000, 1e5 + 1},
			numberHole: 3,
		},
		{
			name:       "spareZone",
			bitmap:     spareZone,
			wants:      []uint64{200, 201, 202, 203},
			numberHole: 10302,
			maxIter:    2000,
		},
		{
			name:       "startAndEnd",
			bitmap:     startAndEnd,
			wants:      []uint64{10001, 10002, 10003, 10004},
			numberHole: -1, // special value to tell test case to do NOT attempt to fill hole
			maxIter:    2000,
		},
		{
			name:       "bug1",
			bitmap:     bug1,
			wants:      []uint64{31436, 31437, 36184, 36185},
			numberHole: 2,
		},
		{
			name:       "bug2",
			bitmap:     bug2,
			wants:      []uint64{179940, 215223, 215673, 215692},
			numberHole: 1376,
		},
		{
			name:       "compact-file",
			bitmap:     compactFile,
			wants:      []uint64{180120, 180121, 180122, 180123},
			numberHole: 0,
		},
		{
			name:       "freeAtSplit1",
			bitmap:     freeAtSplit1,
			wants:      []uint64{50000, 1e5 + 1},
			numberHole: 1,
		},
		{
			name:       "freeAtSplit2",
			bitmap:     freeAtSplit2,
			wants:      []uint64{50001, 1e5 + 1},
			numberHole: 1,
		},
		{
			name:       "freeAtSplit3",
			bitmap:     freeAtSplit3,
			wants:      []uint64{49999, 1e5 + 1},
			numberHole: 1,
		},
	}

	// Create 12 random bitmap, each with 1e6 metrics and 10, 100 and 1000 random hole
	rnd := rand.New(rand.NewSource(42))

	for n := 0; n < 12; n++ {
		var holdCount int

		// When using short, only add one random bitmap
		if testing.Short() && n > 1 {
			continue
		}

		switch n % 3 {
		case 0:
			holdCount = 10
		case 1:
			holdCount = 100
		case 2:
			holdCount = 1000
		}

		bitmap := roaring.NewBTreeBitmap()
		bitmap = bitmap.Flip(1, 1e6)
		// After using Flip, the bitmap is broken. Reload it from bytes

		tmp := bytes.NewBuffer(nil)
		if _, err := bitmap.WriteTo(tmp); err != nil {
			t.Fatal(err)
		}

		bitmap = roaring.NewBTreeBitmap()
		if err := bitmap.UnmarshalBinary(tmp.Bytes()); err != nil {
			t.Fatal(err)
		}

		holdsInt := rnd.Perm(1e6)[:holdCount]
		for _, v := range holdsInt {
			if _, err := bitmap.RemoveN(uint64(v)); err != nil {
				t.Fatal(err)
			}
		}

		sort.Ints(holdsInt)

		holds := make([]uint64, 0, len(holdsInt)+1)

		for _, v := range holdsInt {
			holds = append(holds, uint64(v))
		}

		holds = append(holds, 1e6+1)

		tests = append(tests, testStruct{
			name:       fmt.Sprintf("random-%d", n),
			bitmap:     bitmap,
			wants:      holds,
			numberHole: holdCount,
			maxIter:    1100,
		})
	}

	for _, tt := range tests {
		if testing.Short() && (tt.name == "startAndEnd" || tt.name == "spareZone") {
			t.Skip()
		}

		tt := tt
		buffer := bytes.NewBuffer(nil)

		_, err := tt.bitmap.WriteTo(buffer)
		if err != nil {
			t.Error(err)

			return
		}

		saveEveryList := []int{0, 0, 1000, 2}
		if testing.Short() {
			saveEveryList = []int{0, 1000}
		}

		for i, saveEvery := range saveEveryList {
			saveEvery := saveEvery

			allPosting := tt.bitmap
			if i > 0 {
				// unless first test, reload bitmap from bytes
				allPosting = roaring.NewBTreeBitmap()

				err = allPosting.UnmarshalBinary(buffer.Bytes())
				if err != nil {
					t.Error(err)

					return
				}
			}

			t.Run(fmt.Sprintf("%s-%d-%d", tt.name, i, saveEvery), func(t *testing.T) {
				t.Parallel()

				max := allPosting.Max()
				count := allPosting.Count()

				if max-count != uint64(tt.numberHole) && tt.numberHole != -1 {
					t.Errorf("Had %d hole, want %d", max-count, tt.numberHole)
				}

				iterCount := 1000
				if iterCount < 2*saveEvery {
					iterCount = 2 * saveEvery
				}

				if iterCount < tt.numberHole+10 {
					iterCount = tt.numberHole + 10
				}

				if tt.maxIter > 0 && iterCount > tt.maxIter {
					iterCount = tt.maxIter
				}

				for n := 0; n < iterCount; n++ {
					newID := findFreeID(allPosting)

					if newID == 0 {
						t.Errorf("unable to find newID after %d loop", n)
					}

					if len(tt.wants) > n && newID != tt.wants[n] {
						t.Errorf("loop %d: newID = %d, want %d", n, newID, tt.wants[n])
					}

					if n < tt.numberHole && newID == max+1 {
						t.Errorf("on loop %d, had to use workaround but allPosting should have free hole", n)
					}

					if allPosting.Contains(newID) {
						t.Errorf("newID = %d isn't free", newID)
					}

					_, err := allPosting.AddN(newID)
					if err != nil {
						t.Error(err)
					}

					if n == tt.numberHole {
						gotHole := allPosting.Max() - allPosting.Count()
						if gotHole != 0 {
							t.Errorf("on loop %d, expected a compact bitmap, still had %d hole (last ID %d)", n, gotHole, newID)
						}
					}

					if saveEvery > 0 && n%saveEvery == 0 {
						tmp := bytes.NewBuffer(nil)

						_, err = allPosting.WriteTo(tmp)
						if err != nil {
							t.Error(err)

							return
						}

						allPosting = roaring.NewBTreeBitmap()

						err = allPosting.UnmarshalBinary(tmp.Bytes())
						if err != nil {
							t.Error(err)

							return
						}
					}
				}
			})
		}
	}
}

func Benchmark_freeFreeID(b *testing.B) {
	compact := roaring.NewBTreeBitmap()
	compact = compact.Flip(1, 5000)

	spare := compact.Clone()

	if _, err := spare.RemoveN(42); err != nil {
		b.Fatal(err)
	}

	if _, err := spare.RemoveN(1337); err != nil {
		b.Fatal(err)
	}

	if _, err := spare.RemoveN(44); err != nil {
		b.Fatal(err)
	}

	compactLarge := roaring.NewBTreeBitmap()
	compactLarge = compactLarge.Flip(1, 5e5)

	spareLarge := compactLarge.Clone()

	if _, err := spareLarge.RemoveN(65539); err != nil {
		b.Fatal(err)
	}

	if _, err := spareLarge.RemoveN(65540); err != nil {
		b.Fatal(err)
	}

	if _, err := spareLarge.RemoveN(70000); err != nil {
		b.Fatal(err)
	}

	spareZone := compactLarge.Clone()
	spareZone = spareZone.Flip(200, 500)
	spareZone = spareZone.Flip(1e4, 2e4)

	compactSuperLarge := compactLarge.Clone()
	compactSuperLarge = compactSuperLarge.Flip(5e5+1, 5e6)

	compactLargerest := compactSuperLarge.Clone()
	compactLargerest = compactLargerest.Flip(5e6+1, 5e7)

	spareSuperLarge := compactSuperLarge.Clone()

	if _, err := spareSuperLarge.AddN(2e9, 3e9, 4e9, 1e10, 1e10+1, 1e13); err != nil {
		b.Fatal(err)
	}

	spareSuperLarge.Flip(1e2, 1e3)

	if _, err := spareSuperLarge.RemoveN(4242, 4299, 4288, 1e9, 2e9); err != nil {
		b.Fatal(err)
	}

	spareLargerest := compactLargerest.Clone()

	if _, err := spareLargerest.AddN(2e9, 3e9, 4e9, 1e10, 1e10+1, 1e13); err != nil {
		b.Fatal(err)
	}

	spareLargerest.Flip(1e2, 1e3)

	if _, err := spareLargerest.RemoveN(4242, 4299, 4288, 1e9, 2e9); err != nil {
		b.Fatal(err)
	}

	startAndEnd := roaring.NewBTreeBitmap()
	startAndEnd = startAndEnd.Flip(0, 1e4)

	for n := uint64(math.MaxUint64); true; n++ {
		_, err := startAndEnd.AddN(n)
		if err != nil {
			b.Fatal(err)
		}

		if n == math.MaxUint64 {
			break
		}
	}

	tests := []struct {
		bitmap *roaring.Bitmap
		name   string
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
			name:   "compactSuperLarge",
			bitmap: compactSuperLarge,
		},
		{
			name:   "compactLargerest",
			bitmap: compactLargerest,
		},
		{
			name:   "spareLarge",
			bitmap: spareLarge,
		},
		{
			name:   "spareSuperLarge",
			bitmap: spareSuperLarge,
		},
		{
			name:   "spareLargerest",
			bitmap: spareLargerest,
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
				_ = findFreeID(tt.bitmap)
			}
		})
	}
}

// Test_cache will run a small scenario on the index to check that in-memory cache works.
func Test_cache(t *testing.T) {
	defaultTTL := 365 * 24 * time.Hour
	store := &mockStore{}
	lock := &mockLockFactory{}
	states := &mockState{}
	t0 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)

	index1, err := initialize(
		context.Background(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       lock,
			States:            states,
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index1").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	index2, err := initialize(
		context.Background(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       lock,
			States:            states,
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index2").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	labelsList := make([]labels.Labels, 3000)
	for n := range labelsList {
		lbls := map[string]string{
			"__name__": "filler",
			"id":       strconv.FormatInt(int64(n), 10),
		}

		if n >= 2000 {
			lbls[timeToLiveLabelName] = "3600"
		}

		labelsList[n] = labels.FromMap(lbls)
	}

	tests := []struct {
		name       string
		labelsList []labels.Labels
		indexes    []*CassandraIndex
	}{
		{
			name:       "simple",
			labelsList: labelsList[0:1000],
			indexes:    []*CassandraIndex{index1, index2},
		},
		{
			name:       "simple-index2-first",
			labelsList: labelsList[1000:2000],
			indexes:    []*CassandraIndex{index2, index1},
		},
		{
			name:       "with-ttl",
			labelsList: labelsList[2000:3000],
			indexes:    []*CassandraIndex{index1, index2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, idx := range tt.indexes {
				countBefore := store.queryCount
				_, _, err = idx.lookupIDs(context.Background(), toLookupRequests(tt.labelsList, t0), t0)
				if err != nil {
					t.Fatal(err)
				}
				delta := store.queryCount - countBefore

				if delta <= 0 {
					t.Errorf("First lookup on indexes[%d] caused %d query to store, want > 0", i, delta)
				}

				countBefore = store.queryCount
				_, _, err = idx.lookupIDs(context.Background(), toLookupRequests(tt.labelsList, t0), t0)
				if err != nil {
					t.Fatal(err)
				}
				delta = store.queryCount - countBefore

				if delta > 0 {
					t.Errorf("Second lookup on indexes[%d] caused %d query to store, want == 0", i, delta)
				}
			}

			for i, idx := range tt.indexes {
				countBefore := store.queryCount
				_, _, err = idx.lookupIDs(context.Background(), toLookupRequests(tt.labelsList, t0), t0)
				if err != nil {
					t.Fatal(err)
				}
				delta := store.queryCount - countBefore

				if delta > 0 {
					t.Errorf("Third lookup on indexes[%d] caused %d query to store, want == 0", i, delta)
				}
			}
		})
	}
}

// Test_cluster will run a small scenario on the index to check cluster SquirrelDB.
func Test_cluster(t *testing.T) { //nolint:maintidx
	defaultTTL := 365 * 24 * time.Hour
	store := &mockStore{}
	lock := &mockLockFactory{}
	states := &mockState{}

	index1, err := initialize(
		context.Background(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       lock,
			States:            states,
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index1").Logger(),
	)
	if err != nil {
		t.Error(err)
	}

	metrics := []map[string]string{
		{
			"__name__":    "up",
			"instance":    "index1",
			"description": "Metrics created by index1",
		},
		{
			"__name__":    "up",
			"instance":    "index2",
			"description": "Metrics created by index2",
		},
		{
			"__name__":    "up2",
			"instance":    "index1",
			"description": "index1, one month later",
		},
		{
			"__name__":    "up2",
			"instance":    "index2",
			"description": "index2, two month later",
		},
	}
	metricsID := make([]types.MetricID, len(metrics))

	t0 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	t1 := t0.Add(24 * 30 * time.Hour)
	t2 := t1.Add(24 * 30 * time.Hour)
	t3 := t2.Add(24 * 30 * time.Hour)
	t4 := t3.Add(24 * 30 * time.Hour)
	t5 := t4.Add(24 * 30 * time.Hour)
	t6 := t5.Add(24 * 30 * time.Hour)

	tmp, _, err := index1.lookupIDs(
		context.Background(),
		toLookupRequests(
			[]labels.Labels{
				labels.FromMap(metrics[0]),
			},
			t0,
		),
		t0,
	)
	if err != nil {
		t.Fatal(err)
	}

	metricsID[0] = tmp[0]

	index2, err := initialize(
		context.Background(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       lock,
			States:            states,
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index2").Logger(),
	)
	if err != nil {
		t.Error(err)
	}

	tmp, _, err = index2.lookupIDs(
		context.Background(),
		toLookupRequests(
			[]labels.Labels{
				labels.FromMap(metrics[1]),
				labels.FromMap(metrics[0]),
			},
			t0,
		),
		t0,
	)
	if err != nil {
		t.Fatal(err)
	}

	metricsID[1] = tmp[0]

	if tmp[1] != metricsID[0] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[1], metricsID[0])
	}

	tmp, _, err = index1.lookupIDs(
		context.Background(),
		toLookupRequests(
			[]labels.Labels{
				labels.FromMap(metrics[0]),
				labels.FromMap(metrics[1]),
			},
			t0,
		),
		t0,
	)
	if err != nil {
		t.Fatal(err)
	}

	if tmp[0] != metricsID[0] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[0], metricsID[0])
	}

	if tmp[1] != metricsID[1] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[1], metricsID[1])
	}

	tmp, _, err = index1.lookupIDs(
		context.Background(),
		toLookupRequests(
			[]labels.Labels{
				labels.FromMap(metrics[0]),
				labels.FromMap(metrics[1]),
				labels.FromMap(metrics[2]),
			},
			t1,
		),
		t1,
	)
	if err != nil {
		t.Error(err)
	}

	{
		buffer := bytes.NewBuffer(nil)
		hadIssue, err := index1.verify(context.Background(), t0, buffer, false, false)
		if err != nil {
			t.Error(err)
		}
		if hadIssue {
			t.Fatalf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
		}
	}

	metricsID[2] = tmp[2]

	if tmp[0] != metricsID[0] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[0], metricsID[0])
	}

	if tmp[1] != metricsID[1] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[1], metricsID[1])
	}

	tmp, _, err = index2.lookupIDs(
		context.Background(),
		toLookupRequests(
			[]labels.Labels{
				labels.FromMap(metrics[3]),
				labels.FromMap(metrics[2]),
				labels.FromMap(metrics[1]),
			},
			t2,
		),
		t2,
	)
	if err != nil {
		t.Error(err)
	}

	metricsID[3] = tmp[0]

	if tmp[0] != metricsID[3] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[0], metricsID[3])
	}

	if tmp[1] != metricsID[2] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[1], metricsID[2])
	}

	if tmp[2] != metricsID[1] {
		t.Errorf("lookupIDs(metrics[0]) = %d, want %d", tmp[2], metricsID[1])
	}

	// Do some concurrency tests
	labelsList := make([]labels.Labels, 10000)
	for n := range labelsList {
		lbls := map[string]string{
			"__name__": "filler",
			"id":       strconv.FormatInt(int64(n), 10),
		}
		labelsList[n] = labels.FromMap(lbls)
	}

	batchSize := 100
	workerCount := 4

	for n := 0; n < workerCount; n++ {
		n := n

		t.Run(fmt.Sprintf("worker-%d", n), func(t *testing.T) {
			t.Parallel()

			index := index1

			if n%2 == 1 {
				index = index2
			}

			start := n * len(labelsList) / workerCount
			end := (n+1)*len(labelsList)/workerCount - 1

			current := start
			for current < end {
				idxEnd := current + batchSize
				if idxEnd > end {
					idxEnd = end
				}

				_, _, err := index.lookupIDs(context.Background(), toLookupRequests(labelsList[current:idxEnd], t3), t3)
				if err != nil {
					t.Fatal(err)
				}

				current = idxEnd
			}
		})
	}

	tmp, _, err = index1.lookupIDs(context.Background(), toLookupRequests(labelsList, t3), t4)
	if err != nil {
		t.Fatal(err)
	}

	tmp2, _, err := index2.lookupIDs(context.Background(), toLookupRequests(labelsList, t5), t5)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(tmp, tmp2) {
		t.Errorf("Index don't have the same IDs")
	}

	for index1.RunOnce(context.Background(), t5) {
	}

	for index2.RunOnce(context.Background(), t5) {
	}

	labelsList = []labels.Labels{
		labels.FromMap(map[string]string{
			"__name__":          "expiration_conflict",
			timeToLiveLabelName: "60",
		}),
	}

	labelsList2 := []labels.Labels{
		labels.FromMap(map[string]string{
			"__name__":          "expiration_conflict2",
			timeToLiveLabelName: "60",
		}),
	}

	tmp, _, err = index1.lookupIDs(context.Background(), toLookupRequests(labelsList, t5), t5)
	if err != nil {
		t.Fatal(err)
	}

	tmp2, _, err = index2.lookupIDs(context.Background(), toLookupRequests(labelsList, t5), t5)
	if err != nil {
		t.Fatal(err)
	}

	if tmp[0] != tmp2[0] {
		t.Errorf("lookupIDs() = %d, want %d", tmp2[0], tmp2[0])
	}

	for index1.RunOnce(context.Background(), t6) {
	}

	tmp, _, err = index1.lookupIDs(context.Background(), toLookupRequests(labelsList2, t6), t6)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = index2.lookupIDs(context.Background(), toLookupRequests(labelsList, t6), t6)
	if err != nil {
		t.Fatal(err)
	}

	for index2.RunOnce(context.Background(), t6) {
	}

	tmp2, _, err = index2.lookupIDs(context.Background(), toLookupRequests(labelsList, t6), t6)
	if err != nil {
		t.Fatal(err)
	}

	if tmp[0] == tmp2[0] {
		t.Errorf("lookupIDs() = %d, want != %d", tmp2[0], tmp[0])
	}

	buffer := bytes.NewBuffer(nil)

	hadIssue, err := index1.verify(context.Background(), t6, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	hadIssue, err = index2.verify(context.Background(), t6, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}
}

// Test_expiration will run a small scenario on the index to check expiration.
func Test_expiration(t *testing.T) { //nolint:maintidx
	// For this test, the default TTL must be smaller than longTTL.
	// It should be big enough to be kept until t5 (months if shortTTL & update delays are days)
	defaultTTL := 365 * 24 * time.Hour
	shortTTL := 2 * 24 * time.Hour
	longTTL := 375 * 24 * time.Hour
	// current implementation only delete metrics expired the day before
	implementationDelay := 24 * time.Hour
	// updateDelay is the delay after which we are guaranteed to trigger an TTL update
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
		{
			"__name__":    "up",
			"description": "The metrics without TTL that use default TTL",
		},
		{
			"__name__":    "ttl",
			"unit":        "month",
			"value":       "13",
			"description": "The metrics with TTL set to 13 months",
		},
		{
			"__name__":    "ttl",
			"unit":        "day",
			"value":       "2",
			"description": "The metrics with TTL set to 2 months",
		},
		{
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

	for n := 1; n < len(metrics); n++ {
		secondTTL := int64(metricsTTL[n].Seconds())
		metrics[n]["__ttl__"] = strconv.FormatInt(secondTTL, 10)
	}

	store := &mockStore{}

	index, err := initialize(
		context.Background(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       &mockLockFactory{},
			States:            &mockState{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Error(err)
	}

	var ttls []int64

	labelsList := make([]labels.Labels, len(metrics))
	for i, m := range metrics {
		labelsList[i] = labelsMapToList(m, false)
	}

	metricsID, ttls, err := index.lookupIDs(context.Background(), toLookupRequests(labelsList, t0), t0)
	if err != nil {
		t.Fatal(err)
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

	buffer := bytes.NewBuffer(nil)

	hadIssue, err := index.verify(context.Background(), t0, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	index.expire(t0)
	_, _ = index.cassandraExpire(context.Background(), t0)

	allIds, err := index.AllIDs(context.Background(), t0, t0)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIds))
	}

	hadIssue, err = index.verify(context.Background(), t0, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	labelsList[3] = labelsMapToList(metrics[3], false)

	ids, ttls, err := index.lookupIDs(context.Background(), toLookupRequests(labelsList[3:4], t1), t1)
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

	index.applyExpirationUpdateRequests(context.Background())
	// metrics[3] was moved to a new expiration slot
	if len(store.expiration) != 4 {
		t.Errorf("len(store.expiration) = %v, want 4", len(store.expiration))
	}

	for _, id := range []types.MetricID{metricsID[2], metricsID[3]} {
		expire := store.id2expiration[id].Truncate(24 * time.Hour)
		bitmap := roaring.NewBTreeBitmap()

		err = bitmap.UnmarshalBinary(store.expiration[expire])
		if err != nil {
			t.Fatal(err)
		}

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
		_, _ = index.cassandraExpire(context.Background(), t1)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t1)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIds))
	}

	metrics[0]["__ttl__"] = strconv.FormatInt(int64(shortTTL.Seconds()), 10)
	labelsList[0] = labelsMapToList(metrics[0], false)

	ids, ttls, err = index.lookupIDs(context.Background(), toLookupRequests(labelsList[0:1], t2), t2)
	if err != nil {
		t.Error(err)

		return // can't continue, lock may be held
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
		_, _ = index.cassandraExpire(context.Background(), t2)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t2)
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
		_, _ = index.cassandraExpire(context.Background(), t3)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t3)
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

	_, _, err = index.lookupIDs(context.Background(), toLookupRequests(labelsList, t3), t3)
	if err != nil {
		t.Error(err)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t3)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 2+expireBatchSize+10 {
		t.Errorf("len(allIds) = %d, want %d", len(allIds), 2+expireBatchSize+10)
	}

	index.expire(t4)

	for t := t3; t.Before(t4); t = t.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(context.Background(), t4)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t4)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 2+expireBatchSize+10 {
		t.Errorf("len(allIds) = %d, want %d", len(allIds), 2+expireBatchSize+10)
	}

	index.expire(t5)

	for t := t4; t.Before(t5); t = t.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(context.Background(), t5)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t5)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 2 {
		t.Errorf("len(allIds) = %d, want 2", len(allIds))
	}

	hadIssue, err = index.verify(context.Background(), t5, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	index.expire(t6)

	for t := t5; t.Before(t6); t = t.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(context.Background(), t6)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t6)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 0 {
		t.Errorf("allIds = %v, want []", allIds)
	}

	hadIssue, err = index.verify(context.Background(), t6, buffer, false, false)
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}
}

// Test_expiration_offset checks that the expiration start offset is respected.
func Test_expiration_offset(t *testing.T) {
	defaultTTL := 365 * 24 * time.Hour
	shortTTL := 24 * time.Hour
	// current implementation only delete metrics expired the day before
	implementationDelay := 24 * time.Hour
	// updateDelay is the delay after which we are guaranteed to trigger an TTL update
	updateDelay := cassandraTTLUpdateDelay + cassandraTTLUpdateJitter + time.Second

	// At t0, we create 1 metric with shortTLL.
	t0 := time.Date(2022, 4, 29, 1, 42, 0, 0, time.UTC)
	// At t1, the metric shouldn't have expired because of the expiration offset.
	t1 := t0.Add(updateDelay).Add(shortTTL).Add(implementationDelay).
		Truncate(24 * time.Hour).Add(expirationStartOffset).Add(-time.Second)
	// At t2, the metric should be expired.
	t2 := t1.Add(time.Second)

	metric := map[string]string{
		"__name__":    "ttl",
		"unit":        "day",
		"value":       "2",
		"updated":     "yes",
		"description": "The metrics with TTL set to 2 months",
		"__ttl__":     strconv.FormatInt(int64(shortTTL.Seconds()), 10),
	}

	store := &mockStore{}

	index, err := initialize(
		context.Background(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       &mockLockFactory{},
			States:            &mockState{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Error(err)
	}

	// Check that the metric is in the index.
	labelsList := []labels.Labels{labelsMapToList(metric, false)}

	metricsID, ttls, err := index.lookupIDs(context.Background(), toLookupRequests(labelsList, t0), t0)
	if err != nil {
		t.Fatal(err)
	}

	if len(ttls) != 1 {
		t.Fatalf("got %d ttls, want 1", len(ttls))
	}

	if len(metricsID) != 1 {
		t.Fatalf("got %d metricIDs, want 1", len(ttls))
	}

	want := int64(shortTTL.Seconds())
	if ttls[0] != want {
		t.Errorf("got ttl = %d, want %d", ttls[0], want)
	}

	// t0
	index.expire(t0)
	_, _ = index.cassandraExpire(context.Background(), t0)

	allIds, err := index.AllIDs(context.Background(), t0, t0)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 1 {
		t.Errorf("len(allIds) = %d, want 1", len(allIds))
	}

	// t1
	index.expire(t1)
	// each call to cassandraExpire do one day, but calling multiple time
	// isn't an issue but it must be called at least once per day
	for t := t0; t.Before(t1); t = t.Add(expirationCheckInterval) {
		_, _ = index.cassandraExpire(context.Background(), t1)
	}

	allIds, err = index.AllIDs(context.Background(), t0, t1)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 1 {
		t.Errorf("len(allIds) = %d, want 1", len(allIds))
	}

	// t2
	index.expire(t2)
	_, _ = index.cassandraExpire(context.Background(), t2)

	allIds, err = index.AllIDs(context.Background(), t0, t1)
	if err != nil {
		t.Error(err)
	}

	if len(allIds) != 0 {
		t.Errorf("len(allIds) = %d, want 0", len(allIds))
	}
}

func Test_getTimeShards(t *testing.T) {
	type args struct {
		start time.Time
		end   time.Time
	}

	shardSize := int32(postingShardSize.Hours())

	if shardSize%2 != 0 {
		t.Errorf("shardSize is not an even number of hours. This is not supported")
	}

	now := time.Now()
	reference := time.Date(2020, 10, 15, 18, 40, 0, 0, time.UTC)
	baseTS := int32(reference.Unix()/3600) / shardSize * shardSize
	base := time.Unix(int64(baseTS)*3600, 0)

	index, err := initialize(
		context.Background(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		args args
		want []int32
	}{
		{
			name: "now",
			args: args{
				start: now,
				end:   now,
			},
			want: []int32{int32(now.Unix()/3600) / shardSize * shardSize},
		},
		{
			name: "base",
			args: args{
				start: base,
				end:   base,
			},
			want: []int32{baseTS},
		},
		{
			name: "reference",
			args: args{
				start: reference,
				end:   reference,
			},
			want: []int32{baseTS},
		},
		{
			// This test assume that reference+9h do NOT change its baseTime
			name: "reference+2h",
			args: args{
				start: reference,
				end:   reference.Add(2 * time.Hour),
			},
			want: []int32{baseTS},
		},
		{
			name: "reference+postingShardSize",
			args: args{
				start: reference,
				end:   reference.Add(postingShardSize),
			},
			want: []int32{baseTS, baseTS + shardSize},
		},
		{
			name: "base+postingShardSize",
			args: args{
				start: base,
				end:   base.Add(postingShardSize),
			},
			want: []int32{baseTS, baseTS + shardSize},
		},
		{
			name: "base+postingShardSize- 1seconds",
			args: args{
				start: base,
				end:   base.Add(postingShardSize).Add(-time.Second),
			},
			want: []int32{baseTS},
		},
		{
			// This test assume postingShardSize is at least > 2h
			name: "epoc",
			args: args{
				start: time.Date(1970, 1, 1, 0, 20, 0, 0, time.UTC),
				end:   time.Date(1970, 1, 1, 1, 59, 0, 0, time.UTC),
			},
			want: []int32{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := index.getTimeShards(context.Background(), tt.args.start, tt.args.end, true)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getTimeShards() = %v, want %v", got, tt.want)
			}
		})
	}

	got, err := index.getTimeShards(context.Background(), time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), time.Now(), true)
	if err != nil {
		t.Error(err)
	}

	for i, shard := range got {
		if shard == globalShardNumber {
			t.Errorf("getTimeShards()[%d] = %v, want != %v", i, shard, globalShardNumber)
		}
	}
}

func Test_FilteredLabelValues(t *testing.T) { //nolint:maintidx
	t0 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	t1 := t0.Add(postingShardSize)
	t2 := t1.Add(postingShardSize)
	t3 := t2.Add(postingShardSize * 2)
	now := t3.Add(postingShardSize * 2)

	index1, err := initialize(
		context.Background(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		log.With().Str("component", "index1").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = index1.lookupIDs(
		context.Background(),
		[]types.LookupRequest{
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":   "up",
					"account_id": "1",
					"job":        "prometheus",
					"instance":   "localhost:9090",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":   "up",
					"account_id": "2",
					"job":        "node_exporter",
					"instance":   "localhost:9100",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":   "up",
					"account_id": "2",
					"job":        "node_exporter",
					"instance":   "remotehost:9100",
				}),
				Start: t1,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":   "node_cpu_seconds_total",
					"account_id": "3",
					"job":        "node_exporter",
					"instance":   "remotehost:9100",
					"cpu":        "0",
					"mode":       "idle",
				}),
				Start: t0,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":   "node_cpu_seconds_total",
					"account_id": "3",
					"job":        "node_exporter",
					"instance":   "remotehost:9100",
					"mode":       "user",
				}),
				Start: t1,
				End:   t2,
			},
			{ // index = 5
				Labels: labels.FromMap(map[string]string{
					"__name__":   "node_cpu_seconds_total",
					"account_id": "3",
					"job":        "node_exporter",
					"instance":   "remotehost:9100",
					"cpu":        "1",
					"mode":       "user",
				}),
				Start: t0,
				End:   t0,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":   "node_filesystem_avail_bytes",
					"account_id": "3",
					"job":        "node_exporter",
					"instance":   "localhost:9100",
					"device":     "/dev/mapper/vg0-root",
					"fstype":     "ext4",
					"mountpoint": "/",
				}),
				Start: t1,
				End:   t2,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":    "node_filesystem_avail_bytes",
					"account_id":  "3",
					"job":         "node_exporter",
					"instance":    "localhost:9100",
					"device":      "/dev/mapper/vg0-data",
					"fstype":      "ext4",
					"mountpoint":  "/srv/data",
					"environment": "devel",
				}),
				Start: t0,
				End:   t1,
			},
			{
				Labels: labels.FromMap(map[string]string{
					"__name__":     "node_filesystem_avail_bytes",
					"account_id":   "4",
					"job":          "node_exporter",
					"instance":     "remote:9100",
					"device":       "/dev/mapper/vg0-data",
					"fstype":       "ext4",
					"mountpoint":   "/srv/data",
					"environment":  "production",
					"custom_label": "secret",
				}),
				Start: t0,
				End:   t2,
			},
			{ // index == 9
				Labels: labels.FromMap(map[string]string{
					"__name__":    "node_filesystem_avail_bytes",
					"job":         "node_exporter",
					"instance":    "remote:9100",
					"device":      "/dev/mapper/vg0-data",
					"fstype":      "ext4",
					"mountpoint":  "/srv/data",
					"environment": "production",
					"userID":      "42",
					"secret_name": "secret_value",
				}),
				Start: t2,
				End:   t2,
			},
		},
		now,
	)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		index     *CassandraIndex
		start     time.Time
		end       time.Time
		labelName string
		matchers  []*labels.Matcher
		want      []string
	}{
		{
			name:  "names-account1",
			index: index1,
			start: t0,
			end:   t2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"1",
				),
			},
			labelName: postinglabelName,
			want:      []string{"__name__", "account_id", "instance", "job"},
		},
		{
			name:  "names-account1-t1",
			index: index1,
			start: t1,
			end:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"1",
				),
			},
			labelName: postinglabelName,
			want:      nil,
		},
		{
			name:  "names-account2",
			index: index1,
			start: t0,
			end:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"2",
				),
			},
			labelName: postinglabelName,
			want:      []string{"__name__", "account_id", "instance", "job"},
		},
		{
			name:  "names-account2-t1",
			index: index1,
			start: t1,
			end:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"2",
				),
			},
			labelName: postinglabelName,
			want:      []string{"__name__", "account_id", "instance", "job"},
		},
		{
			name:  "names-account3",
			index: index1,
			start: t0,
			end:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"3",
				),
			},
			labelName: postinglabelName,
			want: []string{
				"__name__", "account_id", "cpu", "device", "environment", "fstype", "instance", "job", "mode", "mountpoint",
			},
		},
		{
			name:  "names-account3-t1",
			index: index1,
			start: t1,
			end:   t1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"3",
				),
			},
			labelName: postinglabelName,
			want: []string{
				"__name__", "account_id", "cpu", "device", "environment",
				"fstype", "instance", "job", "mode", "mountpoint",
			},
		},
		{
			name:  "names-account3-t2",
			index: index1,
			start: t2,
			end:   t2,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"3",
				),
			},
			labelName: postinglabelName,
			want:      []string{"__name__", "account_id", "device", "fstype", "instance", "job", "mode", "mountpoint"},
		},
		{
			name:  "names-account3-t3",
			index: index1,
			start: t3,
			end:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"3",
				),
			},
			labelName: postinglabelName,
			want:      nil,
		},
		{
			name:      "names-all",
			index:     index1,
			start:     t0,
			end:       t3,
			matchers:  nil,
			labelName: postinglabelName,
			want: []string{
				"__name__", "account_id", "cpu", "custom_label", "device", "environment", "fstype",
				"instance", "job", "mode", "mountpoint", "secret_name", "userID",
			},
		},
		{
			name:      "value-__name__-all",
			index:     index1,
			start:     t0,
			end:       t3,
			matchers:  nil,
			labelName: "__name__",
			want:      []string{"node_cpu_seconds_total", "node_filesystem_avail_bytes", "up"},
		},
		{
			name:  "value-__name__-account3",
			index: index1,
			start: t0,
			end:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"3",
				),
			},
			labelName: "__name__",
			want:      []string{"node_cpu_seconds_total", "node_filesystem_avail_bytes"},
		},
		{
			name:  "value-mountpoint-account2",
			index: index1,
			start: t0,
			end:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"2",
				),
			},
			labelName: "mountpoint",
			want:      nil,
		},
		{
			name:  "value-__name__-invalid-account",
			index: index1,
			start: t0,
			end:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"invalid",
				),
			},
			labelName: "__name__",
			want:      nil,
		},
		{
			name:  "value-wronglabel-account3",
			index: index1,
			start: t0,
			end:   t3,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"account_id",
					"3",
				),
			},
			labelName: "wronglabel",
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.index.labelValues(context.Background(), tt.start, tt.end, tt.labelName, tt.matchers)
			if err != nil {
				t.Errorf("labelValues() error = %v", err)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("labelValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mergeSorted(t *testing.T) {
	tests := []struct {
		name       string
		left       []string
		right      []string
		wantResult []string
	}{
		{
			name:       "disjoint",
			left:       []string{"a", "b", "c"},
			right:      []string{"d", "e", "f"},
			wantResult: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:       "left-empty",
			left:       []string{},
			right:      []string{"d", "e", "f"},
			wantResult: []string{"d", "e", "f"},
		},
		{
			name:       "right-empty",
			left:       []string{"d", "e", "f"},
			right:      []string{},
			wantResult: []string{"d", "e", "f"},
		},
		{
			name:       "interleave",
			left:       []string{"a", "c", "e"},
			right:      []string{"b", "d", "f"},
			wantResult: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:       "dup-end",
			left:       []string{"a", "b", "e", "f"},
			right:      []string{"c", "d", "e", "f"},
			wantResult: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name:       "dup-start",
			left:       []string{"a", "b", "c", "d"},
			right:      []string{"a", "b", "e", "f"},
			wantResult: []string{"a", "b", "c", "d", "e", "f"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotResult := mergeSorted(tt.left, tt.right); !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("mergeSorted() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func TestSimplifyRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		inputMatcher   *labels.Matcher
		wantedMatchers []*labels.Matcher
		wantErr        error
	}{
		{
			name: "2-values",
			inputMatcher: labels.MustNewMatcher(
				labels.MatchRegexp,
				"__name__",
				"cpu_used|mem_used_perc",
			),
			wantedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"cpu_used",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"mem_used_perc",
				),
			},
		},
		{
			name: "2-values-capture",
			inputMatcher: labels.MustNewMatcher(
				labels.MatchRegexp,
				"__name__",
				"(cpu_used|mem_used_perc)",
			),
			wantedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"cpu_used",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"mem_used_perc",
				),
			},
		},
		{
			name: "3-values-capture",
			inputMatcher: labels.MustNewMatcher(
				labels.MatchRegexp,
				"__name__",
				"(cpu_used|mem_used_perc|swap_used_perc)",
			),
			wantedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"cpu_used",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"mem_used_perc",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"swap_used_perc",
				),
			},
		},
		{
			name: "2-values-same-prefix",
			inputMatcher: labels.MustNewMatcher(
				labels.MatchRegexp,
				"__name__",
				"probe_ssl_last_chain_expiry_timestamp_seconds|probe_ssl_validation_success",
			),
			wantedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"probe_ssl_last_chain_expiry_timestamp_seconds",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"probe_ssl_validation_success",
				),
			},
		},
		{
			name: "2-values-capture-same-prefix",
			inputMatcher: labels.MustNewMatcher(
				labels.MatchRegexp,
				"__name__",
				"(probe_ssl_last_chain_expiry_timestamp_seconds|probe_ssl_validation_success)",
			),
			wantedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"probe_ssl_last_chain_expiry_timestamp_seconds",
				),
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"probe_ssl_validation_success",
				),
			},
		},
		{
			name: "cant-simplify",
			inputMatcher: labels.MustNewMatcher(
				labels.MatchRegexp,
				"__name__",
				"(cpu_used|i*)",
			),
			wantErr: errNotASimpleRegex,
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotMatchers, err := simplifyRegex(test.inputMatcher)
			if err != nil {
				if errors.Is(err, test.wantErr) {
					return
				}

				t.Fatalf("Failed to simplify regex: %s", err)
			}

			if diff := cmp.Diff(gotMatchers, test.wantedMatchers, cmpopts.IgnoreUnexported(labels.Matcher{})); diff != "" {
				t.Fatalf("Got wrong matchers\n%s", diff)
			}
		})
	}
}

func Test_timeForShard(t *testing.T) {
	tests := []struct {
		inputTime time.Time
		want      time.Time
	}{
		{
			inputTime: time.Date(2022, 10, 1, 0, 0, 0, 0, time.UTC),
			want:      time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			inputTime: time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
			want:      time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			inputTime: time.Date(2022, 9, 29, 1, 0, 0, 0, time.UTC),
			want:      time.Date(2022, 9, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			inputTime: time.Date(2022, 9, 22, 0, 0, 0, 0, time.UTC),
			want:      time.Date(2022, 9, 22, 0, 0, 0, 0, time.UTC),
		},
		{
			inputTime: time.Date(2022, 9, 15, 0, 0, 0, 0, time.UTC),
			want:      time.Date(2022, 9, 15, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		name := tt.inputTime.Format(time.RFC3339)
		t.Run(name, func(t *testing.T) {
			if got := timeForShard(ShardForTime(tt.inputTime.Unix())); !got.Equal(tt.want) {
				t.Errorf("timeForShard() = %s, want %s", got, tt.want)
			}
		})
	}
}
