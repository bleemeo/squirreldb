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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/dummy"
	"github.com/bleemeo/squirreldb/types"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const ttlLabel = "__test_ttl__"

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

var errNotImplemented = errors.New("not implemented")

type mockTime struct {
	l   sync.Mutex
	now time.Time
}

func newMockTime(value time.Time) *mockTime {
	return &mockTime{
		now: value,
	}
}

func (m *mockTime) Now() time.Time {
	m.l.Lock()
	defer m.l.Unlock()

	return m.now
}

func (m *mockTime) Set(value time.Time) {
	m.l.Lock()
	defer m.l.Unlock()

	m.now = value
}

func (m *mockTime) Add(delta time.Duration) {
	m.l.Lock()
	defer m.l.Unlock()

	m.now = m.now.Add(delta)
}

type mockState struct {
	l      sync.Mutex
	values map[string]string
}

func (m *mockState) Read(_ context.Context, name string, output any) (bool, error) {
	m.l.Lock()
	defer m.l.Unlock()

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

func (m *mockState) Write(_ context.Context, name string, value any) error {
	m.l.Lock()
	defer m.l.Unlock()

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

func (f *mockLockFactory) CreateLock(name string, _ time.Duration) types.TryLocker {
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

func (l *mockLock) BlockLock(_ context.Context, blockTTL time.Duration) error {
	_ = blockTTL

	return errNotImplemented
}

func (l *mockLock) UnblockLock(_ context.Context) error {
	return errNotImplemented
}

func (l *mockLock) BlockStatus(_ context.Context) (bool, time.Duration, error) {
	return false, 0, errNotImplemented
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
	expiration      map[int64][]byte
	labels2id       map[string]types.MetricID
	postings        map[int32]map[string]map[string][]byte
	id2labels       map[types.MetricID]labels.Labels
	id2expiration   map[types.MetricID]time.Time
	queryCount      int
	writeQueryCount int
	mutex           sync.Mutex

	HookSelectPostingByNameValueStart func(ctx context.Context, shard int32, name string, value string) context.Context
	HookSelectPostingByNameValueEnd   func(ctx context.Context, shard int32, name string, value string)
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
	s.expiration = make(map[int64][]byte)

	return ctx.Err()
}

// verifyStore check that metric exist in the store with expected metric expiration.
// len(metrics) must be == to len(expiration). If you don't want to check expiration of
// a metrics (but still check it's present), use zero value of time.Time for the expiration.
func (s *mockStore) verifyStore(
	t *testing.T,
	now time.Time,
	metrics []labels.Labels,
	expiration []time.Time,
	expiredMustBeDeleted bool,
) error {
	t.Helper()

	// updateDelay is the delay after which we are guaranteed to trigger an TTL update
	updateDelay := cassandraTTLUpdateDelay + cassandraTTLUpdateJitter + time.Second

	for i, metricLabels := range metrics {
		key := sortLabels(metricLabels).String()
		id, ok := s.labels2id[key]

		wantMinExpire := expiration[i]
		wantMaxExpire := expiration[i].Add(updateDelay)

		if !ok {
			if !expiration[i].IsZero() && now.After(expiration[i]) {
				continue
			}

			return fmt.Errorf("metric %s isn't in labels2id", key)
		}

		labelsInStore := s.id2labels[id]

		if diff := cmp.Diff(metricLabels, labelsInStore); diff != "" {
			return fmt.Errorf("id2labels mismatch: (-want +got): %s", diff)
		}

		if expiration[i].IsZero() {
			continue
		}

		if s.id2expiration[id].After(wantMaxExpire) || s.id2expiration[id].Before(wantMinExpire) {
			return fmt.Errorf(
				"id2expiration[%d (%s)] = %v, want between %v and %v",
				id,
				metricLabels.String(),
				s.id2expiration[id],
				wantMinExpire,
				wantMaxExpire,
			)
		}

		if now.After(wantMaxExpire) && expiredMustBeDeleted {
			return fmt.Errorf("metric %s is not deleted but expired", key)
		}
	}

	return nil
}

// getPresencePostings return the maybePresent & allPosting.
func (s *mockStore) getPresencePostings(shard int32) (*roaring.Bitmap, *roaring.Bitmap, error) {
	maybePresent := roaring.NewBTreeBitmap()
	allPosting := roaring.NewBTreeBitmap()

	// The only error possible for SelectPostingByNameValue is not found. Ignore such error
	bitset, err := s.SelectPostingByNameValue(context.Background(), shard, maybePostingLabel, maybePostingLabel)
	if err == nil {
		err = maybePresent.UnmarshalBinary(bitset)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"UnmarshalBinary maybePosting for shard %s (%d): %w",
				timeForShard(shard).Format(shardDateFormat),
				shard,
				err,
			)
		}
	}

	bitset, err = s.SelectPostingByNameValue(context.Background(), shard, allPostingLabel, allPostingLabel)
	if err == nil {
		err = allPosting.UnmarshalBinary(bitset)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"UnmarshalBinary allPostingLabel for shard %s (%d): %w",
				timeForShard(shard).Format(shardDateFormat),
				shard,
				err,
			)
		}
	}

	return maybePresent, allPosting, nil
}

// verifyShard ensure that given metrics exists in a specific shard. Only presentPosting & maybe present is checked.
func (s *mockStore) verifyShard(
	t *testing.T,
	shard int32,
	metricsPresent []labels.Labels,
	metricsAbsent []labels.Labels,
) error {
	t.Helper()

	var errs prometheus.MultiError

	maybePresent, allPosting, err := s.getPresencePostings(shard)
	if err != nil {
		return err
	}

	for _, metricLabels := range metricsPresent {
		key := sortLabels(metricLabels).String()
		id, ok := s.labels2id[key]

		if !ok {
			errs = append(errs, fmt.Errorf("metric %s isn't in labels2id", key))

			continue
		}

		if !maybePresent.Contains(uint64(id)) {
			errs = append(errs, fmt.Errorf(
				"metric %s is absent from maybePosting of shard %s (%d)",
				metricLabels.String(),
				timeForShard(shard).Format(shardDateFormat),
				shard,
			))
		}

		if !allPosting.Contains(uint64(id)) {
			errs = append(errs, fmt.Errorf(
				"metric %s is absent from allPostingLabel of shard %s (%d)",
				metricLabels.String(),
				timeForShard(shard).Format(shardDateFormat),
				shard,
			))
		}
	}

	for _, metricLabels := range metricsAbsent {
		key := sortLabels(metricLabels).String()
		id, ok := s.labels2id[key]

		if !ok {
			continue
		}

		if maybePresent.Contains(uint64(id)) {
			errs = append(errs, fmt.Errorf(
				"metric %s is present in maybePosting of shard %s (%d)",
				metricLabels.String(),
				timeForShard(shard).Format(shardDateFormat),
				shard,
			))
		}

		if allPosting.Contains(uint64(id)) {
			errs = append(errs, fmt.Errorf(
				"metric %s is present in allPostingLabel of shard %s (%d)",
				metricLabels.String(),
				timeForShard(shard).Format(shardDateFormat),
				shard,
			))
		}
	}

	if errs != nil {
		return errs
	}

	return nil
}

// verifyStoreEmpty ensure that the store is completely empty.
func (s *mockStore) verifyStoreEmpty(t *testing.T) error {
	t.Helper()

	if got := len(s.postings); got != 0 {
		return fmt.Errorf("len(postings) = %d, want 0", got)
	}

	if got := len(s.expiration); got != 0 {
		return fmt.Errorf("len(expiration) = %d, want 0", got)
	}

	if got := len(s.id2expiration); got != 0 {
		return fmt.Errorf("len(id2expiration) = %d, want 0", got)
	}

	if got := len(s.id2labels); got != 0 {
		return fmt.Errorf("len(id2labels) = %d, want 0", got)
	}

	if got := len(s.labels2id); got != 0 {
		return fmt.Errorf("len(labels2id) = %d, want 0", got)
	}

	return nil
}

type blockContextKey struct {
	Name string
}

func getBlockContext(ctx context.Context, name string) *blockContext {
	blockList, ok := ctx.Value(blockContextKey{Name: name}).(*blockContext)
	if !ok {
		return nil
	}

	return blockList
}

// withBlockContext return a context and blockContext that could be used to control execution timing some method.
// The context should be used in call to method you want to block. The method should call WaitStart,
// NotifyEndReached and WaitEnd.
//
// The method will block at most maxWait when it call WaitStart or WaitEnd.
//
// The storeBlock contains:
//   - a function unblockStartAndWait: which allow the method to start and wait until it block at end of invocation.
//     unblockStart only return once method is blocked at the end
//   - a function unblockEnd: which allow the method to return. It also call unblockStart() which
//     is a no-op if it was already called.
//
// This is likely useful with store hooks, like HookSelectPostingByNameValueStart and HookSelectPostingByNameValueEnd.
func withBlockContext(ctx context.Context, name string, maxWait time.Duration) (context.Context, *blockContext) {
	result := &blockContext{maxWait: maxWait}
	result.cond = sync.NewCond(&result.l)

	return context.WithValue(ctx, blockContextKey{Name: name}, result), result
}

type blockContext struct {
	l             sync.Mutex
	cond          *sync.Cond
	maxWait       time.Duration
	startUnlocked bool
	endReached    bool
	endUnlocked   bool
}

func (b *blockContext) unblockStartAndWait() {
	b.l.Lock()
	defer b.l.Unlock()

	b.startUnlocked = true
	b.cond.Broadcast()

	for !b.endReached {
		b.cond.Wait()
	}
}

func (b *blockContext) unblockEnd() {
	b.unblockStartAndWait()

	b.l.Lock()
	defer b.l.Unlock()

	b.endUnlocked = true
	b.cond.Broadcast()
}

func (b *blockContext) WaitStart() {
	b.l.Lock()
	defer b.l.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), b.maxWait)
	defer cancel()

	go func() {
		<-ctx.Done()
		b.cond.Broadcast()
	}()

	for !b.startUnlocked && ctx.Err() == nil {
		b.cond.Wait()
	}
}

func (b *blockContext) NotifyEndReached() {
	b.l.Lock()
	defer b.l.Unlock()

	b.endReached = true
	b.cond.Broadcast()
}

func (b *blockContext) WaitEnd() {
	b.l.Lock()
	defer b.l.Unlock()

	b.endReached = true
	b.cond.Broadcast()

	ctx, cancel := context.WithTimeout(context.Background(), b.maxWait)
	defer cancel()

	go func() {
		<-ctx.Done()
		b.cond.Broadcast()
	}()

	for !b.endUnlocked && ctx.Err() == nil {
		b.cond.Wait()
	}
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

	result, ok := s.expiration[day.Unix()]
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

func (i *mockPostingIter) Err() error {
	return i.err
}

func (i *mockPostingIter) Close() {
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
	if s.HookSelectPostingByNameValueStart != nil {
		ctx = s.HookSelectPostingByNameValueStart(ctx, shard, name, value)
	}

	if s.HookSelectPostingByNameValueEnd != nil {
		defer s.HookSelectPostingByNameValueEnd(ctx, shard, name, value)
	}

	r, e := s.selectPostingByNameValue(ctx, shard, name, value)

	return r, e
}

func (s *mockStore) selectPostingByNameValue(
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
	s.writeQueryCount++

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
	s.writeQueryCount++

	s.id2labels[id] = sortedLabels
	s.id2expiration[id] = expiration

	return ctx.Err()
}

func (s *mockStore) InsertLabels2ID(ctx context.Context, sortedLabelsString string, id types.MetricID) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++
	s.writeQueryCount++

	s.labels2id[sortedLabelsString] = id

	return ctx.Err()
}

func (s *mockStore) InsertExpiration(ctx context.Context, day time.Time, bitset []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++
	s.writeQueryCount++

	s.expiration[day.Unix()] = bitset

	return ctx.Err()
}

func (s *mockStore) UpdateID2LabelsExpiration(ctx context.Context, id types.MetricID, expiration time.Time) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++
	s.writeQueryCount++

	s.id2expiration[id] = expiration

	return ctx.Err()
}

func (s *mockStore) DeleteLabels2ID(ctx context.Context, sortedLabelsString string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++
	s.writeQueryCount++

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
	s.writeQueryCount++

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
	s.writeQueryCount++

	if _, ok := s.expiration[day.Unix()]; !ok {
		return gocql.ErrNotFound
	}

	delete(s.expiration, day.Unix())

	return ctx.Err()
}

func (s *mockStore) DeletePostings(ctx context.Context, shard int32, name string, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++
	s.writeQueryCount++

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

func (s *mockStore) DeletePostingsByNames(ctx context.Context, shard int32, names []string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.queryCount++
	s.writeQueryCount++

	if shard == 0 {
		panic("uninitialized shard value")
	}

	postings, ok := s.postings[shard]
	if !ok {
		return gocql.ErrNotFound
	}

	for _, name := range names {
		_, ok := postings[name]
		if !ok {
			return gocql.ErrNotFound
		}

		delete(postings, name)

		if len(postings) == 0 {
			delete(s.postings, shard)
		}
	}

	return ctx.Err()
}

type mockCluster struct {
	*dummy.LocalCluster

	l            sync.Mutex
	delay        time.Duration
	callbackChan chan any
	wg           *sync.WaitGroup
}

func newMockCluster(cluster *dummy.LocalCluster) *mockCluster {
	return &mockCluster{
		LocalCluster: cluster,
		callbackChan: make(chan any),
		wg:           &sync.WaitGroup{},
	}
}

func (c *mockCluster) Subscribe(topic string, callback func([]byte)) {
	wrappedCallback := func(in []byte) {
		c.l.Lock()
		wg := c.wg
		callbackChan := c.callbackChan
		delay := c.delay
		c.l.Unlock()

		wg.Add(1)

		go func() {
			defer wg.Done()

			// wait for callbackGate to be closed, signifing message should be processed
			<-callbackChan

			if delay > 0 {
				time.Sleep(delay)
			}

			callback(in)
		}()
	}

	c.LocalCluster.Subscribe(topic, wrappedCallback)
}

// AutoProcessWithDelay allow cluster message to be processed without need to call ProcessMessage()
// but after a delay which must be > 0.
// You can still use ProcessMessage() to wait for currently pending messages.
func (c *mockCluster) AutoProcessWithDelay(delay time.Duration) {
	if delay == 0 {
		panic("delay could not be 0")
	}

	c.l.Lock()
	defer c.l.Unlock()

	// When delay != 0, c.callbackChan is already closed.
	if c.delay == 0 {
		close(c.callbackChan)
	}

	c.delay = delay
}

// ProcessMessage will allow all currently pending message to process and wait for their completing.
// This only apply to message currently pending, new message (and possibly one that arrive during
// ProcessMessage() won't be processed and won't be waited).
func (c *mockCluster) ProcessMessage() {
	c.l.Lock()
	// We take copy of the waitgroup & chanel before creating a new one.
	// This ensure we unblock & wait only currently pending messages.
	wg := c.wg
	callbackChan := c.callbackChan
	delay := c.delay

	c.wg = &sync.WaitGroup{}
	c.callbackChan = make(chan any)

	if c.delay != 0 {
		close(c.callbackChan)
	}

	c.l.Unlock()

	// When delay != 0, c.callbackChan is already closed.
	if delay == 0 {
		close(callbackChan)
	}

	wg.Wait()
}

func toLookupRequests(list []labels.Labels, now time.Time) []types.LookupRequest {
	results := make([]types.LookupRequest, len(list))

	for i, l := range list {
		// Set the request TTL from the labels.
		timeToLive := int64(0)

		if l.Has(ttlLabel) {
			var err error

			ttlRaw := l.Get(ttlLabel)

			timeToLive, err = strconv.ParseInt(ttlRaw, 10, 64)
			if err != nil {
				panic(fmt.Errorf("can't parse time to live '%s', using default: %w", ttlRaw, err))
			}

			builder := labels.NewBuilder(l)
			builder.Del(ttlLabel)
			l = builder.Labels()
		}

		results[i] = types.LookupRequest{
			Labels:     l.Copy(),
			TTLSeconds: timeToLive,
			Start:      now,
			End:        now,
		}
	}

	return results
}

func dropTTLFromMetricList(input []labels.Labels) []labels.Labels {
	result := make([]labels.Labels, 0, len(input))

	for _, metric := range input {
		builder := labels.NewBuilder(metric)
		builder.Del(ttlLabel)

		result = append(result, builder.Labels())
	}

	return result
}

func labelsMapToList(m map[string]string, dropSpecialLabel bool) labels.Labels {
	results := make(labels.Labels, 0, len(m))

	for k, v := range m {
		if dropSpecialLabel && (k == ttlLabel) {
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

// getTestLogger return a logger suitable for test.
// It's default level is error because without that test output will be way too large.
// To have details log, use SQUIRRELDB_LOG_LEVEL environment variable, likely with running
// specific test. E.g.:
//
//	SQUIRRELDB_LOG_LEVEL=0 go test ./cassandra/index -run Test_cluster_expiration_and_error
func getTestLogger() zerolog.Logger {
	levelStr := os.Getenv("SQUIRRELDB_LOG_LEVEL")
	if levelStr == "" {
		levelStr = strconv.FormatInt(int64(zerolog.ErrorLevel), 10)
	}

	level, err := strconv.ParseInt(levelStr, 10, 0)
	if err != nil {
		level = int64(zerolog.ErrorLevel)
	}

	return log.Logger.Level(zerolog.Level(level))
}

func mockIndexFromMetrics(
	t *testing.T,
	start, end time.Time,
	metrics map[types.MetricID]map[string]string,
) *CassandraIndex {
	t.Helper()

	index, err := initialize(
		t.Context(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 1 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index").Logger(),
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

	_, err = index.InternalCreateMetric(t.Context(), start, end, metricsList, ids, expirations, false)
	if err != nil {
		panic(err)
	}

	return index
}

func mapsToLabelsList(input []map[string]string) []labels.Labels {
	result := make([]labels.Labels, 0, len(input))

	for _, v := range input {
		result = append(result, labels.FromMap(v))
	}

	return result
}

func sameExpirations(count int, expiration time.Time) []time.Time {
	result := make([]time.Time, count)

	for i := range result {
		result[i] = expiration
	}

	return result
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
			for range b.N {
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
		c, err := initialize(
			b.Context(),
			&mockStore{},
			Options{
				DefaultTimeToLive: 365 * 24 * time.Hour,
				LockFactory:       &mockLockFactory{},
				Cluster:           &dummy.LocalCluster{},
			},
			newMetrics(prometheus.NewRegistry()),
			getTestLogger().With().Str("component", "index1").Logger(),
		)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(tt.name, func(b *testing.B) {
			for range b.N {
				key := tt.labels.Hash()
				idData, _ := c.getIDData(key, tt.labels)
				c.setIDData(key, idData)
			}
		})
	}
}

// Test_interfaces make sure the CassandraIndex implement some interfaces.
func Test_interfaces(t *testing.T) {
	var iface any

	index := &CassandraIndex{}
	iface = index

	_, ok := iface.(types.VerifiableIndex)
	if !ok {
		t.Error("index isn't a VerifiableIndex")
	}

	_, ok = iface.(types.IndexDumper)
	if !ok {
		t.Error("index isn't a IndexDumper")
	}

	_, ok = iface.(types.Index)
	if !ok {
		t.Error("index isn't a Index")
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
			for range b.N {
				_ = tt.labels.String()
			}
		})
	}
}

func Test_postingsForMatchers(t *testing.T) { //nolint:maintidx
	now := time.Now()
	shards := []int32{shardForTime(now.Unix())}
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
	index1 := mockIndexFromMetrics(t, now, now, metrics1)

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
	index3 := mockIndexFromMetrics(t, now, now, metrics3)

	for x := 1; x < 101; x++ {
		for y := range 100 {
			id := types.MetricID(x*100 + y)
			metrics2[id] = map[string]string{
				"__name__":   fmt.Sprintf("generated_%03d", x),
				"label_x":    fmt.Sprintf("%03d", x),
				"label_y":    fmt.Sprintf("%03d", y),
				"multiple_2": strconv.FormatBool(y%2 == 0),
				"multiple_3": strconv.FormatBool(y%3 == 0),
				"multiple_5": strconv.FormatBool(y%5 == 0),
			}
		}
	}

	index2 := mockIndexFromMetrics(t, now, now, metrics2)

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
			name:     "no-matcher",
			index:    index1,
			matchers: []*labels.Matcher{},
			want:     []types.MetricID{},
		},
		{
			name:  "empty-result-simple",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchEqual,
					"__name__",
					"this_name_does_not_exists",
				),
			},
			want: []types.MetricID{},
		},
		{
			name:  "empty-result-complex1",
			index: index1,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(
					labels.MatchNotEqual,
					"this_label_is_always_empty_so_nothing_will_match",
					"",
				),
			},
			want: []types.MetricID{},
		},
		{
			name:  "empty-result-complex2",
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
				labels.MustNewMatcher(
					labels.MatchRegexp,
					"__name__",
					"node_.*total$",
				),
			},
			want: []types.MetricID{},
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
			got, err := tt.index.idsForMatchers(t.Context(), shards, tt.matchers, 0)
			if err != nil {
				t.Errorf("idsForMatchers() error = %v", err)

				return
			}

			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}

			if got.Count() != tt.wantLen {
				t.Errorf("idsForMatchers().Count()=%v, want %v", got.Count(), tt.wantLen)
			}

			gotIDs := make([]types.MetricID, 0, len(tt.want))

			for got.Next() {
				gotIDs = append(gotIDs, got.At().ID)
			}

			if diff := cmp.Diff(tt.want, gotIDs[:len(tt.want)]); diff != "" {
				t.Errorf("idsForMatchers mismatch: (-want +got): %s", diff)
			}
		})

		t.Run(tt.name+" direct", func(t *testing.T) {
			got, err := tt.index.idsForMatchers(t.Context(), shards, tt.matchers, 1000)
			if err != nil {
				t.Errorf("idsForMatchers() error = %v", err)

				return
			}

			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}

			if got.Count() != tt.wantLen {
				t.Errorf("idsForMatchers().Count()=%v, want %v", got.Count(), tt.wantLen)
			}

			gotIDs := make([]types.MetricID, 0, len(tt.want))

			for got.Next() {
				gotIDs = append(gotIDs, got.At().ID)
			}

			if diff := cmp.Diff(tt.want, gotIDs[:len(tt.want)]); diff != "" {
				t.Errorf("idsForMatchers mismatch: (-want +got): %s", diff)
			}
		})

		t.Run(tt.name+" reverse", func(t *testing.T) {
			matchersReverse := make([]*labels.Matcher, len(tt.matchers))
			for i := range matchersReverse {
				matchersReverse[i] = tt.matchers[len(tt.matchers)-i-1]
			}

			got, err := tt.index.idsForMatchers(t.Context(), shards, matchersReverse, 0)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)

				return
			}

			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}

			if got.Count() != tt.wantLen {
				t.Errorf("idsForMatchers().Count()=%v, want %v", got.Count(), tt.wantLen)
			}

			gotIDs := make([]types.MetricID, 0, len(tt.want))

			for got.Next() {
				gotIDs = append(gotIDs, got.At().ID)
			}

			if diff := cmp.Diff(tt.want, gotIDs[:len(tt.want)]); diff != "" {
				t.Errorf("idsForMatchers mismatch: (-want +got): %s", diff)
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
		t.Context(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
			States:            &mockState{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index1").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics1IDs, _, err := index1.lookupIDs(
		t.Context(),
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

	for x := range 100 {
		var start, end time.Time

		for y := range 100 {
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
					"multiple_2": strconv.FormatBool(y%2 == 0),
					"multiple_3": strconv.FormatBool(y%3 == 0),
					"multiple_5": strconv.FormatBool(y%5 == 0),
				}),
				Start: start,
				End:   end,
			})
		}
	}

	index2, err := initialize(
		t.Context(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
			States:            &dummy.States{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index2").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics2IDs, _, err := index2.lookupIDs(t.Context(), requests, now)
	if err != nil {
		t.Fatal(err)
	}

	index3, err := initialize(
		t.Context(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
			States:            &dummy.States{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index3").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics3IDs1, _, err := index3.lookupIDs(
		t.Context(),
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
		t.Context(),
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
		t.Context(),
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
		shards, err := tt.index.getTimeShards(t.Context(), tt.queryStart, tt.queryEnd, false)
		if err != nil {
			t.Errorf("getTimeShards() error = %v", err)
		}

		shardsAll, err := tt.index.getTimeShards(t.Context(), tt.queryStart, tt.queryEnd, true)
		if err != nil {
			t.Errorf("getTimeShards(returnAll=true) error = %v", err)
		}

		t.Run(tt.name+" shardsAll", func(t *testing.T) {
			got, err := tt.index.idsForMatchers(t.Context(), shardsAll, tt.matchers, 0)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)

				return
			}

			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}

			if got.Count() != tt.wantLen {
				t.Errorf("idsForMatchers().Count()=%v, want %v", got.Count(), tt.wantLen)
			}

			gotIDs := make([]types.MetricID, 0, len(tt.want))

			for got.Next() {
				gotIDs = append(gotIDs, got.At().ID)
			}

			if diff := cmp.Diff(tt.want, gotIDs[:len(tt.want)]); diff != "" {
				t.Errorf("idsForMatchers mismatch: (-want +got): %s", diff)
			}
		})

		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.index.idsForMatchers(t.Context(), shards, tt.matchers, 0)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)

				return
			}
			if tt.wantLen == 0 { //nolint: wsl
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}

			if got.Count() != tt.wantLen {
				t.Errorf("idsForMatchers().Count()=%v, want %v", got.Count(), tt.wantLen)
			}

			gotIDs := make([]types.MetricID, 0, len(tt.want))

			for got.Next() {
				gotIDs = append(gotIDs, got.At().ID)
			}

			if diff := cmp.Diff(tt.want, gotIDs[:len(tt.want)]); diff != "" {
				t.Errorf("idsForMatchers mismatch: (-want +got): %s", diff)
			}
		})

		t.Run(tt.name+" direct", func(t *testing.T) {
			got, err := tt.index.idsForMatchers(t.Context(), shards, tt.matchers, 1000)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)

				return
			}

			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}

			if got.Count() != tt.wantLen {
				t.Errorf("idsForMatchers().Count()=%v, want %v", got.Count(), tt.wantLen)
			}

			gotIDs := make([]types.MetricID, 0, len(tt.want))

			for got.Next() {
				gotIDs = append(gotIDs, got.At().ID)
			}

			if diff := cmp.Diff(tt.want, gotIDs[:len(tt.want)]); diff != "" {
				t.Errorf("idsForMatchers mismatch: (-want +got): %s", diff)
			}
		})

		t.Run(tt.name+" reverse", func(t *testing.T) {
			matchersReverse := make([]*labels.Matcher, len(tt.matchers))

			for i := range matchersReverse {
				matchersReverse[i] = tt.matchers[len(tt.matchers)-i-1]
			}

			got, err := tt.index.idsForMatchers(t.Context(), shards, matchersReverse, 0)
			if err != nil {
				t.Errorf("postingsForMatchers() error = %v", err)

				return
			}

			if tt.wantLen == 0 {
				// Avoid requirement to set tt.wantLen on simple test
				tt.wantLen = len(tt.want)
			}

			if got.Count() != tt.wantLen {
				t.Errorf("idsForMatchers().Count()=%v, want %v", got.Count(), tt.wantLen)
			}

			gotIDs := make([]types.MetricID, 0, len(tt.want))

			for got.Next() {
				gotIDs = append(gotIDs, got.At().ID)
			}

			if diff := cmp.Diff(tt.want, gotIDs[:len(tt.want)]); diff != "" {
				t.Errorf("idsForMatchers mismatch: (-want +got): %s", diff)
			}
		})
	}

	buffer := bytes.NewBuffer(nil)

	hadIssue, err := index1.Verifier(buffer).
		WithNow(now).
		WithStrictMetricCreation(true).
		Verify(t.Context())
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	hadIssue, err = index2.Verifier(buffer).
		WithNow(now).
		WithStrictMetricCreation(true).
		Verify(t.Context())
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	hadIssue, err = index3.Verifier(buffer).
		WithNow(now).
		WithStrictMetricCreation(true).
		Verify(t.Context())
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

	for n := range 12 {
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

				maxPosting := allPosting.Max()
				count := allPosting.Count()

				if maxPosting-count != uint64(tt.numberHole) && tt.numberHole != -1 {
					t.Errorf("Had %d hole, want %d", maxPosting-count, tt.numberHole)
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

				for n := range iterCount {
					newID := findFreeID(allPosting)

					if newID == 0 {
						t.Errorf("unable to find newID after %d loop", n)
					}

					if len(tt.wants) > n && newID != tt.wants[n] {
						t.Errorf("loop %d: newID = %d, want %d", n, newID, tt.wants[n])
					}

					if n < tt.numberHole && newID == maxPosting+1 {
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
			for range b.N {
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
		t.Context(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       lock,
			States:            states,
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index1").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	index2, err := initialize(
		t.Context(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       lock,
			States:            states,
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index2").Logger(),
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
			lbls[ttlLabel] = "3600"
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

				_, _, err = idx.lookupIDs(t.Context(), toLookupRequests(tt.labelsList, t0), t0)
				if err != nil {
					t.Fatal(err)
				}

				delta := store.queryCount - countBefore
				if delta <= 0 {
					t.Errorf("First lookup on indexes[%d] caused %d query to store, want > 0", i, delta)
				}

				countBefore = store.queryCount

				_, _, err = idx.lookupIDs(t.Context(), toLookupRequests(tt.labelsList, t0), t0)
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

				_, _, err = idx.lookupIDs(t.Context(), toLookupRequests(tt.labelsList, t0), t0)
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

// Test_cache_bug_posting_invalidation will run a scenario on index cache to show invalidation issue.
// It happened that a 2-node SquirrelDB cluster returned inconsitent result between SquirrelDB A and SquirrelDB B
// on a query that used newly-created metrics. This issue resolved by itself after 15 minutes. For that reason
// I suspect the posting cache to not being invalidated correctly on SquirrelDB B after metric creation on
// SquirrelDB A.
// After looking at code, I even think it could be possible to have issue on a single node. Step to reproduce (what
// this test does):
// - Run a query on SquirrelDB B.
//   - The result will not be in cache and Cassandra will be queried
//   - To simulate concurrency issue, once Cassandra get the data, sleep the goroutine
//     (e.g. delay return from SelectPostingByNameValue)
//
// - While query on SquirrelDB B is waiting, create a metric on SquirrelDB A
// - Once the creation is done SquirrelDB B cache is invalidated (strictly speaking a message to invalidate it is sent)
// - Resume query on SquirrelDB B. The bug was that Cassandra already read old value and we write result in cache.
// - Since cache was already invalidated, it would be persistent with old/wrong value.
func Test_cache_bug_posting_invalidation(t *testing.T) { //nolint:maintidx
	defaultTTL := 365 * 24 * time.Hour
	t0 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	t1 := t0.Add(5 * time.Hour)

	type clusterMessageBehavior int

	const (
		clusterSynchronious clusterMessageBehavior = 1
		clusterFast         clusterMessageBehavior = 2
		clusterDelay        clusterMessageBehavior = 3
	)

	// The test will actually write 2 time:
	// * write write1Metrics with t0 and ensure cluster message are processed. This simulate an old writes.
	// * start the query with t1
	// * write write2Metrics with t1
	// * if clusterMessageBeforeUnblock process cluster message
	// * end the query
	// * if not clusterMessageBeforeUnblock, process cluster message
	// * re-run the query without any blocking. Check that result match wanted value
	tests := []struct {
		name                   string
		useTwoSquirrelDB       bool
		clusterMessageBehavior clusterMessageBehavior
		write1Metrics          []labels.Labels
		write2Metrics          []labels.Labels
		query                  []*labels.Matcher
		want                   []labels.Labels
	}{
		{
			name:                   "Two SquirrelDB",
			useTwoSquirrelDB:       true,
			clusterMessageBehavior: clusterSynchronious,
			write1Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
			},
			write2Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
			query: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "counter_total"),
			},
			want: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
		},
		{
			name:                   "One SquirrelDB",
			useTwoSquirrelDB:       false,
			clusterMessageBehavior: clusterSynchronious,
			write1Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
			},
			write2Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
			query: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "counter_total"),
			},
			want: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
		},
		{
			name:                   "Two SquirrelDB, async cluster",
			useTwoSquirrelDB:       true,
			clusterMessageBehavior: clusterFast,
			write1Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
			},
			write2Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
			query: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "counter_total"),
			},
			want: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
		},
		{
			name:                   "Two SquirrelDB, slow cluster",
			useTwoSquirrelDB:       true,
			clusterMessageBehavior: clusterDelay,
			write1Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
			},
			write2Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
			query: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "counter_total"),
			},
			want: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
		},
		{
			name:                   "One SquirrelDB, slow cluster",
			useTwoSquirrelDB:       false,
			clusterMessageBehavior: clusterDelay,
			write1Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
			},
			write2Metrics: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
			query: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "counter_total"),
			},
			want: []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item1",
				}),
				labels.FromMap(map[string]string{
					"__name__": "counter_total",
					"item":     "item2",
				}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := &dummy.LocalCluster{}
			storeSubCtx, unblock := withBlockContext(t.Context(), "blockQuery", time.Second)
			wrappedCluster := newMockCluster(cluster)

			var usedCluster types.Cluster

			if tt.clusterMessageBehavior == clusterSynchronious {
				usedCluster = cluster
			} else {
				usedCluster = wrappedCluster
			}

			store := &mockStore{
				HookSelectPostingByNameValueStart: func(ctx context.Context, shard int32, _, _ string) context.Context {
					if shard != -1 {
						block := getBlockContext(ctx, "blockQuery")
						if block != nil {
							block.WaitStart()
						}
					}

					return ctx
				},
				HookSelectPostingByNameValueEnd: func(ctx context.Context, shard int32, _, _ string) {
					if shard != -1 {
						block := getBlockContext(ctx, "blockQuery")
						if block != nil {
							block.WaitEnd() //nolint:contextcheck
						}
					}
				},
			}
			lock := &mockLockFactory{}
			states := &mockState{}

			index1, err := initialize(
				t.Context(),
				store,
				Options{
					DefaultTimeToLive: defaultTTL,
					LockFactory:       lock,
					States:            states,
					Cluster:           usedCluster,
				},
				newMetrics(prometheus.NewRegistry()),
				getTestLogger().With().Str("component", "index1").Logger(),
			)
			if err != nil {
				t.Fatal(err)
			}

			index2, err := initialize(
				t.Context(),
				store,
				Options{
					DefaultTimeToLive: defaultTTL,
					LockFactory:       lock,
					States:            states,
					Cluster:           usedCluster,
				},
				newMetrics(prometheus.NewRegistry()),
				getTestLogger().With().Str("component", "index2").Logger(),
			)
			if err != nil {
				t.Fatal(err)
			}

			writeIndex := index1
			readIndex := index1

			if tt.useTwoSquirrelDB {
				readIndex = index2
			}

			_, _, err = writeIndex.lookupIDs(t.Context(), toLookupRequests(tt.write1Metrics, t0), t0)
			if err != nil {
				t.Fatal(err)
			}

			wrappedCluster.ProcessMessage()

			group, _ := errgroup.WithContext(t.Context())

			group.Go(func() error {
				_, err := readIndex.Search(storeSubCtx, t1, t1, tt.query)

				return err
			})

			unblock.unblockStartAndWait()

			_, _, err = writeIndex.lookupIDs(t.Context(), toLookupRequests(tt.write2Metrics, t1), t1)
			if err != nil {
				t.Fatal(err)
			}

			if tt.clusterMessageBehavior == clusterFast {
				wrappedCluster.ProcessMessage()
			}

			unblock.unblockEnd()

			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}

			if tt.clusterMessageBehavior == clusterDelay {
				wrappedCluster.ProcessMessage()
			}

			ids, err := readIndex.Search(t.Context(), t1, t1, tt.query)
			if err != nil {
				t.Fatal(err)
			}

			_ = ids

			if diff, err := cmpMetricsSetByLabels(ids, tt.want); err != nil {
				t.Error(err)
			} else if diff != "" {
				t.Errorf("Search mismatch: (-got +want)\n%s", diff)
			}

			// re-do the request, but this time ensure it has cache
			previousQueryCount := store.queryCount
			// 1 query will be done, for fetch labels value for IDs. There is currently no
			// cache for labels values.
			expectedQueryCount := previousQueryCount + 1

			ids, err = readIndex.Search(t.Context(), t1, t1, tt.query)
			if err != nil {
				t.Fatal(err)
			}

			if diff, err := cmpMetricsSetByLabels(ids, tt.want); err != nil {
				t.Error(err)
			} else if diff != "" {
				t.Errorf("Search mismatch: (-got +want)\n%s", diff)
			}

			if store.queryCount != expectedQueryCount {
				t.Errorf("Store query count = %d, want %d", store.queryCount, expectedQueryCount)
			}
		})
	}
}

func cmpMetricsSetByLabels(ids types.MetricsSet, want []labels.Labels) (string, error) {
	got := make([]labels.Labels, 0, len(want))

	for ids.Next() {
		m := ids.At()
		got = append(got, m.Labels)
	}

	if err := ids.Err(); err != nil {
		return "", err
	}

	sorter := cmpopts.SortSlices(func(a, b labels.Labels) bool {
		return labels.Compare(a, b) < 0
	})

	return cmp.Diff(got, want, sorter), nil
}

// Test_cluster will run a small scenario on the index to check cluster SquirrelDB.
func Test_cluster(t *testing.T) { //nolint:maintidx
	defaultTTL := 365 * 24 * time.Hour

	type clusterMessageBehavior int

	const (
		noCluster            clusterMessageBehavior = 1
		synchroniousCluster  clusterMessageBehavior = 2
		asynchroniousCluster clusterMessageBehavior = 3
	)

	tests := []struct {
		name                   string
		clusterMessageBehavior clusterMessageBehavior
	}{
		{
			// This situation is unsupported. It means a cluster of two SquirrelDB run
			// without being able to communicate. We will still try to don't break everything
			// but at least caching could be wrong.
			name:                   "test-without-redis",
			clusterMessageBehavior: noCluster,
		},
		{
			name:                   "dummy-cluster",
			clusterMessageBehavior: synchroniousCluster,
		},
		{
			name:                   "async-cluster",
			clusterMessageBehavior: asynchroniousCluster,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &mockStore{}
			lock := &mockLockFactory{}
			states := &mockState{}

			cluster := &dummy.LocalCluster{}
			wrappedCluster := newMockCluster(cluster)
			wrappedCluster.AutoProcessWithDelay(100 * time.Millisecond)

			var (
				index1Cluster types.Cluster
				index2Cluster types.Cluster
			)

			switch tt.clusterMessageBehavior {
			case noCluster:
				index1Cluster = &dummy.LocalCluster{}
				index2Cluster = &dummy.LocalCluster{}
			case synchroniousCluster:
				index1Cluster = cluster
				index2Cluster = cluster
			case asynchroniousCluster:
				index1Cluster = wrappedCluster
				index2Cluster = wrappedCluster
			}

			index1, err := initialize(
				t.Context(),
				store,
				Options{
					DefaultTimeToLive: defaultTTL,
					LockFactory:       lock,
					States:            states,
					Cluster:           index1Cluster,
				},
				newMetrics(prometheus.NewRegistry()),
				getTestLogger().With().Str("component", "index1").Logger(),
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
			t7 := t6.Add(24 * 370 * time.Hour)

			tmp, _, err := index1.lookupIDs(
				t.Context(),
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
				t.Context(),
				store,
				Options{
					DefaultTimeToLive: defaultTTL,
					LockFactory:       lock,
					States:            states,
					Cluster:           index2Cluster,
				},
				newMetrics(prometheus.NewRegistry()),
				getTestLogger().With().Str("component", "index2").Logger(),
			)
			if err != nil {
				t.Error(err)
			}

			tmp, _, err = index2.lookupIDs(
				t.Context(),
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
				t.Context(),
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
				t.Context(),
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

				hadIssue, err := index1.Verifier(buffer).
					WithNow(t0).
					WithStrictMetricCreation(true).
					Verify(t.Context())
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
				t.Context(),
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
			group, _ := errgroup.WithContext(t.Context())

			for n := range workerCount {
				group.Go(func() error {
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

						_, _, err := index.lookupIDs(t.Context(), toLookupRequests(labelsList[current:idxEnd], t3), t3)
						if err != nil {
							return err
						}

						current = idxEnd
					}

					return nil
				})
			}

			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}

			tmp, _, err = index1.lookupIDs(t.Context(), toLookupRequests(labelsList, t3), t4)
			if err != nil {
				t.Fatal(err)
			}

			tmp2, _, err := index2.lookupIDs(t.Context(), toLookupRequests(labelsList, t5), t5)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tmp, tmp2) {
				t.Errorf("Index don't have the same IDs")
			}

			if err := executeRunOnce(t.Context(), t5, index1, 1000, t6); err != nil {
				t.Fatal(err)
			}

			if err := executeRunOnce(t.Context(), t5, index2, 1000, t6); err != nil {
				t.Fatal(err)
			}

			labelsList = []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "expiration_conflict",
					ttlLabel:   "60",
				}),
			}

			labelsList2 := []labels.Labels{
				labels.FromMap(map[string]string{
					"__name__": "expiration_conflict2",
					ttlLabel:   "60",
				}),
			}

			tmp, _, err = index1.lookupIDs(t.Context(), toLookupRequests(labelsList, t5), t5)
			if err != nil {
				t.Fatal(err)
			}

			tmp2, _, err = index2.lookupIDs(t.Context(), toLookupRequests(labelsList, t5), t5)
			if err != nil {
				t.Fatal(err)
			}

			if tmp[0] != tmp2[0] {
				t.Errorf("lookupIDs() = %d, want %d", tmp2[0], tmp2[0])
			}

			if err := executeRunOnce(t.Context(), t6, index1, 1000, t6.Add(24*time.Hour)); err != nil {
				t.Fatal(err)
			}

			tmp, _, err = index1.lookupIDs(t.Context(), toLookupRequests(labelsList2, t6), t6)
			if err != nil {
				t.Fatal(err)
			}

			_, _, err = index2.lookupIDs(t.Context(), toLookupRequests(labelsList, t6), t6)
			if err != nil {
				t.Fatal(err)
			}

			if err := executeRunOnce(t.Context(), t6, index2, 1000, t6.Add(24*time.Hour)); err != nil {
				t.Fatal(err)
			}

			tmp2, _, err = index2.lookupIDs(t.Context(), toLookupRequests(labelsList, t6), t6)
			if err != nil {
				t.Fatal(err)
			}

			if tmp[0] == tmp2[0] {
				t.Errorf("lookupIDs() = %d, want != %d", tmp2[0], tmp[0])
			}

			buffer := bytes.NewBuffer(nil)

			indexes := []*CassandraIndex{index1, index2}

			for i, idx := range indexes {
				// applyExpirationUpdateRequests() didn't run on index1 after latest lookupIDs
				hadIssue, err := idx.Verifier(buffer).
					WithNow(t6).
					WithStrictMetricCreation(true).
					WithStrictExpiration(false).
					Verify(t.Context())
				if err != nil {
					t.Error(err)
				}

				if hadIssue {
					t.Errorf("index%d.Verify() had issues: %s", i+1, bufferToStringTruncated(buffer.Bytes()))
				}
			}

			if err := executeRunOnce(t.Context(), t6, index1, 1000, t6.Add(24*time.Hour)); err != nil {
				t.Fatal(err)
			}

			for i, idx := range indexes {
				hadIssue, err := idx.Verifier(buffer).
					WithNow(t6).
					WithStrictMetricCreation(true).
					WithStrictExpiration(true).
					Verify(t.Context())
				if err != nil {
					t.Error(err)
				}

				if hadIssue {
					t.Errorf("index%d.Verify() had issues: %s", i+1, bufferToStringTruncated(buffer.Bytes()))
				}
			}

			wrappedCluster.ProcessMessage()

			if err := executeRunOnce(t.Context(), t7, index1, 1000, t7.Add(24*time.Hour)); err != nil {
				t.Fatal(err)
			}

			for i, idx := range indexes {
				hadIssue, err := idx.Verifier(buffer).
					WithNow(t7).
					WithStrictMetricCreation(true).
					WithPedanticExpiration(true).
					Verify(t.Context())
				if err != nil {
					t.Error(err)
				}

				if hadIssue {
					t.Errorf("index%d.Verify() had issues: %s", i+1, bufferToStringTruncated(buffer.Bytes()))
				}
			}
		})
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
	t2 := t1.Add(shortTTL).Add(implementationDelay)
	// At t3, the entry refreshed at t1 expired and is deleted
	// At t3, also insert expireBatchSize + 10 metrics with shortTTL
	t3 := t2.Add(shortTTL).Add(implementationDelay)
	// At t4, check that nothing happened
	t4 := t3.Add(shortTTL - time.Second)
	// At t5, metrics added at t3 have expired and are deleted
	t5 := t4.Add(24 * time.Hour).Add(implementationDelay)
	// At t6, all metrics are expired and deleted
	t6 := t5.Add(updateDelay).Add(longTTL).Add(implementationDelay)

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
		metrics[n][ttlLabel] = strconv.FormatInt(secondTTL, 10)
	}

	store := &mockStore{}

	index, err := initialize(
		t.Context(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       &mockLockFactory{},
			States:            &mockState{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Error(err)
	}

	var ttls []int64

	labelsList := make([]labels.Labels, len(metrics))
	for i, m := range metrics {
		labelsList[i] = labelsMapToList(m, false)
	}

	metricsID, ttls, err := index.lookupIDs(t.Context(), toLookupRequests(labelsList, t0), t0)
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
	for n := range metrics {
		labels := labelsMapToList(metrics[n], true)
		id := metricsID[n]

		if !reflect.DeepEqual(store.id2labels[id], labels) {
			t.Errorf("id2labels[%d] = %v, want %v", id, store.id2labels[id], labels)
		}

		wantMinExpire := t0.Add(metricsTTL[n]).Add(cassandraTTLUpdateDelay)
		wantMaxExpire := t0.Add(metricsTTL[n]).Add(updateDelay)

		if store.id2expiration[id].After(wantMaxExpire) || store.id2expiration[id].Before(wantMinExpire) {
			t.Errorf(
				"id2expiration[%d (%s)] = %v, want between %v and %v",
				id,
				labels.String(),
				store.id2expiration[id],
				wantMinExpire,
				wantMaxExpire,
			)
		}

		if len(store.expiration) != 3 {
			t.Errorf("len(store.expiration) = %v, want 3", len(store.expiration))
		}
	}

	buffer := bytes.NewBuffer(nil)

	hadIssue, err := index.Verifier(buffer).
		WithNow(t0).
		WithStrictMetricCreation(true).
		WithPedanticExpiration(true).
		Verify(t.Context())
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	index.expire(t0)
	_, _ = index.cassandraExpire(t.Context(), t0)

	allIDs, err := index.AllIDs(t.Context(), t0, t0)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIDs))
	}

	hadIssue, err = index.Verifier(buffer).
		WithNow(t0).
		WithStrictMetricCreation(true).
		WithPedanticExpiration(true).
		Verify(t.Context())
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	labelsList[3] = labelsMapToList(metrics[3], false)

	ids, ttls, err := index.lookupIDs(t.Context(), toLookupRequests(labelsList[3:4], t1), t1)
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

	index.applyExpirationUpdateRequests(t.Context(), t1)
	// metrics[3] was moved to a new expiration slot
	if len(store.expiration) != 5 {
		t.Errorf("len(store.expiration) = %v, want 5", len(store.expiration))
	}

	for _, id := range []types.MetricID{metricsID[2], metricsID[3]} {
		expire := store.id2expiration[id].Truncate(24 * time.Hour)
		bitmap := roaring.NewBTreeBitmap()

		err = bitmap.UnmarshalBinary(store.expiration[expire.Unix()])
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
	for ts := t0; ts.Before(t1); ts = ts.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(t.Context(), t1)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t1)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 4 {
		t.Errorf("len(allIds) = %d, want 4", len(allIDs))
	}

	metrics[0][ttlLabel] = strconv.FormatInt(int64(shortTTL.Seconds()), 10)
	labelsList[0] = labelsMapToList(metrics[0], false)

	ids, ttls, err = index.lookupIDs(t.Context(), toLookupRequests(labelsList[0:1], t2), t2)
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

	for ts := t1; ts.Before(t2); ts = ts.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(t.Context(), t2)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t2)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 3 {
		t.Errorf("len(allIds) = %d, want 3", len(allIDs))
	}

	for _, id := range allIDs {
		if id == metricsID[2] {
			t.Errorf("allIds = %v and contains %d, want not contains this value", allIDs, metricsID[2])
		}
	}

	index.expire(t3)

	for ts := t2; ts.Before(t3); ts = ts.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(t.Context(), t3)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t3)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 2 {
		t.Errorf("len(allIds) = %d, want 2", len(allIDs))
	}

	labelsList = make([]labels.Labels, expireBatchSize+10)

	for n := range expireBatchSize + 10 {
		labels := map[string]string{
			"__name__": "filler",
			"id":       strconv.FormatInt(int64(n), 10),
			ttlLabel:   strconv.FormatInt(int64(shortTTL.Seconds()), 10),
		}
		labelsList[n] = labelsMapToList(labels, false)
	}

	_, _, err = index.lookupIDs(t.Context(), toLookupRequests(labelsList, t3), t3)
	if err != nil {
		t.Error(err)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t3)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 2+expireBatchSize+10 {
		t.Errorf("len(allIds) = %d, want %d", len(allIDs), 2+expireBatchSize+10)
	}

	index.expire(t4)

	for ts := t3; ts.Before(t4); ts = ts.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(t.Context(), t4)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t4)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 2+expireBatchSize+10 {
		t.Errorf("len(allIds) = %d, want %d", len(allIDs), 2+expireBatchSize+10)
	}

	index.expire(t5)

	for ts := t4; ts.Before(t5); ts = ts.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(t.Context(), t5)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t5)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 2 {
		t.Errorf("len(allIds) = %d, want 2", len(allIDs))
	}

	hadIssue, err = index.Verifier(buffer).
		WithNow(t5).
		WithStrictMetricCreation(true).
		WithPedanticExpiration(true).
		Verify(t.Context())
	if err != nil {
		t.Error(err)
	}

	if hadIssue {
		t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
	}

	index.expire(t6)

	for ts := t5; ts.Before(t6); ts = ts.Add(24 * time.Hour) {
		_, _ = index.cassandraExpire(t.Context(), t6)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t6)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 0 {
		t.Errorf("allIds = %v, want []", allIDs)
	}

	hadIssue, err = index.Verifier(buffer).
		WithNow(t6).
		WithStrictMetricCreation(true).
		WithPedanticExpiration(true).
		Verify(t.Context())
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
		ttlLabel:      strconv.FormatInt(int64(shortTTL.Seconds()), 10),
	}

	store := &mockStore{}

	index, err := initialize(
		t.Context(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       &mockLockFactory{},
			States:            &mockState{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Error(err)
	}

	// Check that the metric is in the index.
	labelsList := []labels.Labels{labelsMapToList(metric, false)}

	metricsID, ttls, err := index.lookupIDs(t.Context(), toLookupRequests(labelsList, t0), t0)
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
	_, _ = index.cassandraExpire(t.Context(), t0)

	allIDs, err := index.AllIDs(t.Context(), t0, t0)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 1 {
		t.Errorf("len(allIds) = %d, want 1", len(allIDs))
	}

	// t1
	index.expire(t1)
	// each call to cassandraExpire do one day, but calling multiple time
	// isn't an issue but it must be called at least once per day
	for ts := t0; ts.Before(t1); ts = ts.Add(expirationCheckInterval) {
		_, _ = index.cassandraExpire(t.Context(), t1)
	}

	allIDs, err = index.AllIDs(t.Context(), t0, t1)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 1 {
		t.Errorf("len(allIds) = %d, want 1", len(allIDs))
	}

	// t2
	index.expire(t2)
	_, _ = index.cassandraExpire(t.Context(), t2)

	allIDs, err = index.AllIDs(t.Context(), t0, t1)
	if err != nil {
		t.Error(err)
	}

	if len(allIDs) != 0 {
		t.Errorf("len(allIds) = %d, want 0", len(allIDs))
	}
}

// Test_expiration_longlived check that expiration also works when long-lived metrics exists
// (that is metric that continue to exists for longer that their TTL).
// It test that metrics are still present when they must be still present
// (e.g. don't expire BEFORE the TTL at time of write).
func Test_expiration_longlived(t *testing.T) { //nolint:maintidx
	shortestTTL := 1 * 24 * time.Hour
	shortTTL := 7 * 24 * time.Hour
	longTTL := 375 * 24 * time.Hour
	defaultTTL := longTTL
	step := 13 * time.Hour // mostly write 2 time per day, but not exactly aligned

	rnd := rand.New(rand.NewSource(1568706164))

	baseTime := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	// Duration of the phase will probably need to be updated, depending on implementation of purge.
	// Currently phase duration assume metrics stay for twice their TTL + postingsShardSize
	// Test have N phase:
	// * phase1: all metrics are written (including temporary).
	//           phase1 lasts 2*shortTTL+postingsShardSize time.
	//           At the end of phase1, the oldest shard contains metrics with longTTL.
	// 			 It also still contains metrics with shortTTL and shortestTTL even if they have expired
	// 			 because currently we only expire a shard after all metrics in it have expired.
	// * phase2: temporary are no longer written. Metrics that change TTL swap their TTL.
	//           phase2 last 2*shortTTL+postingsShardSize time.
	//           At the end of phase2, the oldest shard contains metrics with changed from long to short TTL.
	// 			 It also still contains metrics with shortTTL and shortestTTL even if they have expired
	// 			 because currently we only expire a shard after all metrics in it have expired.
	// * phase3: continue as previous phase for 2*longTTL+postingsShardSize
	//           At the end of phase3, all shards oldest that beginning of phase3 are empty.
	//           At the end of phase3, oldest non-empty shards only contains metrics with long TTL.
	// * phase4: no more write for 2*longTTL+postingsShardSize
	//           At the end of phase4, index is empty.
	// Also in all phase (but phase 4), few metrics are written with randomized TTL. The TTL change each new shard.
	// For this metric, we don't check if it's deleted from the shard but that it's still in the shard it belongs to.
	phase1End := baseTime.Add(2*shortTTL + postingShardSize)
	phase2End := phase1End.Add(2*shortTTL + postingShardSize)
	phase3End := phase2End.Add(2*longTTL + postingShardSize)
	phase4End := phase3End.Add(2*longTTL + postingShardSize)

	metricsLongTTL := []labels.Labels{
		labels.FromStrings(
			"__name__", "disk_used",
			"item", "/",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "uniqueName1",
			"uniqueLabel1", "samevalue",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
	}

	metricsRandomTTL := []labels.Labels{
		labels.FromStrings(
			"__name__", "disk_used",
			"item", "/dev/urandom",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "uniqueNameRnd",
			"uniqueLabelRnd", "samevalue",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "more_random",
			"device", "one",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "more_random",
			"device", "two",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
	}

	metricsShortTTL := []labels.Labels{
		labels.FromStrings(
			"__name__", "disk_used",
			"item", "/home",
			ttlLabel, strconv.FormatInt(int64(shortTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "uniqueName2",
			"uniqueLabel2", "samevalue",
			ttlLabel, strconv.FormatInt(int64(shortTTL.Seconds()), 10),
		),
	}

	metricsShortestTTL := []labels.Labels{
		labels.FromStrings(
			"__name__", "disk_used",
			"item", "/home2",
			ttlLabel, strconv.FormatInt(int64(shortestTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "uniqueName0",
			"uniqueLabel0", "samevalue",
			ttlLabel, strconv.FormatInt(int64(shortestTTL.Seconds()), 10),
		),
	}

	// TTL change switch between short & long TTL
	metricsLongToShortTTL := []labels.Labels{
		labels.FromStrings(
			"__name__", "disk_free",
			"item", "/",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "uniqueName3",
			"uniqueLabel3", "samevalue",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
	}

	metricsShortToLongTTL := []labels.Labels{
		labels.FromStrings(
			"__name__", "disk_free",
			"item", "/home",
			ttlLabel, strconv.FormatInt(int64(shortTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "uniqueName4",
			"uniqueLabel4", "samevalue",
			ttlLabel, strconv.FormatInt(int64(shortTTL.Seconds()), 10),
		),
	}

	metricsTemporary := []labels.Labels{
		labels.FromStrings(
			"__name__", "disk_total",
			"item", "/srv",
			ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10),
		),
		labels.FromStrings(
			"__name__", "uniqueName5",
			"uniqueLabel5", "samevalue",
			ttlLabel, strconv.FormatInt(int64(shortTTL.Seconds()), 10),
		),
	}

	store := &mockStore{}

	index, err := initialize(
		t.Context(),
		store,
		Options{
			DefaultTimeToLive: defaultTTL,
			LockFactory:       &mockLockFactory{},
			States:            &mockState{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Error(err)
	}

	// Map index of metricsRandomTTL -> shardID -> expiration
	rndExpirationByShard := make(map[int]map[int32]time.Time)
	currentTime := baseTime
	currentPhase := 1

	for i := range metricsRandomTTL {
		rndExpirationByShard[i] = make(map[int32]time.Time)
	}

	var currentShard int32

	for currentTime.Before(phase4End) {
		var metricToWrite []labels.Labels

		nextShard := shardForTime(currentTime.Unix())
		if currentShard != nextShard {
			for i := range metricsRandomTTL {
				builder := labels.NewBuilder(metricsRandomTTL[i])
				// randomize between shortestTTL & longTTL
				rndSecond := shortestTTL.Seconds() + rnd.Float64()*(longTTL.Seconds()-shortestTTL.Seconds())
				builder.Set(ttlLabel, strconv.FormatInt(int64(rndSecond), 10))
				metricsRandomTTL[i] = builder.Labels()

				rndExpirationByShard[i][nextShard] = currentTime.Add(time.Duration(rndSecond) * time.Second)
			}
		}

		currentShard = nextShard

		switch currentPhase {
		case 1:
			metricToWrite = append(metricToWrite, metricsLongTTL...)
			metricToWrite = append(metricToWrite, metricsShortTTL...)
			metricToWrite = append(metricToWrite, metricsShortestTTL...)
			metricToWrite = append(metricToWrite, metricsLongToShortTTL...)
			metricToWrite = append(metricToWrite, metricsShortToLongTTL...)
			metricToWrite = append(metricToWrite, metricsTemporary...)
			metricToWrite = append(metricToWrite, metricsRandomTTL...)
		case 2, 3:
			metricToWrite = append(metricToWrite, metricsLongTTL...)
			metricToWrite = append(metricToWrite, metricsShortTTL...)
			metricToWrite = append(metricToWrite, metricsShortestTTL...)
			metricToWrite = append(metricToWrite, metricsLongToShortTTL...)
			metricToWrite = append(metricToWrite, metricsShortToLongTTL...)
			metricToWrite = append(metricToWrite, metricsRandomTTL...)
		}

		rnd.Shuffle(len(metricToWrite), func(i, j int) {
			metricToWrite[i], metricToWrite[j] = metricToWrite[j], metricToWrite[i]
		})

		_, _, err := index.lookupIDs(
			t.Context(),
			toLookupRequests(metricToWrite, currentTime),
			currentTime,
		)
		if err != nil {
			t.Fatal(err)
		}

		index.InternalRunOnce(t.Context(), currentTime)

		writeTime := currentTime
		currentTime = currentTime.Add(step)

		var nextPhase int

		switch {
		case currentTime.Before(phase1End):
			nextPhase = 1
		case currentTime.Before(phase2End):
			nextPhase = 2
		case currentTime.Before(phase3End):
			nextPhase = 3
		default:
			nextPhase = 4
		}

		if currentPhase != nextPhase {
			err = expirationLonglivedEndOfPhaseCheck(
				t,
				store,
				index,
				currentTime,
				metricToWrite,
				metricsLongToShortTTL,
				metricsRandomTTL,
				writeTime,
				currentPhase,
				phase1End,
				longTTL,
			)
			if err != nil {
				t.Fatalf("at end of phase %d: %v", currentPhase, err)
			}

			err = expirationLonglivedRndCheck(
				t,
				store,
				currentTime,
				rndExpirationByShard,
				metricsRandomTTL,
			)
			if err != nil {
				t.Fatalf("at end of phase %d: %v", currentPhase, err)
			}
		}

		switch {
		case currentPhase == 1 && nextPhase == 2:
			// End of phase 1
			oldestShard := shardForTime(baseTime.Unix())

			var present []labels.Labels

			// metricsShortTTL, metricsShortestTTL and metricsShortToLongTTL have expired, but they
			// are currently still in the shard because the shard only expires when all metrics in it
			// have expired. We don't check if they are present because they don't need to be present.
			present = append(present, metricsLongTTL...)
			present = append(present, metricsLongToShortTTL...)

			err = store.verifyShard(
				t,
				oldestShard,
				dropTTLFromMetricList(present),
				nil,
			)
			if err != nil {
				t.Fatalf("at end of phase %d: %v", currentPhase, err)
			}

			// Update TTL of metrics
			for i := range metricsLongToShortTTL {
				builder := labels.NewBuilder(metricsLongToShortTTL[i])
				builder.Set(ttlLabel, strconv.FormatInt(int64(shortTTL.Seconds()), 10))
				metricsLongToShortTTL[i] = builder.Labels()
			}

			for i := range metricsShortToLongTTL {
				builder := labels.NewBuilder(metricsShortToLongTTL[i])
				builder.Set(ttlLabel, strconv.FormatInt(int64(longTTL.Seconds()), 10))
				metricsShortToLongTTL[i] = builder.Labels()
			}
		case currentPhase == 2 && nextPhase == 3:
			// End of phase 2
			oldestShard := shardForTime(baseTime.Unix())

			var present []labels.Labels

			// metricsShortTTL, metricsShortestTTL and metricsShortToLongTTL have expired, but they
			// are currently still in the shard because the shard only expires when all metrics in it
			// have expired. We don't check if they are present because they don't need to be present.
			present = append(present, metricsLongTTL...)
			present = append(present, metricsLongToShortTTL...)

			err = store.verifyShard(
				t,
				oldestShard,
				dropTTLFromMetricList(present),
				nil,
			)
			if err != nil {
				t.Fatalf("at end of phase %d: %v", currentPhase, err)
			}
		case currentPhase == 3 && nextPhase == 4:
			// End of phase 3
			minShard := shardForTime(currentTime.Unix())

			for shard := range store.postings {
				if shard < minShard && shard != globalShardNumber {
					minShard = shard
				}
			}

			shardCutoff := shardForTime(phase2End.Unix())

			if minShard <= shardCutoff {
				t.Errorf(
					"minShard = %s (%d), want greater than %s (%d)",
					timeForShard(minShard).Format(shardDateFormat),
					minShard,
					timeForShard(shardCutoff).Format(shardDateFormat),
					shardCutoff,
				)
			}

			var present []labels.Labels

			// metricsShortTTL, metricsShortestTTL and metricsShortToLongTTL have expired, but they
			// are currently still in the shard because the shard only expires when all metrics in it
			// have expired. We don't check if they are present because they don't need to be present.
			present = append(present, metricsLongTTL...)
			present = append(present, metricsShortToLongTTL...)

			// Temporary metrics are absent because they have expired.
			absent := metricsTemporary

			err = store.verifyShard(
				t,
				minShard,
				dropTTLFromMetricList(present),
				dropTTLFromMetricList(absent),
			)
			if err != nil {
				t.Fatalf("at end of phase %d: %v", currentPhase, err)
			}
		}

		currentPhase = nextPhase
	}

	if err := store.verifyStoreEmpty(t); err != nil {
		t.Error(err)
	}
}

func expirationLonglivedEndOfPhaseCheck(
	t *testing.T,
	store *mockStore,
	index *CassandraIndex,
	currentTime time.Time,
	metricToWrite []labels.Labels,
	metricsLongToShortTTL []labels.Labels,
	metricsRandomTTL []labels.Labels,
	writeTime time.Time,
	currentPhase int,
	phase1End time.Time,
	longTTL time.Duration,
) error {
	t.Helper()

	var errs prometheus.MultiError

	wantedExpiration := make([]time.Time, 0, len(metricToWrite))

	for _, metric := range metricToWrite {
		ignoreExpiration := false

		for _, m := range metricsRandomTTL {
			if labels.Equal(metric, m) {
				ignoreExpiration = true
			}
		}

		if ignoreExpiration {
			wantedExpiration = append(wantedExpiration, time.Time{})

			continue
		}

		ttl, err := strconv.ParseInt(metric.Get(ttlLabel), 10, 0)
		if err != nil {
			return err
		}

		metricExpiration := writeTime.Add(time.Duration(ttl) * time.Second)

		// From phase2 or more, if the metric belong to metricsLongToShortTTL,
		// then the expiration is at least phase1End+longTTL
		if currentPhase >= 2 {
			isLongToShort := false
			minExpiration := phase1End.Add(longTTL)

			for _, m := range metricsLongToShortTTL {
				if labels.Equal(metric, m) {
					isLongToShort = true
				}
			}

			if isLongToShort && metricExpiration.Before(minExpiration) {
				metricExpiration = minExpiration
			}
		}

		wantedExpiration = append(wantedExpiration, metricExpiration)
	}

	// At the end of any phase, index contains all metrics currently written
	err := store.verifyStore(
		t,
		currentTime,
		dropTTLFromMetricList(metricToWrite),
		wantedExpiration,
		true,
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("at end of phase %d: %w", currentPhase, err))
	}

	// At the end of any phase, the most recent shard contains all metrics currently write
	err = store.verifyShard(
		t,
		shardForTime(writeTime.Unix()),
		dropTTLFromMetricList(metricToWrite),
		nil,
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("at end of phase %d: %w", currentPhase, err))
	}

	buffer := bytes.NewBuffer(nil)

	hadError, err := index.Verifier(buffer).
		WithNow(currentTime).
		WithStrictMetricCreation(true).
		WithPedanticExpiration(true).
		Verify(t.Context())
	if err != nil {
		errs = append(errs, err)
	}

	if hadError {
		errs = append(errs, fmt.Errorf("at end of phase %d, verify had error, message=%s", currentPhase, buffer.String()))
	}

	if errs != nil {
		return errs
	}

	return nil
}

// expirationLonglivedRndCheck check that metrics still exist in shard if they are not expired.
// This function only checks if metrics are present, it doesn't check that they are absent
// when they expired.
func expirationLonglivedRndCheck(
	t *testing.T,
	store *mockStore,
	currentTime time.Time,
	rndExpirationByShard map[int]map[int32]time.Time,
	metricsRandomTTL []labels.Labels,
) error {
	t.Helper()

	var errs prometheus.MultiError

	metricIDs := make([]uint64, len(metricsRandomTTL))

	for i, metric := range dropTTLFromMetricList(metricsRandomTTL) {
		id, ok := store.labels2id[metric.String()]
		if !ok {
			errs.Append(fmt.Errorf("metric %s isn't found in store", metric))
		}

		metricIDs[i] = uint64(id)
	}

	for idx, shardExpirations := range rndExpirationByShard {
		for shard, expiration := range shardExpirations {
			maybePresent, allPosting, err := store.getPresencePostings(shard)
			if err != nil {
				return err
			}

			if currentTime.After(expiration) {
				continue
			}

			id := metricIDs[idx]
			if id == 0 {
				continue
			}

			if !maybePresent.Contains(id) {
				errs.Append(fmt.Errorf(
					"metric %s not present in maybePresent of shard %s (%d). It should expire after %s",
					metricsRandomTTL[idx],
					timeForShard(shard).Format(shardDateFormat),
					shard,
					expiration,
				))
			}

			if !allPosting.Contains(id) {
				errs.Append(fmt.Errorf(
					"metric %s not present in allPosting of shard %s (%d). It should expire after %s",
					metricsRandomTTL[idx],
					timeForShard(shard).Format(shardDateFormat),
					shard,
					expiration,
				))
			}
		}
	}

	return errs.MaybeUnwrap()
}

func Test_getTimeShards(t *testing.T) { //nolint:maintidx
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
	baseShardID := int32(reference.Unix()/3600) / shardSize * shardSize
	base := time.Unix(int64(baseShardID)*3600, 0)

	nowShard := int32(now.Unix()/3600) / shardSize * shardSize

	tests := []struct {
		name           string
		args           args
		existingShards []int32
		want           []int32
		wantReturnAll  []int32
	}{
		{
			name: "now",
			args: args{
				start: now,
				end:   now,
			},
			existingShards: []int32{nowShard - shardSize, nowShard, nowShard + shardSize},
			want:           []int32{nowShard},
			wantReturnAll:  []int32{nowShard},
		},
		{
			name: "now-empty-index",
			args: args{
				start: now,
				end:   now,
			},
			existingShards: []int32{},
			want:           []int32{},
			wantReturnAll:  []int32{nowShard},
		},
		{
			name: "now-not-existing",
			args: args{
				start: now,
				end:   now,
			},
			existingShards: []int32{nowShard - shardSize, nowShard + shardSize},
			want:           []int32{},
			wantReturnAll:  []int32{nowShard},
		},
		{
			name: "base",
			args: args{
				start: base,
				end:   base,
			},
			existingShards: []int32{baseShardID},
			want:           []int32{baseShardID},
			wantReturnAll:  []int32{baseShardID},
		},
		{
			name: "reference",
			args: args{
				start: reference,
				end:   reference,
			},
			existingShards: []int32{baseShardID},
			want:           []int32{baseShardID},
			wantReturnAll:  []int32{baseShardID},
		},
		{
			// This test assume that reference+2h do NOT change its baseTime
			name: "reference+2h",
			args: args{
				start: reference,
				end:   reference.Add(2 * time.Hour),
			},
			existingShards: []int32{baseShardID},
			want:           []int32{baseShardID},
			wantReturnAll:  []int32{baseShardID},
		},
		{
			name: "reference+postingShardSize",
			args: args{
				start: reference,
				end:   reference.Add(postingShardSize),
			},
			existingShards: []int32{baseShardID, baseShardID + shardSize},
			want:           []int32{baseShardID, baseShardID + shardSize},
			wantReturnAll:  []int32{baseShardID, baseShardID + shardSize},
		},
		{
			name: "base+postingShardSize",
			args: args{
				start: base,
				end:   base.Add(postingShardSize),
			},
			existingShards: []int32{baseShardID, baseShardID + shardSize},
			want:           []int32{baseShardID, baseShardID + shardSize},
			wantReturnAll:  []int32{baseShardID, baseShardID + shardSize},
		},
		{
			name: "base+postingShardSize- 1seconds",
			args: args{
				start: base,
				end:   base.Add(postingShardSize).Add(-time.Second),
			},
			existingShards: []int32{baseShardID, baseShardID + shardSize},
			want:           []int32{baseShardID},
			wantReturnAll:  []int32{baseShardID},
		},
		{
			// This test assume postingShardSize is at least > 2h
			name: "first-valid-shard",
			args: args{
				start: indexMinValidTime.Add(20 * time.Minute),
				end:   indexMinValidTime.Add(time.Hour + 59*time.Minute),
			},
			existingShards: []int32{8736},
			want:           []int32{8736},
			wantReturnAll:  []int32{8736},
		},
		{
			name: "reference_5_postingShardSize_wide",
			args: args{
				start: reference.Add(-2 * postingShardSize),
				end:   reference.Add(2 * postingShardSize),
			},
			existingShards: []int32{baseShardID - 2*shardSize, baseShardID, baseShardID + shardSize, baseShardID + 10*shardSize},
			want:           []int32{baseShardID - 2*shardSize, baseShardID, baseShardID + shardSize},
			wantReturnAll: []int32{
				baseShardID - 2*shardSize, baseShardID - shardSize, baseShardID, baseShardID + shardSize, baseShardID + 2*shardSize,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			index, err := initialize(
				t.Context(),
				&mockStore{},
				Options{
					DefaultTimeToLive: 365 * 24 * time.Hour,
					LockFactory:       &mockLockFactory{},
					Cluster:           &dummy.LocalCluster{},
				},
				newMetrics(prometheus.NewRegistry()),
				getTestLogger().With().Str("component", "index").Logger(),
			)
			if err != nil {
				t.Fatal(err)
			}

			if len(tt.existingShards) > 0 {
				newShard := make([]uint64, 0, len(tt.existingShards))

				for _, v := range tt.existingShards {
					newShard = append(newShard, uint64(v))
				}

				_, err := index.postingUpdate(t.Context(), postingUpdateRequest{
					Shard: globalShardNumber,
					Label: labels.Label{
						Name:  existingShardsLabel,
						Value: existingShardsLabel,
					},
					AddIDs: newShard,
				})
				if err != nil && !errors.Is(err, errBitmapEmpty) {
					t.Fatal(err)
				}
			}

			gotAll, err := index.getTimeShards(t.Context(), tt.args.start, tt.args.end, true)
			if err != nil {
				t.Error(err)
			}

			gotNotAll, err := index.getTimeShards(t.Context(), tt.args.start, tt.args.end, false)
			if err != nil {
				t.Error(err)
			}

			if diff := cmp.Diff(tt.wantReturnAll, gotAll, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("getTimeShards(returnall) mismatch: (-want +got)\n%s", diff)
			}

			if diff := cmp.Diff(tt.want, gotNotAll, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("getTimeShards() mismatch: (-want +got)\n%s", diff)
			}

			got, err := index.getTimeShards(t.Context(), indexMinValidTime, time.Now(), true)
			if err != nil {
				t.Error(err)
			}

			for i, shard := range got {
				if shard == globalShardNumber {
					t.Errorf("getTimeShards(returnall)[%d] = %v, want != %v", i, shard, globalShardNumber)
				}
			}

			got, err = index.getTimeShards(t.Context(), indexMinValidTime, time.Now(), false)
			if err != nil {
				t.Error(err)
			}

			for i, shard := range got {
				if shard == globalShardNumber {
					t.Errorf("getTimeShards()[%d] = %v, want != %v", i, shard, globalShardNumber)
				}
			}
		})
	}
}

func Test_FilteredLabelValues(t *testing.T) { //nolint:maintidx
	t0 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	t1 := t0.Add(postingShardSize)
	t2 := t1.Add(postingShardSize)
	t3 := t2.Add(postingShardSize * 2)
	now := t3.Add(postingShardSize * 2)

	index1, err := initialize(
		t.Context(),
		&mockStore{},
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index1").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = index1.lookupIDs(
		t.Context(),
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
			got, err := tt.index.labelValues(t.Context(), tt.start, tt.end, tt.labelName, tt.matchers)
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
			if got := timeForShard(shardForTime(tt.inputTime.Unix())); !got.Equal(tt.want) {
				t.Errorf("timeForShard() = %s, want %s", got, tt.want)
			}
		})
	}
}

func executeRunOnce(
	ctx context.Context,
	now time.Time,
	index *CassandraIndex,
	maxRunCount int,
	maxTime time.Time,
) error {
	previousLastProcessedDay, err := index.expirationLastProcessedDay(ctx)
	if err != nil {
		return err
	}

	currentNow := now
	allWorkDone := false

	// RunOnce does few actions that could fail is store is failing:
	// * periodicRefreshIDInShard: error for this one will be ignored
	// * applyExpirationUpdateRequests: we can check index.expirationUpdateRequests to check for completion
	// * cassandraExpire: we can check expirationLastProcessedDay to check for completion
	for runCount := range maxRunCount {
		// If we are close to maxRunCount or maxTime, enable skipping errors
		if runCount > maxRunCount*3/4 && !shouldSkipError(ctx) {
			ctx = contextSkipError(ctx) //nolint: fatcontext
		}

		if maxTime.Sub(currentNow) < 300*time.Minute && !shouldSkipError(ctx) {
			ctx = contextSkipError(ctx)
		}

		moreWore := index.InternalRunOnce(ctx, currentNow)

		if moreWore {
			// when moreWork is true, run call to cassandraExpire isn't delayed
			currentNow = currentNow.Add(backgroundCheckInterval)
		} else {
			// when moreWork is false, it's either due to an error or because all works are done.
			// For the later case, we break the for-loop few lines below.
			// For error case, cassandraExpire will be delayed by up to 15 minutes, so add 15 minutes to current time.
			currentNow = currentNow.Add(15 * time.Minute)

			lastProcessedDay, err := index.expirationLastProcessedDay(ctx)
			if err != nil {
				return err
			}

			// We finished our work if:
			// * the expirationLastProcessedDay didn't changed
			// * the expirationLastProcessedDay is close enough to now (we have some margin, because
			//	 the real value depends on implemention).
			// * expirationUpdateRequests are all processed.
			if previousLastProcessedDay.Equal(lastProcessedDay) &&
				lastProcessedDay.Add(48*time.Hour).After(now) &&
				len(index.expirationUpdateRequests) == 0 {
				allWorkDone = true

				break
			}

			previousLastProcessedDay = lastProcessedDay
		}

		if currentNow.After(maxTime) {
			return fmt.Errorf("RunOnce didn't completed it work before the max-time %s", maxTime)
		}
	}

	if !allWorkDone {
		return fmt.Errorf("RunOnce didn't completed it work after %d runs", maxRunCount)
	}

	return nil
}

// Test_store_errors try that in case of store errors, it will recovery and end in correct state.
// In this test we will:
//   - At t1 to t4: insert metrics (metrics1 to metrics4 respectively), run the expiration & the index verify
//   - During each insert & expiration processing, the store have a failure rate of 10%. We retry the
//     insert & expiration until it succeed.
//   - We repeat the whole processed few times, since the failure are randomized.
func Test_store_errors(t *testing.T) { //nolint:maintidx
	defaultTTL := 365 * 24 * time.Hour
	// updateDelay is the delay after which we are guaranteed to trigger an TTL update
	updateDelay := cassandraTTLUpdateDelay + cassandraTTLUpdateJitter + time.Second
	defaultRetryCount := 50

	t1 := time.Date(2019, 9, 17, 7, 42, 44, 0, time.UTC)
	t2 := t1.Add(updateDelay)
	t3 := t2.Add(postingShardSize)
	t4 := t3.Add(300 * 24 * time.Hour)
	t5 := t4.Add(defaultTTL + 72*time.Hour)
	metrics := []map[string]string{
		{
			"__name__": "up",
		},
		{
			"__name__": "disk_used",
			"item":     "/",
		},
		{
			"__name__": "disk_used",
			"item":     "/home",
		},
		{
			"__name__": "disk_used",
			"item":     "/srv",
		},
		{
			"__name__": "cpu_used",
		},
	}
	metrics2 := []map[string]string{
		{
			"__name__": "up",
		},
		{
			"__name__": "disk_used",
			"item":     "/",
		},
		{
			"__name__": "disk_used",
			"item":     "/home",
		},
		{
			"__name__": "disk_used",
			"item":     "/mnt",
		},
		{
			"__name__": "cpu_used",
		},
	}
	metrics3 := []map[string]string{
		{
			"__name__": "up",
		},
		{
			"__name__": "disk_used",
			"item":     "/",
		},
		{
			"__name__": "disk_used",
			"item":     "/maison",
		},
		{
			"__name__": "disk_used",
			"item":     "/mnt",
		},
		{
			"__name__": "cpu_used",
		},
	}
	metrics4 := []map[string]string{
		{
			"__name__": "up",
		},
		{
			"__name__": "disk_free",
			"item":     "/",
		},
		{
			"__name__": "disk_used",
			"item":     "/maison",
		},
		{
			"__name__": "disk_used",
			"item":     "/mnt",
		},
		{
			"__name__": "cpu_used",
		},
	}

	unknownError := errors.New("this error isn't expected by code")

	runs := []struct {
		name           string
		failRateBefore float64
		failRateAfter  float64
		err            error
		rndSeed        int64
	}{
		{
			name:           "no error",
			failRateBefore: 0,
			failRateAfter:  0,
			err:            nil,
		},
		{
			name:           "before-1",
			failRateBefore: 0.1,
			failRateAfter:  0,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "before-2",
			failRateBefore: 0.1,
			failRateAfter:  0,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        8685443574379551565, // this seed was known to generate an error when all concurrent* variable are 1.
		},
		{
			name:           "before-3",
			failRateBefore: 0.1,
			failRateAfter:  0,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        1184364543667845743, // this seed was known to generate an error when all concurrent* variable are 1.
		},
		{
			name:           "before-4",
			failRateBefore: 0.05,
			failRateAfter:  0,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "before-rnd",
			failRateBefore: 0.1,
			failRateAfter:  0,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        rand.Int63(),
		},
		{
			name:           "before-unknown-error",
			failRateBefore: 0.1,
			failRateAfter:  0,
			err:            unknownError,
			rndSeed:        42,
		},
		{
			name:           "after-1",
			failRateBefore: 0,
			failRateAfter:  0.1,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "after-2",
			failRateBefore: 0,
			failRateAfter:  0.1,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        8685443574379551565, // this seed was known to generate an error when all concurrent* variable are 1.
		},
		{
			name:           "after-3",
			failRateBefore: 0,
			failRateAfter:  0.1,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        1184364543667845743, // this seed was known to generate an error when all concurrent* variable are 1.
		},
		{
			name:           "after-4",
			failRateBefore: 0,
			failRateAfter:  0.05,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "after-rnd",
			failRateBefore: 0,
			failRateAfter:  0.1,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        rand.Int63(),
		},
		{
			name:           "after-unknown-error",
			failRateBefore: 0,
			failRateAfter:  0.1,
			err:            unknownError,
			rndSeed:        42,
		},
		{
			name:           "both-1",
			failRateBefore: 0.1,
			failRateAfter:  0.1,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        1184364543667845743, // this seed was known to generate an error when all concurrent* variable are 1.
		},
		{
			name:           "both-2",
			failRateBefore: 0.1,
			failRateAfter:  0.1,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "both-rnd",
			failRateBefore: 0.1,
			failRateAfter:  0.1,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        rand.Int63(),
		},
		{
			name:           "both-lower-1",
			failRateBefore: 0.05,
			failRateAfter:  0.05,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "both-lower-2",
			failRateBefore: 0.02,
			failRateAfter:  0.02,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "both-lower-3",
			failRateBefore: 0.01,
			failRateAfter:  0.01,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
		{
			name:           "both-lower-4",
			failRateBefore: 0.005,
			failRateAfter:  0.005,
			err:            gocql.ErrTimeoutNoResponse,
			rndSeed:        2015,
		},
	}

	batches := []struct {
		now     time.Time
		metrics []map[string]string
	}{
		{t1, metrics},
		{t2, metrics2},
		{t3, metrics3},
		{t4, metrics4},
		{t5, nil},
	}

	for _, run := range runs {
		t.Run(run.name, func(t *testing.T) {
			verifyResults := make([]*bytes.Buffer, 0, len(batches))
			verifyHadErrors := make([]bool, len(batches))

			for range batches {
				verifyResults = append(verifyResults, bytes.NewBuffer(nil))
			}

			shouldFail := newRandomShouldFail(run.rndSeed, run.err)

			realStore := &mockStore{}
			store := newFailingStore(realStore, shouldFail)

			index, err := initialize(
				t.Context(),
				store,
				Options{
					DefaultTimeToLive: defaultTTL,
					LockFactory:       &mockLockFactory{},
					States:            &mockState{},
					Cluster:           &dummy.LocalCluster{},
				},
				newMetrics(prometheus.NewRegistry()),
				getTestLogger().With().Str("component", "index").Logger(),
			)
			if err != nil {
				t.Error(err)
			}

			for batchIdx, batch := range batches {
				var nextNow time.Time

				if batchIdx+1 < len(batches) {
					nextNow = batches[batchIdx+1].now
				} else {
					nextNow = batch.now.Add(365 * 24 * time.Hour)
				}

				labelsList := make([]labels.Labels, len(batch.metrics))
				for i, m := range batch.metrics {
					labelsList[i] = labelsMapToList(m, false)
				}

				shouldFail.SetRate(run.failRateBefore, run.failRateAfter)

				for i := range defaultRetryCount {
					ctx := t.Context()
					if i == defaultRetryCount-1 {
						ctx = contextSkipError(ctx)
					}

					_, _, err = index.lookupIDs(ctx, toLookupRequests(labelsList, batch.now), batch.now)
					if err == nil {
						break
					}
				}

				if err != nil {
					t.Fatalf("on batch %d: %v", batchIdx, err)
				}

				// Check in store that correct write happened
				// Note: no error even if store still had 10% failure, because everything is in cache.
				_, _, err := index.lookupIDs(t.Context(), toLookupRequests(labelsList, batch.now), batch.now)
				if err != nil {
					t.Fatalf("on batch %d: %v", batchIdx, err)
				}

				err = realStore.verifyStore(
					t,
					batch.now,
					mapsToLabelsList(batch.metrics),
					sameExpirations(len(batch.metrics),
						batch.now.Add(defaultTTL)),
					true,
				)
				if err != nil {
					t.Fatal(err)
				}

				if err := executeRunOnce(t.Context(), batch.now, index, 2000, nextNow); err != nil {
					t.Fatalf("on batch %d: %v", batchIdx, err)
				}

				err = realStore.verifyStore(
					t,
					batch.now,
					mapsToLabelsList(batch.metrics),
					sameExpirations(len(batch.metrics),
						batch.now.Add(defaultTTL)),
					true,
				)
				if err != nil {
					t.Fatal(err)
				}

				shouldFail.SetRate(0, 0)

				verifyHadErrors[batchIdx], err = index.Verifier(verifyResults[batchIdx]).
					WithNow(batch.now).
					WithStrictMetricCreation(true).
					WithPedanticExpiration(true).
					Verify(t.Context())
				if err != nil {
					t.Error(err)
				}

				if batchIdx == len(batches)-1 && verifyHadErrors[batchIdx] {
					for i := range verifyResults {
						t.Errorf("batch %d, hadError=%v message=%s", i, verifyHadErrors[i], verifyResults[i].String())
					}
				}

				if batchIdx == 4 {
					if err := realStore.verifyStoreEmpty(t); err != nil {
						t.Error(err)
					}
				}
			}
		})
	}
}

// Test_cluster_expiration_and_error will tests multiple SquirrelDB with expiration AND error.
func Test_cluster_expiration_and_error(t *testing.T) { //nolint:maintidx
	defaultTTL := 10 * 24 * time.Hour
	baseTime := time.Date(2019, 9, 1, 0, 0, 0, 0, time.UTC)

	runs := []struct {
		name                string
		node1FailRateBefore float64
		node1FfailRateAfter float64
		node2FailRateBefore float64
		node2FfailRateAfter float64
		err                 error
		rndSeed             int64
	}{
		{
			name:                "no-error",
			node1FailRateBefore: 0,
			node1FfailRateAfter: 0,
			node2FailRateBefore: 0,
			node2FfailRateAfter: 0,
			err:                 nil,
		},
		{
			name:                "before-rnd",
			node1FailRateBefore: 0.1,
			node1FfailRateAfter: 0,
			node2FailRateBefore: 0.1,
			node2FfailRateAfter: 0,
			err:                 gocql.ErrTimeoutNoResponse,
			rndSeed:             rand.Int63(),
		},
		{
			name:                "after-rnd",
			node1FailRateBefore: 0,
			node1FfailRateAfter: 0.1,
			node2FailRateBefore: 0,
			node2FfailRateAfter: 0.1,
			err:                 gocql.ErrTimeoutNoResponse,
			rndSeed:             rand.Int63(),
		},
		{
			name:                "node1-fail",
			node1FailRateBefore: 0.03,
			node1FfailRateAfter: 0.01,
			node2FailRateBefore: 0,
			node2FfailRateAfter: 0,
			err:                 gocql.ErrTimeoutNoResponse,
			rndSeed:             2019,
		},
		{
			name:                "node2-fail",
			node1FailRateBefore: 0,
			node1FfailRateAfter: 0,
			node2FailRateBefore: 0.04,
			node2FfailRateAfter: 0.01,
			err:                 gocql.ErrTimeoutNoResponse,
			rndSeed:             2019,
		},
	}

	for _, withClusterConfigured := range []bool{false, true} {
		for _, run := range runs {
			fullName := fmt.Sprintf("%s-cluster-%v", run.name, withClusterConfigured)

			t.Run(fullName, func(t *testing.T) {
				expectedMetrics := make(map[string]metricLabelsExpiration)
				realStore := &mockStore{}
				lock := &mockLockFactory{}
				states := &mockState{}

				cluster := &dummy.LocalCluster{}
				wrappedCluster := newMockCluster(cluster)
				wrappedCluster.AutoProcessWithDelay(100 * time.Millisecond)

				var (
					index1Cluster types.Cluster
					index2Cluster types.Cluster
				)

				if withClusterConfigured {
					index1Cluster = wrappedCluster
					index2Cluster = wrappedCluster
				} else {
					index1Cluster = &dummy.LocalCluster{}
					index2Cluster = &dummy.LocalCluster{}
				}

				rnd := rand.New(rand.NewSource(run.rndSeed))

				shouldFail1 := newRandomShouldFail(rnd.Int63(), run.err)
				shouldFail2 := newRandomShouldFail(rnd.Int63(), run.err)

				index1, err := initialize(
					t.Context(),
					newFailingStore(realStore, shouldFail1),
					Options{
						DefaultTimeToLive: defaultTTL,
						LockFactory:       lock,
						States:            states,
						Cluster:           index1Cluster,
					},
					newMetrics(prometheus.NewRegistry()),
					getTestLogger().With().Str("component", "index2").Logger(),
				)
				if err != nil {
					t.Fatal(err)
				}

				index2, err := initialize(
					t.Context(),
					newFailingStore(realStore, shouldFail2),
					Options{
						DefaultTimeToLive: defaultTTL,
						LockFactory:       lock,
						States:            states,
						Cluster:           index2Cluster,
					},
					newMetrics(prometheus.NewRegistry()),
					getTestLogger().With().Str("component", "index2").Logger(),
				)
				if err != nil {
					t.Fatal(err)
				}

				indexes := []*CassandraIndex{index1, index2}

				var metricToWrite []labels.Labels

				currentTime := baseTime
				step := 18 * time.Hour

				for currentTime.Before(baseTime.Add(22 * 24 * time.Hour)) {
					// Each 18 hours, write some metrics:
					// We will have both brand new metrics and few reused from previous run.
					// In total metricsNew + metricsReuse metrics will be wrote (except very first run).
					// This list is split in 3 part: common, unique to node 1 and unique to node 2.
					currentTime = currentTime.Add(18 * time.Hour)

					// To speed-up test, do a just after 7 days (one shard time)
					if currentTime.After(baseTime.Add(postingShardSize)) &&
						currentTime.Before(baseTime.Add(postingShardSize).Add(step)) {
						currentTime = currentTime.Add(12 * 24 * time.Hour)
					}

					metricToWrite, expectedMetrics = clusterDoOneBatch(
						t,
						currentTime,
						rnd,
						defaultTTL,
						metricToWrite,
						expectedMetrics,
						indexes,
					)

					allExpectedLabels := make([]labels.Labels, 0, len(expectedMetrics))
					allExpectedExpirations := make([]time.Time, 0, len(expectedMetrics))

					for _, metric := range expectedMetrics {
						allExpectedLabels = append(allExpectedLabels, metric.labels)
						allExpectedExpirations = append(allExpectedExpirations, metric.Expiration)
					}

					err = realStore.verifyStore(
						t,
						currentTime,
						allExpectedLabels,
						allExpectedExpirations,
						false, // expiration are likely to not be applied, because RunOnce isn't retried until no failure.
					)
					if err != nil {
						t.Fatalf("at %s: %v", currentTime, err)
					}
				}

				if err := clusterCheckSearch(t.Context(), t, currentTime, index1, realStore); err != nil {
					t.Error(err)
				}

				if err := clusterCheckSearch(t.Context(), t, currentTime, index2, realStore); err != nil {
					t.Error(err)
				}

				allExpectedLabels := make([]labels.Labels, 0, len(expectedMetrics))
				allExpectedExpirations := make([]time.Time, 0, len(expectedMetrics))

				for _, metric := range expectedMetrics {
					allExpectedLabels = append(allExpectedLabels, metric.labels)
					allExpectedExpirations = append(allExpectedExpirations, metric.Expiration)
				}

				err = realStore.verifyStore(
					t,
					currentTime,
					allExpectedLabels,
					allExpectedExpirations,
					false, // expiration are likely to not be applied, because RunOnce isn't retried until no failure.
				)
				if err != nil {
					t.Fatal(err)
				}

				group, _ := errgroup.WithContext(t.Context())

				for _, index := range indexes {
					group.Go(func() error {
						return executeRunOnce(t.Context(), currentTime, index, 2000, currentTime.Add(10*24*time.Hour))
					})
				}

				if err := group.Wait(); err != nil {
					t.Fatal(err)
				}

				err = realStore.verifyStore(
					t,
					currentTime,
					allExpectedLabels,
					allExpectedExpirations,
					true,
				)
				if err != nil {
					t.Fatal(err)
				}

				wrappedCluster.ProcessMessage()

				buffer := bytes.NewBuffer(nil)

				hadError, err := index1.Verifier(buffer).
					WithNow(currentTime).
					WithStrictMetricCreation(true).
					WithStrictExpiration(true).
					Verify(t.Context())
				if err != nil {
					t.Error(err)
				}

				if hadError {
					t.Errorf("Verify had error, message=%s", buffer.String())
				}
			})
		}
	}
}

type metricLabelsExpiration struct {
	Expiration time.Time
	labels     labels.Labels
}

func clusterDoOneBatch(t *testing.T,
	currentTime time.Time,
	rnd *rand.Rand,
	defaultTTL time.Duration,
	oldMetricToWrite []labels.Labels,
	expectedMetrics map[string]metricLabelsExpiration,
	indexes []*CassandraIndex,
) ([]labels.Labels, map[string]metricLabelsExpiration) {
	t.Helper()

	const (
		retryCount    = 10
		metricsReuse  = 20
		metricsCommon = 10
		metricsNew    = 30
	)

	var rndLock sync.Mutex

	metricToWrite := make([]labels.Labels, 0, metricsReuse+metricsNew)

	if len(oldMetricToWrite) > 0 {
		indices := rnd.Perm(len(oldMetricToWrite))
		for count, idx := range indices {
			if count < len(oldMetricToWrite) {
				metricToWrite = append(metricToWrite, oldMetricToWrite[idx])
			}
		}
	}

	for n := range metricsNew {
		lbls := map[string]string{
			"__name__": "filler",
			"id":       strconv.FormatInt(int64(n), 10),
			"ts":       currentTime.Format("2006-01-02T15"),
		}

		if rnd.Float64() < 0.5 {
			lbls["random_present"] = "1"
		}

		if rnd.Float64() < 0.5 {
			lbls[fmt.Sprintf("lbl_name_%d", n)] = "name_is_randomized"
		}

		if rnd.Float64() < 0.5 {
			lbls[fmt.Sprintf("lbl2_name_%d", n)] = fmt.Sprintf("name_value_rnd_%d", rnd.Int63())
		}

		metricToWrite = append(metricToWrite, labels.FromMap(lbls))
	}

	commonMetrics := metricToWrite[:len(metricToWrite)/3]
	nodeSpecificMetrics := metricToWrite[len(commonMetrics):]

	group, ctx := errgroup.WithContext(t.Context())

	for workerID, index := range indexes {
		group.Go(func() error {
			fullList := append([]labels.Labels{}, commonMetrics...)

			perNodeCount := len(nodeSpecificMetrics) / len(indexes)
			if workerID == len(indexes)-1 {
				fullList = append(fullList, nodeSpecificMetrics[workerID*perNodeCount:]...)
			} else {
				fullList = append(fullList, nodeSpecificMetrics[workerID*perNodeCount:(workerID+1)*perNodeCount]...)
			}

			rndLock.Lock()

			rnd.Shuffle(len(fullList), func(i, j int) {
				fullList[i], fullList[j] = fullList[j], fullList[i]
			})

			rndLock.Unlock()

			var err error

			for i := range retryCount {
				if i == retryCount-1 {
					ctx = contextSkipError(ctx) //nolint: fatcontext
				}

				_, _, err = index.lookupIDs(ctx, toLookupRequests(fullList, currentTime), currentTime)
				if err == nil {
					break
				}
			}

			if err != nil {
				return fmt.Errorf("worker %d: %w", workerID, err)
			}

			index.InternalRunOnce(ctx, currentTime)

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		t.Fatalf("at time %s: %v", currentTime, err)
	}

	for _, lbls := range metricToWrite {
		lblsString := lbls.String()

		expectedMetrics[lblsString] = metricLabelsExpiration{
			Expiration: currentTime.Add(defaultTTL),
			labels:     lbls,
		}
	}

	return metricToWrite, expectedMetrics
}

func clusterCheckSearch(
	ctx context.Context,
	t *testing.T,
	now time.Time,
	index *CassandraIndex,
	realStore *mockStore,
) error {
	t.Helper()

	ctx = contextSkipError(ctx)

	matchers := make([]*labels.Matcher, 0, 2)
	matchers = append(matchers, labels.MustNewMatcher(
		labels.MatchEqual,
		"__name__",
		"filler",
	))
	matchers = append(matchers, labels.MustNewMatcher(
		labels.MatchEqual,
		"id",
		"1",
	))

	result, err := index.Search(ctx, now, now, matchers)
	if err != nil {
		return err
	}

	for result.Next() {
		item := result.At()

		if !matchers[0].Matches(item.Labels.Get("__name__")) {
			return fmt.Errorf("metric ID=%d: %s shouldn't be in Search()", item.ID, item.Labels)
		}

		if !matchers[1].Matches(item.Labels.Get("id")) {
			return fmt.Errorf("metric ID=%d: %s shouldn't be in Search()", item.ID, item.Labels)
		}

		realLabels := realStore.id2labels[item.ID]
		if !labels.Equal(realLabels, item.Labels) {
			return fmt.Errorf("labels = %s, want %s", item.Labels, realLabels)
		}
	}

	return nil
}

type concurentAccessTestExecution struct {
	now *mockTime

	pendingRequest int
	blockRequest   bool
	stopWorker     bool
	c              *sync.Cond
}

// Test_concurrent_access will test that concurrent access works.
// With the race-detector enabled, it might find data-races.
func Test_concurrent_access(t *testing.T) {
	defaultTTL := 10 * 24 * time.Hour

	for _, withCluster := range []bool{false, true} {
		name := "without-cluster"
		if withCluster {
			name = "with-cluster"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			realStore := &mockStore{}
			lock := &mockLockFactory{}
			states := &mockState{}

			var indexes []*CassandraIndex

			index1, err := initialize(
				t.Context(),
				realStore,
				Options{
					DefaultTimeToLive: defaultTTL,
					LockFactory:       lock,
					States:            states,
					Cluster:           &dummy.LocalCluster{},
				},
				newMetrics(prometheus.NewRegistry()),
				getTestLogger().With().Str("component", "index1").Logger(),
			)
			if err != nil {
				t.Fatal(err)
			}

			indexes = append(indexes, index1)

			if withCluster {
				index2, err := initialize(
					t.Context(),
					realStore,
					Options{
						DefaultTimeToLive: defaultTTL,
						LockFactory:       lock,
						States:            states,
						Cluster:           &dummy.LocalCluster{},
					},
					newMetrics(prometheus.NewRegistry()),
					getTestLogger().With().Str("component", "index2").Logger(),
				)
				if err != nil {
					t.Fatal(err)
				}

				indexes = append(indexes, index2)
			}

			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()

			ctxTestDuration, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			group, ctxTestDuration := errgroup.WithContext(ctxTestDuration)

			execution := &concurentAccessTestExecution{
				now: newMockTime(time.Now()),
				c:   sync.NewCond(&sync.Mutex{}),
			}

			go func() {
				for ctxTestDuration.Err() == nil {
					// Make time advance a bit
					execution.now.Add(time.Millisecond)
					time.Sleep(time.Microsecond)
				}

				execution.c.L.Lock()
				defer execution.c.L.Unlock()

				execution.stopWorker = true
			}()

			group.Go(func() error {
				rnd := rand.New(rand.NewSource(68464))

				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						idx := indexes[rnd.Intn(len(indexes))]
						idx.InternalRunOnce(ctx, execution.now.Now())
						// Advancing the time by one hour while request are in progress is like source of few bugs.
						// Block request before changing time.
						execution.c.L.Lock()
						execution.blockRequest = true

						for execution.pendingRequest > 0 {
							execution.c.Wait()
						}

						execution.blockRequest = false
						execution.c.Broadcast()
						execution.c.L.Unlock()

						execution.now.Add(time.Hour)
					case <-ctxTestDuration.Done():
						return nil
					}
				}
			})

			rnd := rand.New(rand.NewSource(123456789))

			for range 8 {
				rndSeed := rnd.Int63()

				group.Go(func() error {
					return concurrentAccessWriter(ctx, execution, indexes, rndSeed)
				})
			}

			for range 2 {
				rndSeed := rnd.Int63()

				group.Go(func() error {
					return concurrentAccessReader(ctx, execution, indexes, rndSeed)
				})
			}

			if err := group.Wait(); err != nil {
				t.Error(err)
			}

			tmp, err := indexes[0].expirationLastProcessedDay(t.Context())
			t.Logf("test now = %s, expirationLastProcessedDay=%v, %v", execution.now.Now(), tmp.Format(shardDateFormat), err)

			allIDs, err := indexes[0].AllIDs(t.Context(), indexMinValidTime, indexMaxValidTime)
			if err != nil {
				t.Error(err)
			}

			labels, err := indexes[0].lookupLabels(t.Context(), allIDs, execution.now.Now())
			if err != nil {
				t.Error(err)
			}

			for i, id := range allIDs {
				t.Logf("ID = %d is %s", id, labels[i])
			}

			for _, idx := range indexes {
				idx.InternalRunOnce(ctx, execution.now.Now())
			}

			for _, idx := range indexes {
				buffer := bytes.NewBuffer(nil)

				hadIssue, err := idx.Verifier(buffer).
					WithNow(execution.now.Now()).
					WithStrictMetricCreation(true).
					WithStrictExpiration(true).
					Verify(t.Context())
				if err != nil {
					t.Error(err)
				}

				if hadIssue {
					t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
				}
			}

			for range 400 * 2 {
				execution.now.Add(12 * time.Hour)
				indexes[0].InternalRunOnce(ctx, execution.now.Now())
			}

			tmp, err = indexes[0].expirationLastProcessedDay(t.Context())
			t.Logf("test now = %s, expirationLastProcessedDay=%v, %v", execution.now.Now(), tmp.Format(shardDateFormat), err)

			for _, idx := range indexes {
				buffer := bytes.NewBuffer(nil)

				hadIssue, err := idx.Verifier(buffer).
					WithNow(execution.now.Now()).
					WithStrictMetricCreation(true).
					WithPedanticExpiration(true).
					Verify(t.Context())
				if err != nil {
					t.Error(err)
				}

				if hadIssue {
					t.Errorf("Verify() had issues: %s", bufferToStringTruncated(buffer.Bytes()))
				}
			}
		})
	}
}

func concurrentAccessWriter(
	ctx context.Context, execution *concurentAccessTestExecution, indexes []*CassandraIndex, rndSeed int64,
) error {
	rnd := rand.New(rand.NewSource(rndSeed))

	fullList := []types.LookupRequest{
		{
			Start: execution.now.Now(),
			End:   execution.now.Now(),
			Labels: labels.FromMap(map[string]string{
				"__name__": "common",
				"item":     "low-ttl",
			}),
			TTLSeconds: int64((24 * 7 * time.Hour).Seconds()),
		},
		{
			Start: execution.now.Now(),
			End:   execution.now.Now(),
			Labels: labels.FromMap(map[string]string{
				"__name__": "common",
				"item":     "medium-ttl",
			}),
			TTLSeconds: int64((24 * 90 * time.Hour).Seconds()),
		},
		{
			Start: execution.now.Now(),
			End:   execution.now.Now(),
			Labels: labels.FromMap(map[string]string{
				"__name__": "common",
				"item":     "high-ttl",
			}),
			TTLSeconds: int64((24 * 365 * time.Hour).Seconds()),
		},
		{
			Start: execution.now.Now(),
			End:   execution.now.Now(),
			Labels: labels.FromMap(map[string]string{
				"__name__": "not_common",
				"item":     "low-ttl",
				"unique":   strconv.FormatInt(rnd.Int63(), 10),
			}),
			TTLSeconds: int64((24 * 7 * time.Hour).Seconds()),
		},
		{
			Start: execution.now.Now(),
			End:   execution.now.Now(),
			Labels: labels.FromMap(map[string]string{
				"__name__": "not_common",
				"item":     "medium-ttl",
				"unique":   strconv.FormatInt(rnd.Int63(), 10),
			}),
			TTLSeconds: int64((24 * 90 * time.Hour).Seconds()),
		},
		{
			Start: execution.now.Now(),
			End:   execution.now.Now(),
			Labels: labels.FromMap(map[string]string{
				"__name__": "not_common",
				"item":     "high-ttl",
				"unique":   strconv.FormatInt(rnd.Int63(), 10),
			}),
			TTLSeconds: int64((24 * 365 * time.Hour).Seconds()),
		},
	}

	for {
		execution.c.L.Lock()
		if execution.stopWorker {
			execution.c.L.Unlock()

			break
		}

		for execution.blockRequest {
			execution.c.Wait()
		}

		execution.pendingRequest++

		execution.c.L.Unlock()

		nowValue := execution.now.Now()
		for i := range fullList {
			fullList[i].Start = nowValue
			fullList[i].End = nowValue
		}

		idxID := rnd.Intn(len(indexes))
		idx := indexes[idxID]

		_, _, err := idx.lookupIDs(ctx, fullList, nowValue)

		execution.c.L.Lock()

		execution.pendingRequest--
		execution.c.Broadcast()

		execution.c.L.Unlock()

		if err != nil {
			return fmt.Errorf("concurrentAccessWriter idxID=%d: %w", idxID+1, err)
		}
	}

	return nil
}

func concurrentAccessReader(
	ctx context.Context, execution *concurentAccessTestExecution, indexes []*CassandraIndex, rndSeed int64,
) error {
	rnd := rand.New(rand.NewSource(rndSeed))

	for {
		execution.c.L.Lock()
		if execution.stopWorker {
			execution.c.L.Unlock()

			break
		}

		for execution.blockRequest {
			execution.c.Wait()
		}

		execution.pendingRequest++

		execution.c.L.Unlock()

		idxID := rnd.Intn(len(indexes))
		idx := indexes[idxID]

		var matchers []*labels.Matcher

		switch rnd.Intn(4) {
		case 0:
			matchers = []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "common"),
			}
		case 1:
			matchers = []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "item", "(medium-ttl|low-ttl)"),
			}
		case 2:
			matchers = []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "unique", ""),
				{Type: labels.MatchNotEqual, Name: "unique", Value: ""},
			}
		case 3:
			matchers = []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "unique", strconv.FormatInt(rnd.Int63(), 10)),
			}
		}

		_, err := idx.Search(ctx, execution.now.Now().Add(-14*24*time.Hour), execution.now.Now(), matchers)
		execution.c.L.Lock()

		execution.pendingRequest--
		execution.c.Broadcast()

		execution.c.L.Unlock()

		if err != nil {
			return fmt.Errorf("concurrentAccessReader idxID=%d: %w", idxID+1, err)
		}
	}

	return nil
}

// TestLookForOldData tends to reproduce a behavior which can be described as:
//   - Cassandra exists with a metric that has old points (at least 2 weeks ago)
//   - At SquirrelDB start, we write points for the same metrics but at current timestamp
//   - Then, within the first minute of SquirrelDB's existence,
//     we try to look the points we talked about in the first step.
func TestLookForOldData(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	oldMetricDate := now.Add(-30 * 24 * time.Hour) // a few shards ago
	existingMetrics := map[types.MetricID]map[string]string{
		MetricIDTest1: {
			"__name__": "cpu_used",
			"instance": "localhost:8015",
		},
	}
	existingIndex := mockIndexFromMetrics(t, oldMetricDate, oldMetricDate, existingMetrics)

	// Creating a new index with an empty cache, as when SquirrelDB (re)starts.
	// The existing store is persisted, as Cassandra does.
	index, err := initialize(
		ctx,
		existingIndex.store,
		Options{
			DefaultTimeToLive: 365 * 24 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
			States:            &mockState{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	reqTime := now.Truncate(10 * time.Second)
	writeReqs := []types.LookupRequest{
		{
			Start: reqTime,
			End:   reqTime,
			Labels: labels.Labels{
				{
					Name:  "__name__",
					Value: "cpu_used",
				},
				{
					Name:  "instance",
					Value: "localhost:8015",
				},
			},
			TTLSeconds: 0,
		},
	}

	// Simulating a writing request in the current shard
	_, _, err = index.lookupIDs(ctx, writeReqs, now)
	if err != nil {
		t.Fatal(err)
	}

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu_used"),
	}
	// Now looking for old points, like the one we wrote earlier ...
	metricSet, err := index.Search(ctx, oldMetricDate, oldMetricDate.Add(10*time.Minute), matchers)
	if err != nil {
		t.Fatal(err)
	}

	if count := metricSet.Count(); count != 1 {
		t.Fatalf("Expected index to return 1 metric, got %d", count)
	}
}

// TestBadInitilizationOfIdInShard check a "bug" where SquirrelDB perform
// unnecessary update due to idInShard cache being initilized with wrong (empty) value.
// Step:
//   - SquirrelDB write (current) data in Cassandra
//   - SquirrelDB is re-started, and write the same metrics. Index shouldn't need any
//     update (we write in the same shard), but the bug caused update of the index.
func TestBadInitilizationOfIdInShard(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2024, 9, 16, 11, 1, 0, 0, time.UTC)
	past := now.Add(-1 * time.Minute)

	store := &mockStore{}

	index, err := initialize(
		t.Context(),
		store,
		Options{
			DefaultTimeToLive: 1 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	writeReqs := []types.LookupRequest{
		{
			Start: past,
			End:   past,
			Labels: labels.Labels{
				{
					Name:  "__name__",
					Value: "cpu_used",
				},
				{
					Name:  "instance",
					Value: "localhost:8015",
				},
			},
			TTLSeconds: 0,
		},
	}

	// Simulating a writing request in the current shard
	_, _, err = index.lookupIDs(ctx, writeReqs, past)
	if err != nil {
		t.Fatal(err)
	}

	writeBeforeStop := store.writeQueryCount

	// Creating a new index with an empty cache, as when SquirrelDB (re)starts.
	// The existing store is persisted, as Cassandra does.
	index, err = initialize(
		ctx,
		store,
		Options{
			DefaultTimeToLive: 1 * time.Hour,
			LockFactory:       &mockLockFactory{},
			Cluster:           &dummy.LocalCluster{},
		},
		newMetrics(prometheus.NewRegistry()),
		getTestLogger().With().Str("component", "index").Logger(),
	)
	if err != nil {
		t.Fatal(err)
	}

	writeReqs = []types.LookupRequest{
		{
			Start: now,
			End:   now,
			Labels: labels.Labels{
				{
					Name:  "__name__",
					Value: "cpu_used",
				},
				{
					Name:  "instance",
					Value: "localhost:8015",
				},
			},
			TTLSeconds: 0,
		},
	}

	// Same lookup as previous one, but 1 minute later
	_, _, err = index.lookupIDs(ctx, writeReqs, now)
	if err != nil {
		t.Fatal(err)
	}

	writeAfterRestart := store.writeQueryCount

	if writeBeforeStop != writeAfterRestart {
		t.Fatalf("writeAfterRestart = %d, want %d", writeAfterRestart, writeBeforeStop)
	}
}

// TestStableLabelsToText checks that Prometheus Labels to String() is stable across versions.
func TestStableLabelsToText(t *testing.T) {
	cases := []struct {
		lbls labels.Labels
		want string
	}{
		{
			lbls: labels.FromMap(map[string]string{
				"__name__": "cpu_used",
			}),
			want: "{__name__=\"cpu_used\"}",
		},
		{
			lbls: labels.FromMap(map[string]string{
				"__name__": "cpu_utilisé",
			}),
			want: "{__name__=\"cpu_utilisé\"}",
		},
		{
			lbls: labels.FromMap(map[string]string{
				"__name__": "utf8_values",
				"key":      "value with unicode ☃︎ éß是 and quote \"'`", //nolint: gosmopolitan
			}),
			want: "{__name__=\"utf8_values\", key=\"value with unicode ☃︎ éß是 and quote \\\"'`\"}", //nolint: gosmopolitan
		},
		{
			lbls: labels.FromMap(map[string]string{
				"__name__": "utf8_key",
				"key with unicode ☃︎ éß是 and quote \"'`": "value", //nolint: gosmopolitan
			}),
			want: "{__name__=\"utf8_key\", \"key with unicode ☃︎ éß是 and quote \\\"'`\"=\"value\"}", //nolint: gosmopolitan
		},
		{
			lbls: labels.FromMap(map[string]string{
				"__name__": "conflict1?",
				"key":      "value",
			}),
			want: "{__name__=\"conflict1?\", key=\"value\"}",
		},
		{
			lbls: labels.FromMap(map[string]string{
				"__name__": "conflict3?",
				"key1é":    "value",
				"key2é":    "value",
			}),
			want: "{__name__=\"conflict3?\", \"key1é\"=\"value\", \"key2é\"=\"value\"}",
		},
	}

	for _, tt := range cases {
		got := tt.lbls.String()
		if diff := cmp.Diff(tt.want, got); diff != "" {
			t.Errorf("lbls.String() mismatch (-want +got)\n%s", diff)
		}
	}
}

// TestNoLabelsToTextConflict check that Prometheus Labels to String() can't produce conflict.
// i.e. two different Labels don't have the same String() value.
func TestNoLabelsToTextConflict(t *testing.T) {
	cases := []struct {
		lblsA labels.Labels
		lblsB labels.Labels
	}{
		{
			lblsA: labels.FromMap(map[string]string{
				"__name__": "conflict1?",
				"key":      "value",
			}),
			lblsB: labels.FromMap(map[string]string{
				"__name__": "conflict1?",
				"\"key\"":  "value",
			}),
		},
		{
			lblsA: labels.FromMap(map[string]string{
				"__name__": "conflict2?",
				"keyé":     "value",
			}),
			lblsB: labels.FromMap(map[string]string{
				"__name__": "conflict2?",
				"\"keyé\"": "value",
			}),
		},
		{
			lblsA: labels.FromMap(map[string]string{
				"__name__": "conflict3?",
				"key1é":    "value",
				"key2é":    "value",
			}),
			lblsB: labels.FromMap(map[string]string{
				"__name__":                   "conflict3?",
				"key1é\"=\"value\", \"key2é": "value",
			}),
		},
	}

	for _, tt := range cases {
		gotA := tt.lblsA.String()
		gotB := tt.lblsB.String()

		if labels.Equal(tt.lblsA, tt.lblsB) {
			t.Errorf("Test itself is wrong, lblsA & lblsA are equal. lblsA = %s", tt.lblsA)
		}

		if gotA == gotB {
			t.Errorf("both lbl.String() are equal with lblsA = %s", tt.lblsA)
		}
	}
}
