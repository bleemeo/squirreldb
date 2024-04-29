package index

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/prometheus/model/labels"
)

type randomShouldFail struct {
	l          sync.Mutex
	rateBefore float64
	rateAfter  float64
	err        error
	rnd        *rand.Rand
}

type contextKey string

func contextSkipError(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKey("skipError"), "1")
}

func shouldSkipError(ctx context.Context) bool {
	return ctx.Value(contextKey("skipError")) == "1"
}

func newRandomShouldFail(seed int64, err error) *randomShouldFail {
	return &randomShouldFail{
		err: err,
		rnd: rand.New(rand.NewSource(seed)),
	}
}

func (r *randomShouldFail) SetRate(rateBefore float64, rateAfter float64) {
	r.l.Lock()
	defer r.l.Unlock()

	r.rateBefore = rateBefore
	r.rateAfter = rateAfter
}

func (r *randomShouldFail) ShouldFailBefore(_ context.Context) error {
	r.l.Lock()
	defer r.l.Unlock()

	if r.rnd.Float64() < r.rateBefore {
		return r.err
	}

	return nil
}

func (r *randomShouldFail) ShouldFailAfter(_ context.Context) error {
	r.l.Lock()
	defer r.l.Unlock()

	if r.rnd.Float64() < r.rateAfter {
		return r.err
	}

	return nil
}

type dontFail struct{}

func (dontFail) ShouldFailBefore(_ context.Context) error {
	return nil
}

func (dontFail) ShouldFailAfter(_ context.Context) error {
	return nil
}

type shouldFail interface {
	ShouldFailBefore(ctx context.Context) error
	ShouldFailAfter(ctx context.Context) error
}

type failingStore struct {
	l          sync.Mutex
	realStore  storeImpl
	shouldFail shouldFail
}

func newFailingStore(realStore storeImpl, shouldFail shouldFail) *failingStore {
	s := &failingStore{
		realStore:  realStore,
		shouldFail: dontFail{},
	}

	s.SetShouldFail(shouldFail)

	return s
}

func (s *failingStore) SetShouldFail(obj shouldFail) {
	s.l.Lock()
	defer s.l.Unlock()

	s.shouldFail = obj

	if s.shouldFail == nil {
		s.shouldFail = dontFail{}
	}
}

func (s *failingStore) callShouldFailBefore(ctx context.Context, _ string) error {
	s.l.Lock()
	defer s.l.Unlock()

	if shouldSkipError(ctx) {
		return nil
	}

	return s.shouldFail.ShouldFailBefore(ctx)
}

func (s *failingStore) callShouldFailAfter(ctx context.Context, _ string, realErr error) error {
	s.l.Lock()
	defer s.l.Unlock()

	if shouldSkipError(ctx) {
		return realErr
	}

	err := s.shouldFail.ShouldFailAfter(ctx)
	if err != nil {
		return err
	}

	return realErr
}

func (s *failingStore) Init(ctx context.Context) error {
	if err := s.callShouldFailBefore(ctx, "Init"); err != nil {
		return err
	}

	err := s.realStore.Init(ctx)

	return s.callShouldFailAfter(ctx, "Init", err)
}

func (s *failingStore) SelectLabelsList2ID(
	ctx context.Context,
	sortedLabelsListString []string,
) (map[string]types.MetricID, error) {
	if err := s.callShouldFailBefore(ctx, "SelectLabelsList2ID"); err != nil {
		return nil, err
	}

	r1, err := s.realStore.SelectLabelsList2ID(ctx, sortedLabelsListString)

	return r1, s.callShouldFailAfter(ctx, "SelectLabelsList2ID", err)
}

func (s *failingStore) SelectIDS2LabelsAndExpiration(
	ctx context.Context,
	id []types.MetricID,
) (map[types.MetricID]labels.Labels, map[types.MetricID]time.Time, error) {
	if err := s.callShouldFailBefore(ctx, "SelectIDS2LabelsAndExpiration"); err != nil {
		return nil, nil, err
	}

	r1, r2, err := s.realStore.SelectIDS2LabelsAndExpiration(ctx, id)

	return r1, r2, s.callShouldFailAfter(ctx, "SelectIDS2LabelsAndExpiration", err)
}

func (s *failingStore) SelectExpiration(ctx context.Context, day time.Time) ([]byte, error) {
	if err := s.callShouldFailBefore(ctx, "SelectExpiration"); err != nil {
		return nil, err
	}

	r1, err := s.realStore.SelectExpiration(ctx, day)

	return r1, s.callShouldFailAfter(ctx, "SelectExpiration", err)
}

type errPostingIter struct {
	err error
}

func (it errPostingIter) HasNext() bool {
	return false
}

func (it errPostingIter) Next() (string, []byte) {
	panic("Next() shouldn't be called, HasNext() returned false")
}

func (it errPostingIter) Err() error {
	return it.err
}

func (it errPostingIter) Close() {
}

// SelectPostingByName return  label value with the associated postings in sorted order.
func (s *failingStore) SelectPostingByName(ctx context.Context, shard int32, name string) postingIter {
	if err := s.callShouldFailBefore(ctx, "SelectPostingByName"); err != nil {
		return &errPostingIter{err: err}
	}

	r1 := s.realStore.SelectPostingByName(ctx, shard, name)
	defer r1.Close()

	err := s.callShouldFailAfter(ctx, "SelectPostingByName", r1.Err())
	if err != nil {
		return &errPostingIter{err: err}
	}

	return r1
}

func (s *failingStore) SelectPostingByNameValue(
	ctx context.Context,
	shard int32,
	name string,
	value string,
) ([]byte, error) {
	if err := s.callShouldFailBefore(ctx, "SelectPostingByNameValue"); err != nil {
		return nil, err
	}

	r1, err := s.realStore.SelectPostingByNameValue(ctx, shard, name, value)

	return r1, s.callShouldFailAfter(ctx, "SelectPostingByNameValue", err)
}

func (s *failingStore) SelectValueForName(ctx context.Context, shard int32, name string) ([]string, [][]byte, error) {
	if err := s.callShouldFailBefore(ctx, "SelectValueForName"); err != nil {
		return nil, nil, err
	}

	r1, r2, err := s.realStore.SelectValueForName(ctx, shard, name)

	return r1, r2, s.callShouldFailAfter(ctx, "SelectValueForName", err)
}

func (s *failingStore) InsertPostings(
	ctx context.Context,
	shard int32,
	name string,
	value string,
	bitset []byte,
) error {
	if err := s.callShouldFailBefore(ctx, "InsertPostings"); err != nil {
		return err
	}

	err := s.realStore.InsertPostings(ctx, shard, name, value, bitset)

	return s.callShouldFailAfter(ctx, "InsertPostings", err)
}

func (s *failingStore) InsertID2Labels(
	ctx context.Context,
	id types.MetricID,
	sortedLabels labels.Labels,
	expiration time.Time,
) error {
	if err := s.callShouldFailBefore(ctx, "InsertID2Labels"); err != nil {
		return err
	}

	err := s.realStore.InsertID2Labels(ctx, id, sortedLabels, expiration)

	return s.callShouldFailAfter(ctx, "InsertID2Labels", err)
}

func (s *failingStore) InsertLabels2ID(ctx context.Context, sortedLabelsString string, id types.MetricID) error {
	if err := s.callShouldFailBefore(ctx, "InsertLabels2ID"); err != nil {
		return err
	}

	err := s.realStore.InsertLabels2ID(ctx, sortedLabelsString, id)

	return s.callShouldFailAfter(ctx, "InsertLabels2ID", err)
}

func (s *failingStore) InsertExpiration(ctx context.Context, day time.Time, bitset []byte) error {
	if err := s.callShouldFailBefore(ctx, "InsertExpiration"); err != nil {
		return err
	}

	err := s.realStore.InsertExpiration(ctx, day, bitset)

	return s.callShouldFailAfter(ctx, "InsertExpiration", err)
}

func (s *failingStore) UpdateID2LabelsExpiration(ctx context.Context, id types.MetricID, expiration time.Time) error {
	if err := s.callShouldFailBefore(ctx, "UpdateID2LabelsExpiration"); err != nil {
		return err
	}

	err := s.realStore.UpdateID2LabelsExpiration(ctx, id, expiration)

	return s.callShouldFailAfter(ctx, "UpdateID2LabelsExpiration", err)
}

func (s *failingStore) DeleteLabels2ID(ctx context.Context, sortedLabelsString string) error {
	if err := s.callShouldFailBefore(ctx, "DeleteLabels2ID"); err != nil {
		return err
	}

	err := s.realStore.DeleteLabels2ID(ctx, sortedLabelsString)

	return s.callShouldFailAfter(ctx, "DeleteLabels2ID", err)
}

func (s *failingStore) DeleteID2Labels(ctx context.Context, id types.MetricID) error {
	if err := s.callShouldFailBefore(ctx, "DeleteID2Labels"); err != nil {
		return err
	}

	err := s.realStore.DeleteID2Labels(ctx, id)

	return s.callShouldFailAfter(ctx, "DeleteID2Labels", err)
}

func (s *failingStore) DeleteExpiration(ctx context.Context, day time.Time) error {
	if err := s.callShouldFailBefore(ctx, "DeleteExpiration"); err != nil {
		return err
	}

	err := s.realStore.DeleteExpiration(ctx, day)

	return s.callShouldFailAfter(ctx, "DeleteExpiration", err)
}

func (s *failingStore) DeletePostings(ctx context.Context, shard int32, name string, value string) error {
	if err := s.callShouldFailBefore(ctx, "DeletePostings"); err != nil {
		return err
	}

	err := s.realStore.DeletePostings(ctx, shard, name, value)

	return s.callShouldFailAfter(ctx, "DeletePostings", err)
}

func (s *failingStore) DeletePostingsByNames(ctx context.Context, shard int32, names []string) error {
	if err := s.callShouldFailBefore(ctx, "DeletePostingsByNames"); err != nil {
		return err
	}

	err := s.realStore.DeletePostingsByNames(ctx, shard, names)

	return s.callShouldFailAfter(ctx, "DeletePostingsByNames", err)
}
