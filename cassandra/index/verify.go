package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"squirreldb/types"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/prometheus/model/labels"
)

type verifier struct {
	index                *CassandraIndex
	doFix                bool
	acquireLock          bool
	strictMetricCreation bool
	strictExpiration     bool
	pedanticExpiration   bool
	output               io.Writer
	now                  time.Time
}

type verifierExecution struct {
	verifier
	bulkDeleter *deleter
	// allPosting is the content of the special posting globalAllPostingLabel
	allPosting *roaring.Bitmap
	// allGoodIDs will contains all MetricIDs which have correct id <-> labels entry in
	// tables labels2id and id2labels.
	allGoodIDs *roaring.Bitmap
	// expectedExpiration contains the expected value for index_expiration table (based on id2labels table)
	expectedExpiration map[int64]*roaring.Bitmap
	// expectedUnknowDate contains the metric ID which should expire but we ignore when.
	expectedUnknowDate *roaring.Bitmap
}

const maxIDBeforeTruncate = 50

func (c *CassandraIndex) Verifier(output io.Writer) types.IndexVerifier {
	return verifier{
		index:  c,
		now:    time.Now(),
		output: output,
	}
}

// WithNow fixes the time the verify process think we are curretly. This is useful for test AND
// when running verify on a dump that is few day old.
func (v verifier) WithNow(now time.Time) types.IndexVerifier {
	v.now = now

	return v
}

// WithDoFix enable or disable fix for correctable error found. It imply taking the newMetricGlobalLock lock.
func (v verifier) WithDoFix(enable bool) types.IndexVerifier {
	if enable {
		v.acquireLock = true
	}

	v.doFix = enable

	return v
}

// WithLock enable or disable taking lock during index verification.
// The lock is required as a metric creation / expiration during the verification process could
// return a false-positive, but holding the lock will block metric creation during the whole verification process.
// If disabled, it also disable DoFix.
func (v verifier) WithLock(enable bool) types.IndexVerifier {
	v.acquireLock = enable

	if !enable {
		v.doFix = false
	}

	return v
}

// WithStrictExpiration enable or disable fail on warning condition for expiration.
// By default, it's only checked that expiration don't have fatal error (which would cause metric
// to don't expire at all for example). With this option, it also check that expiration information
// are up-to-date but still allow for duplicate. For this check to works, task like cassandraExpire()
// and applyExpirationUpdateRequests() must have run.
// To even disallow duplicate, with WithPedanticExpiration
// It modify the verifier and return it to allow chaining call.
func (v verifier) WithStrictExpiration(enable bool) types.IndexVerifier {
	v.strictExpiration = enable

	if !enable {
		v.pedanticExpiration = false
	}

	return v
}

// WithStrictMetricCreation enable or disable fail on warning condition for metric creation.
// The way metric creation is done, could lead to partially creation metric, that will:
// * not be used
// * be cleaned on their expiration
// So such error don't have significant impact unless they happen way to often. They should
// only happen if during metric creation something goes wrong (like SquirrelDB crash or just
// the HTTP client that disconnect during the creation - i.e. timeout).
// It modify the verifier and return it to allow chaining call.
func (v verifier) WithStrictMetricCreation(enable bool) types.IndexVerifier {
	v.strictMetricCreation = enable

	return v
}

// WithPedanticExpiration enable or disable all check on expiration.
// It imply WithStrictExpiration and also enable checking the ID aren't duplicated in previous day.
// This check is advanced as it can't be easily avoided with a cluster:
//   - Two SquirrelDB have knowledge of the same metric
//   - SquirrelDB A created the metric, with expiration day D1
//   - A write on SquirrelDB B cause the TTL to update from D1 to D2
//   - Later, a write on SquirrelDB A cause another update. SquirrelDB A thing that update is from D1 to D3
//   - SquirrelDB B restart (or refresh the expiration cache)
//   - The metric is present in D2 and D3. No SquirrelDBs think that it's present in D2 and will not remove
//     it from there (excepted when that day is processed by the expiration process).
//
// It modify the verifier and return it to allow chaining call.
func (v verifier) WithPedanticExpiration(enable bool) types.IndexVerifier {
	v.pedanticExpiration = enable

	if enable {
		v.strictExpiration = true
	}

	return v
}

// Verify perform some checks on index consistency.
// It could apply fixes and could acquire the newMetricGlobalLock to ensure a consitent read of the index.
// When any issue is found, the hadIssue will be true (and description of the issue is written to w). Note
// that some issue might get fixed automatically (e.g. expired metrics that are not yet processed, partial write should
// be fixed when expiration is applied for them, ...).
func (v verifier) Verify(ctx context.Context) (hadIssue bool, err error) {
	execution := &verifierExecution{
		verifier:           v,
		bulkDeleter:        newBulkDeleter(v.index),
		expectedExpiration: make(map[int64]*roaring.Bitmap),
		expectedUnknowDate: roaring.NewBTreeBitmap(),
	}

	return execution.verify(ctx)
}

func (ve *verifierExecution) verify(ctx context.Context) (hadIssue bool, err error) {
	if ve.acquireLock {
		ve.index.newMetricGlobalLock.Lock()
		defer ve.index.newMetricGlobalLock.Unlock()
	}

	issueCount, shards, err := ve.verifyMissingShard(ctx)
	if err != nil {
		return hadIssue, err
	}

	hadIssue = hadIssue || issueCount > 0

	ve.allGoodIDs = roaring.NewBTreeBitmap()

	ve.allPosting, err = ve.index.postings(
		ctx,
		[]int32{globalShardNumber},
		globalAllPostingLabel,
		globalAllPostingLabel,
		false,
	)
	if err != nil {
		return hadIssue, err
	}

	fmt.Fprintf(ve.output, "Index had %d shards and should have %d metrics\n", shards.Count(), ve.allPosting.Count())

	if respf, ok := ve.output.(http.Flusher); ok {
		respf.Flush()
	}

	count := 0
	it := ve.allPosting.Iterator()

	pendingIDs := make([]types.MetricID, 0, 10000)

	for ctx.Err() == nil {
		pendingIDs = pendingIDs[:0]

		for ctx.Err() == nil {
			id, eof := it.Next()
			if eof {
				break
			}

			metricID := types.MetricID(id)

			count++

			pendingIDs = append(pendingIDs, metricID)

			if len(pendingIDs) > verifyBulkSize {
				break
			}
		}

		if len(pendingIDs) == 0 {
			break
		}

		if len(pendingIDs) > 0 {
			bulkHadIssue, err := ve.verifyBulk(ctx, pendingIDs)
			if err != nil {
				return hadIssue, err
			}

			hadIssue = hadIssue || bulkHadIssue
		}
	}

	fmt.Fprintf(ve.output, "Index contains %d metrics and %d ok\n", count, ve.allGoodIDs.Count())

	if respf, ok := ve.output.(http.Flusher); ok {
		respf.Flush()
	}

	if ve.doFix {
		fmt.Fprintf(ve.output, "Applying fix...")

		if err := ve.bulkDeleter.Delete(ctx); err != nil {
			return hadIssue, err
		}
	}

	for _, shard := range shards.Slice() {
		shard := int32(shard)
		fmt.Fprintf(ve.output, "Checking shard %s (ID %d)\n", timeForShard(shard).Format(shardDateFormat), shard)

		shardHadIssue, err := ve.verifyShard(ctx, shard)
		if err != nil {
			return hadIssue, err
		}

		hadIssue = hadIssue || shardHadIssue

		if respf, ok := ve.output.(http.Flusher); ok {
			respf.Flush()
		}
	}

	expirationIssue, err := ve.verifyExpiration(ctx)
	if err != nil {
		return hadIssue, err
	}

	hadIssue = hadIssue || expirationIssue

	return hadIssue, ctx.Err()
}

// verifyMissingShard search from now+3 weeks to 100 weeks before now+3 weeks for shard not present in existingShards.
// It also verify that all shards in existingShards actually exists.
func (ve *verifierExecution) verifyMissingShard(
	ctx context.Context) (errorCount int, shards *roaring.Bitmap, err error,
) {
	shards, err = ve.index.postings(ctx, []int32{globalShardNumber}, existingShardsLabel, existingShardsLabel, false)
	if err != nil {
		return 0, shards, fmt.Errorf("get postings for existing shards: %w", err)
	}

	current := time.Now().Add(3 * postingShardSize)

	for n := 0; n < 100; n++ {
		if ctx.Err() != nil {
			return 0, shards, ctx.Err()
		}

		queryShard := []int32{shardForTime(current.Unix())}

		for _, name := range []string{maybePostingLabel, allPostingLabel} {
			it, err := ve.index.postings(ctx, queryShard, name, name, false)
			if err != nil {
				return 0, shards, err
			}

			if it != nil && it.Any() && !shards.Contains(uint64(queryShard[0])) {
				errorCount++

				fmt.Fprintf(
					ve.output,
					"Shard %s for time %v isn't in all shards",
					timeForShard(queryShard[0]).Format(shardDateFormat),
					current.String(),
				)

				if ve.doFix {
					_, err = shards.AddN(uint64(queryShard[0]))
					if err != nil {
						return 0, shards, fmt.Errorf("update bitmap: %w", err)
					}
				}
			}
		}

		current = current.Add(-postingShardSize)
	}

	slice := shards.Slice()
	for _, shard := range slice {
		if ctx.Err() != nil {
			return 0, shards, ctx.Err()
		}

		shard := int32(shard)

		it, err := ve.index.postings(ctx, []int32{shard}, maybePostingLabel, maybePostingLabel, false)
		if err != nil {
			return 0, shards, fmt.Errorf("get postings for maybe metric IDs: %w", err)
		}

		if it == nil || !it.Any() {
			errorCount++

			fmt.Fprintf(
				ve.output,
				"Shard %s is listed in all shards but don't exists\n",
				timeForShard(shard).Format(shardDateFormat),
			)

			if ve.doFix {
				_, err = shards.RemoveN(uint64(shard))
				if err != nil {
					return 0, shards, fmt.Errorf("update bitmap: %w", err)
				}
			}
		}
	}

	if errorCount > 0 && ve.doFix {
		var buffer bytes.Buffer

		_, err = shards.WriteTo(&buffer)
		if err != nil {
			return errorCount, shards, fmt.Errorf("serialize bitmap: %w", err)
		}

		err = ve.index.store.InsertPostings(ctx, globalShardNumber, existingShardsLabel, existingShardsLabel, buffer.Bytes())
		if err != nil {
			return errorCount, shards, fmt.Errorf("update existing shards: %w", err)
		}
	}

	return errorCount, shards, nil
}

// check that given metric IDs existing in labels2id and id2labels.
func (ve *verifierExecution) verifyBulk( //nolint:maintidx
	ctx context.Context,
	ids []types.MetricID,
) (hadIssue bool, err error) {
	id2Labels, id2expiration, err := ve.index.selectIDS2LabelsAndExpiration(ctx, ids)
	if err != nil {
		return hadIssue, fmt.Errorf("get labels: %w", err)
	}

	allLabelsString := make([]string, 0, len(ids))

	for _, id := range ids {
		lbls, ok := id2Labels[id]
		if !ok && ve.strictMetricCreation {
			fmt.Fprintf(ve.output, "ID %10d does not exists in ID2Labels, partial write ?\n", id)

			hadIssue = true

			if ve.doFix {
				ve.bulkDeleter.PrepareDelete(id, nil, false)
			}
		}

		if !ok {
			if _, err := ve.expectedUnknowDate.AddN(uint64(id)); err != nil {
				return false, err
			}

			continue
		}

		allLabelsString = append(allLabelsString, lbls.String())

		_, ok = id2expiration[id]
		if !ok {
			fmt.Fprintf(
				ve.output,
				"ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify\n",
				id,
				lbls.String(),
			)

			hadIssue = true
		}

		if !ok {
			if _, err := ve.expectedUnknowDate.AddN(uint64(id)); err != nil {
				return false, err
			}

			continue
		}
	}

	if ctx.Err() != nil {
		return hadIssue, ctx.Err()
	}

	labels2ID, err := ve.index.selectLabelsList2ID(ctx, allLabelsString)
	if err != nil {
		return hadIssue, fmt.Errorf("get labels2ID: %w", err)
	}

	for _, id := range ids {
		if ctx.Err() != nil {
			return hadIssue, ctx.Err()
		}

		lbls, ok := id2Labels[id]
		if !ok {
			continue
		}

		expiration, ok := id2expiration[id]
		if !ok {
			continue
		}

		expirationDay := expiration.Truncate(24 * time.Hour)

		it := ve.expectedExpiration[expirationDay.Unix()]
		if it == nil {
			it = roaring.NewBTreeBitmap()
		}

		if _, err := it.AddN(uint64(id)); err != nil {
			return false, err
		}

		ve.expectedExpiration[expirationDay.Unix()] = it

		id2, ok := labels2ID[lbls.String()]
		if !ok && ve.strictMetricCreation {
			fmt.Fprintf(ve.output, "ID %10d (%v) not found in Labels2ID, partial write ?\n", id, lbls.String())

			hadIssue = true

			if ve.doFix {
				ve.bulkDeleter.PrepareDelete(id, lbls, false)
			}
		}

		if !ok {
			continue
		}

		if id != id2 { //nolint:nestif
			tmp, tmp2, err := ve.index.selectIDS2LabelsAndExpiration(ctx, []types.MetricID{id2})
			if err != nil {
				return hadIssue, fmt.Errorf("get labels from store: %w", err)
			}

			lbls2 := tmp[id2]
			expiration2, ok := tmp2[id2]

			if !ok && lbls2 != nil {
				fmt.Fprintf(
					ve.output,
					"ID %10d (%v) found in ID2labels but not for expiration! You may need to took the lock to verify",
					id2,
					lbls2.String(),
				)

				hadIssue = true

				continue
			}

			switch {
			case lbls2 == nil:
				fmt.Fprintf(
					ve.output,
					"ID %10d (%v) conflict with ID %d (which is a partial write! THIS SHOULD NOT HAPPEN.)\n",
					id,
					lbls.String(),
					id2,
				)

				hadIssue = true

				if ve.doFix {
					// well, the only solution is to delete *both* ID.
					ve.bulkDeleter.PrepareDelete(id2, lbls, false)
					ve.bulkDeleter.PrepareDelete(id, lbls, false)
				}
			case !ve.allPosting.Contains(uint64(id2)):
				fmt.Fprintf(
					ve.output,
					"ID %10d (%v) conflict with ID %d (which isn't listed in all posting! THIS SHOULD NOT HAPPEN.)\n",
					id,
					lbls.String(),
					id2,
				)

				hadIssue = true

				if ve.doFix {
					// well, the only solution is to delete *both* ID.
					ve.bulkDeleter.PrepareDelete(id2, lbls2, false)
					ve.bulkDeleter.PrepareDelete(id, lbls, false)
				}
			case ve.strictMetricCreation:
				fmt.Fprintf(
					ve.output,
					"ID %10d (%v) conflict with ID %d (%v). first expire at %v, second at %v\n",
					id,
					lbls.String(),
					id2,
					lbls2.String(),
					expiration,
					expiration2,
				)

				hadIssue = true

				// Assume that metric2 is better. It has id2labels, labels2id and in all postings
				if ve.doFix {
					ve.bulkDeleter.PrepareDelete(id, lbls, true)
				}
			}

			continue
		}

		if ve.now.After(expiration.Add(24 * time.Hour)) {
			fmt.Fprintf(ve.output, "ID %10d (%v) should have expired on %v\n", id, lbls.String(), expiration)

			hadIssue = true

			if ve.doFix {
				ve.bulkDeleter.PrepareDelete(id, lbls, false)
			}

			continue
		}

		_, err = ve.allGoodIDs.AddN(uint64(id))
		if err != nil {
			return hadIssue, fmt.Errorf("update bitmap: %w", err)
		}
	}

	return hadIssue, nil
}

// check that postings for given shard is consistent.
func (ve *verifierExecution) verifyShard( //nolint:maintidx
	ctx context.Context,
	shard int32,
) (hadIssue bool, err error) {
	updates := make([]postingUpdateRequest, 0)
	labelToIndex := make(map[labels.Label]int)

	localAll, err := ve.index.postings(ctx, []int32{shard}, allPostingLabel, allPostingLabel, false)
	if err != nil {
		return false, err
	}

	localMaybe, err := ve.index.postings(ctx, []int32{shard}, maybePostingLabel, maybePostingLabel, false)
	if err != nil {
		return false, err
	}

	if !localAll.Any() {
		hadIssue = true

		fmt.Fprintf(
			ve.output,
			"shard %s is empty (automatic cleanup may fix this)!\n",
			timeForShard(shard).Format(shardDateFormat),
		)
	}

	if !localMaybe.Any() {
		hadIssue = true

		fmt.Fprintf(
			ve.output,
			"shard %s is empty!\n",
			timeForShard(shard).Format(shardDateFormat),
		)
	}

	tmp := localAll.Difference(localMaybe)
	it := tmp.Iterator()

	for {
		id, eof := it.Next()
		if eof {
			break
		}

		hadIssue = true

		fmt.Fprintf(
			ve.output,
			"shard %s: ID %d is present in localAll but not in localMaybe!\n",
			timeForShard(shard).Format(shardDateFormat),
			id,
		)

		if ve.doFix {
			lbl := labels.Label{
				Name:  maybePostingLabel,
				Value: maybePostingLabel,
			}

			idx, ok := labelToIndex[lbl]
			if !ok {
				idx = len(updates)
				updates = append(updates, postingUpdateRequest{
					Label: lbl,
					Shard: shard,
				})
				labelToIndex[lbl] = idx
			}

			updates[idx].AddIDs = append(updates[idx].AddIDs, id)
		}
	}

	tmp = localMaybe.Difference(localAll)
	it = tmp.Iterator()

	for {
		id, eof := it.Next()
		if eof {
			break
		}

		hadIssue = true

		fmt.Fprintf(
			ve.output,
			"shard %s: ID %d is present in localMaybe but not in localAll (automatic cleanup may fix this)!\n",
			timeForShard(shard).Format(shardDateFormat),
			id,
		)

		if ve.doFix {
			lbl := labels.Label{
				Name:  maybePostingLabel,
				Value: maybePostingLabel,
			}

			idx, ok := labelToIndex[lbl]
			if !ok {
				idx = len(updates)
				updates = append(updates, postingUpdateRequest{
					Label: lbl,
					Shard: shard,
				})
				labelToIndex[lbl] = idx
			}

			updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, id)
		}
	}

	wantedPostings := make(map[labels.Label]*roaring.Bitmap)
	labelNames := make(map[string]interface{})
	it = localAll.Iterator()

	pendingIDs := make([]types.MetricID, 0, 10000)

	for ctx.Err() == nil {
		pendingIDs = pendingIDs[:0]

		for ctx.Err() == nil {
			id, eof := it.Next()
			if eof {
				break
			}

			pendingIDs = append(pendingIDs, types.MetricID(id))
			if len(pendingIDs) > 1000 {
				break
			}
		}

		if len(pendingIDs) == 0 {
			break
		}

		tmp, _, err := ve.index.selectIDS2LabelsAndExpiration(ctx, pendingIDs)
		if err != nil {
			return true, fmt.Errorf("get labels: %w", err)
		}

		for id, lbls := range tmp {
			for _, lbl := range lbls {
				labelNames[lbl.Name] = nil

				bitset := wantedPostings[lbl]
				if bitset == nil {
					bitset = roaring.NewBTreeBitmap()
				}

				_, err = bitset.AddN(uint64(id))
				if err != nil {
					return true, fmt.Errorf("update bitmap: %w", err)
				}

				wantedPostings[lbl] = bitset

				lbl2 := labels.Label{
					Name:  postinglabelName,
					Value: lbl.Name,
				}

				bitset = wantedPostings[lbl2]
				if bitset == nil {
					bitset = roaring.NewBTreeBitmap()
				}

				_, err = bitset.AddN(uint64(id))
				if err != nil {
					return true, fmt.Errorf("update bitmap: %w", err)
				}

				wantedPostings[lbl2] = bitset
			}
		}
	}

	references := []struct {
		it   *roaring.Bitmap
		name string
	}{
		{name: "global all IDs", it: ve.allPosting},
		{name: "shard all IDs", it: localAll},
		{name: "shard maybe present IDs", it: localMaybe},
	}

	labelNames[postinglabelName] = true

	iter := ve.index.store.SelectPostingByName(ctx, shard, postinglabelName)
	defer iter.Close()

	for iter.HasNext() {
		labelValue, buffer := iter.Next()

		if _, ok := labelNames[labelValue]; !ok { //nolint:nestif
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"shard %s: postinglabelName has extra name=%s\n",
				timeForShard(shard).Format(shardDateFormat),
				labelValue,
			)

			if ve.doFix {
				lbl := labels.Label{
					Name:  postinglabelName,
					Value: labelValue,
				}
				tmp := roaring.NewBTreeBitmap()

				err := tmp.UnmarshalBinary(buffer)
				if err != nil {
					return hadIssue, fmt.Errorf("unmarshal fail: %w", err)
				}

				idx, ok := labelToIndex[lbl]
				if !ok {
					idx = len(updates)
					updates = append(updates, postingUpdateRequest{
						Label: lbl,
						Shard: shard,
					})
					labelToIndex[lbl] = idx
				}

				updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, tmp.Slice()...)
			}
		}
	}

	for name := range labelNames {
		if ctx.Err() != nil {
			break
		}

		iter := ve.index.store.SelectPostingByName(ctx, shard, name)
		defer iter.Close()

		for iter.HasNext() {
			tmp := roaring.NewBTreeBitmap()
			labelValue, buffer := iter.Next()

			err := tmp.UnmarshalBinary(buffer)
			if err != nil {
				return hadIssue, fmt.Errorf("unmarshal fail: %w", err)
			}

			lbl := labels.Label{
				Name:  name,
				Value: labelValue,
			}

			wanted := wantedPostings[lbl]
			if wanted == nil { //nolint:nestif
				hadIssue = true

				fmt.Fprintf(
					ve.output,
					"shard %s: extra posting for %s=%s exists (with %d IDs)\n",
					timeForShard(shard).Format(shardDateFormat),
					name,
					labelValue,
					tmp.Count(),
				)

				if ve.doFix {
					idx, ok := labelToIndex[lbl]
					if !ok {
						idx = len(updates)
						updates = append(updates, postingUpdateRequest{
							Label: lbl,
							Shard: shard,
						})
						labelToIndex[lbl] = idx
					}

					updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, tmp.Slice()...)
				}
			} else {
				delete(wantedPostings, lbl)

				tmp2 := wanted.Difference(tmp)
				it := tmp2.Iterator()

				for {
					id, eof := it.Next()
					if eof {
						break
					}

					hadIssue = true

					fmt.Fprintf(
						ve.output,
						"shard %s: missing ID %d in posting for %s=%s\n",
						timeForShard(shard).Format(shardDateFormat),
						id,
						name,
						labelValue,
					)

					if ve.doFix {
						idx, ok := labelToIndex[lbl]
						if !ok {
							idx = len(updates)
							updates = append(updates, postingUpdateRequest{
								Label: lbl,
								Shard: shard,
							})
							labelToIndex[lbl] = idx
						}

						updates[idx].AddIDs = append(updates[idx].AddIDs, id)
					}
				}

				tmp2 = tmp.Difference(wanted)
				it = tmp2.Iterator()

				for {
					id, eof := it.Next()
					if eof {
						break
					}

					hadIssue = true

					fmt.Fprintf(
						ve.output,
						"shard %s: extra ID %d in posting for %s=%s (present in maybe=%v allId=%v globalAll=%v)\n",
						timeForShard(shard).Format(shardDateFormat),
						id,
						name,
						labelValue,
						localMaybe.Contains(id),
						localAll.Contains(id),
						ve.allPosting.Contains(id),
					)

					if ve.doFix {
						idx, ok := labelToIndex[lbl]
						if !ok {
							idx = len(updates)
							updates = append(updates, postingUpdateRequest{
								Label: lbl,
								Shard: shard,
							})
							labelToIndex[lbl] = idx
						}

						updates[idx].RemoveIDs = append(updates[idx].RemoveIDs, id)
					}
				}
			}

			for _, reference := range references {
				tmp = tmp.Difference(reference.it)
				it := tmp.Iterator()

				for ctx.Err() == nil {
					id, eof := it.Next()
					if eof {
						break
					}

					hadIssue = true

					fmt.Fprintf(
						ve.output,
						"shard %s: posting for %s=%s has ID %d which is not in %s!\n",
						timeForShard(shard).Format(shardDateFormat),
						name,
						labelValue,
						id,
						reference.name,
					)
				}
			}
		}
	}

	for lbl, wantValue := range wantedPostings {
		hadIssue = true

		fmt.Fprintf(
			ve.output,
			"shard %s: posting %s=%s was expected to exists\n",
			timeForShard(shard).Format(shardDateFormat),
			lbl.Name,
			lbl.Value,
		)

		if ve.doFix {
			idx, ok := labelToIndex[lbl]
			if !ok {
				idx = len(updates)
				updates = append(updates, postingUpdateRequest{
					Label: lbl,
					Shard: shard,
				})
				labelToIndex[lbl] = idx
			}

			updates[idx].AddIDs = append(updates[idx].AddIDs, wantValue.Slice()...)
		}
	}

	if ve.doFix && len(updates) > 0 {
		err = ve.index.concurrentTasks(
			ctx,
			concurrentInsert,
			func(ctx context.Context, work chan<- func() error) error {
				for _, req := range updates {
					req := req
					task := func() error {
						_, err := ve.index.postingUpdate(ctx, req)

						if errors.Is(err, errBitmapEmpty) {
							err = nil
						}

						return err
					}
					select {
					case work <- task:
					case <-ctx.Done():
						return ctx.Err()
					}
				}

				return nil
			},
		)
		if err != nil {
			return hadIssue, err
		}
	}

	return hadIssue, nil
}

// check that expiration entries are consitent.
func (ve *verifierExecution) verifyExpiration(ctx context.Context) (hadIssue bool, err error) {
	// In a perfect situation, what is in Cassandra should exactly match v.expectedExpiration
	// Thing that are allowed:
	// * entry in expiration not present in v.allPosting (mean a creation was aborted)
	// * few entries present in unexpected bucket (mean some operation fail)
	// * entry no present in expected bucket, but present in older bucket not yet processed
	//
	// What is unexpected:
	// * an ID from v.allPosting never present
	startDay, err := ve.index.expirationLastProcessedDay(ctx)
	if err != nil {
		return false, err
	}

	if startDay.IsZero() {
		startDay = ve.now.Truncate(24 * time.Hour).Add(-24 * time.Hour)
	}

	var endDay time.Time

	idExpectedInFuture := roaring.NewBTreeBitmap()
	idSeenInExpiration := roaring.NewBTreeBitmap()
	idExpectedInPast := roaring.NewBTreeBitmap()

	for timestamp, tmp := range ve.expectedExpiration {
		day := time.Unix(timestamp, 0)

		if day.Before(startDay) {
			startDay = day
		}

		if endDay.IsZero() || day.After(endDay) {
			endDay = day
		}

		idExpectedInFuture.UnionInPlace(tmp)
	}

	// Start and end few days before/after expected limit
	startDay = startDay.Add(-24 * time.Hour * 7)
	endDay = endDay.Add(24 * time.Hour * 7)

	{
		tmp := ve.allGoodIDs.Difference(idExpectedInFuture)
		if tmp.Any() {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"allGoodIDs contains %d IDs for which we don't have an expiration expected. This seems a bug in verifier\n",
				tmp.Count(),
			)
		}

		tmp = idExpectedInFuture.Difference(ve.allPosting)
		if tmp.Any() {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"There is %d IDs that are expected to have an expiration which"+
					" don't existing in allPosting. This seems a bug in verifier\n",
				tmp.Count(),
			)
		}
	}

	for currentDay := startDay; endDay.After(currentDay); currentDay = currentDay.Add(24 * time.Hour) {
		gotExpiration, err := ve.index.cassandraGetExpirationList(ctx, currentDay)
		if err != nil {
			return false, err
		}

		wantExpiration := ve.expectedExpiration[currentDay.Unix()]

		if gotExpiration == nil {
			gotExpiration = roaring.NewBTreeBitmap()
		}

		if wantExpiration == nil {
			wantExpiration = roaring.NewBTreeBitmap()
		}

		gotUnexpectedIDs := gotExpiration.Difference(wantExpiration)
		gotInvalidIDs := gotUnexpectedIDs.Difference(ve.allPosting)
		gotValidUnexpectedIDs := gotUnexpectedIDs.Intersect(ve.allPosting)
		gotExpectedInPast := gotValidUnexpectedIDs.Intersect(idExpectedInPast)
		gotExpectedInFuture := gotValidUnexpectedIDs.Intersect(idExpectedInFuture)
		gotUnexpctedVerifierBug := gotValidUnexpectedIDs.
			Difference(idExpectedInPast).
			Difference(idExpectedInFuture).
			Difference(ve.expectedUnknowDate)

		gotMissingIDs := wantExpiration.Difference(gotExpiration)
		gotMissingSeenInPast := gotMissingIDs.Intersect(idSeenInExpiration)
		gotMissingNeverSeen := gotMissingIDs.Difference(idSeenInExpiration)

		if gotInvalidIDs.Any() && ve.strictExpiration {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"expiration day %s had %d IDs which are unknown to allPosting: %s\n",
				currentDay.Format(shardDateFormat),
				gotInvalidIDs.Count(),
				truncatedIDList(gotInvalidIDs, maxIDBeforeTruncate),
			)
		}

		if gotExpectedInFuture.Any() && ve.pedanticExpiration {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"expiration day %s had %d IDs which are expected in the future: %s\n",
				currentDay.Format(shardDateFormat),
				gotExpectedInFuture.Count(),
				truncatedIDList(gotExpectedInFuture, maxIDBeforeTruncate),
			)
		}

		if gotExpectedInPast.Any() && ve.strictExpiration {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"expiration day %s had %d IDs which are expected in the past: %s\n",
				currentDay.Format(shardDateFormat),
				gotExpectedInPast.Count(),
				truncatedIDList(gotExpectedInPast, maxIDBeforeTruncate),
			)
		}

		if gotUnexpctedVerifierBug.Any() {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"expiration day %s had %d IDs which are not expected in past, future or this day (verifier bug?): %s\n",
				currentDay.Format(shardDateFormat),
				gotUnexpctedVerifierBug.Count(),
				truncatedIDList(gotUnexpctedVerifierBug, maxIDBeforeTruncate),
			)
		}

		if gotMissingNeverSeen.Any() {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"expiration day %s do not contains expected ID (and there were not in previous day): %s\n",
				currentDay.Format(shardDateFormat),
				truncatedIDList(gotMissingNeverSeen, maxIDBeforeTruncate),
			)
		}

		if gotMissingSeenInPast.Any() && ve.strictExpiration {
			hadIssue = true

			fmt.Fprintf(
				ve.output,
				"expiration day %s do not contains expected ID (but there were in previous day): %s\n",
				currentDay.Format(shardDateFormat),
				truncatedIDList(gotMissingSeenInPast, maxIDBeforeTruncate),
			)
		}

		idSeenInExpiration.UnionInPlace(gotExpiration)
		idExpectedInFuture.DifferenceInPlace(wantExpiration)
		idExpectedInPast.UnionInPlace(wantExpiration)
	}

	unknownDateNotSeen := ve.expectedUnknowDate.Difference(idSeenInExpiration)
	if unknownDateNotSeen.Any() {
		hadIssue = true

		fmt.Fprintf(
			ve.output,
			"expirations do not contains expected ID (with unknown date): %s\n",
			truncatedIDList(unknownDateNotSeen, maxIDBeforeTruncate),
		)
	}

	if idExpectedInFuture.Any() {
		hadIssue = true

		fmt.Fprintf(
			ve.output,
			"there is still ID expected in the future... this is a verifier bug?: %s\n",
			truncatedIDList(idExpectedInFuture, maxIDBeforeTruncate),
		)
	}

	return hadIssue, nil
}

func truncatedIDList(bitmap *roaring.Bitmap, maxItem int) string {
	iter := bitmap.Iterator()
	buffer := make([]string, 0, maxItem)
	elipsis := false
	numberMore := 0

	for {
		id, eof := iter.Next()
		if eof {
			break
		}

		if len(buffer) == maxItem {
			elipsis = true
			numberMore = int(bitmap.Count()) - maxItem

			break
		}

		buffer = append(buffer, strconv.FormatInt(int64(id), 10))
	}

	if len(buffer) == 0 {
		return "nothing"
	}

	result := strings.Join(buffer, ", ")
	if elipsis {
		result += fmt.Sprintf("... (%d more)", numberMore)
	}

	return result
}

func truncatedSliceIDList(ids []uint64, maxItem int) string {
	buffer := make([]string, 0, maxItem)
	elipsis := false
	numberMore := 0

	for _, id := range ids {
		if len(buffer) == maxItem {
			elipsis = true
			numberMore = len(ids) - maxItem

			break
		}

		buffer = append(buffer, strconv.FormatInt(int64(id), 10))
	}

	if len(buffer) == 0 {
		return "nothing"
	}

	result := strings.Join(buffer, ", ")
	if elipsis {
		result += fmt.Sprintf("... (%d more)", numberMore)
	}

	return result
}
