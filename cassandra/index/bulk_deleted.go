package index

import (
	"context"
	"errors"
	"squirreldb/types"
	"sync"

	"github.com/gocql/gocql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/prometheus/prometheus/pkg/labels"
)

type deleter struct {
	c                       *CassandraIndex
	deleteLabels            []string
	deleteIDs               []uint64
	unshardedPostingUpdates []postingUpdateRequest
	invalidateKey           []postingsCacheKey
	labelToPostingUpdates   map[string]map[string]int
}

func newBulkDeleter(c *CassandraIndex) *deleter {
	return &deleter{
		labelToPostingUpdates: make(map[string]map[string]int),
		c:                     c,
	}
}

// PrepareDelete add a metric to be deleted. No read is performed at this point and no lock is required
// sortedLabels may be nil if labels are unknown (so only ID is deleted from all postings).
// Skipping labels2id is used by index verification & fix. It shouldn't be used in normal condition.
func (d *deleter) PrepareDelete(id types.MetricID, sortedLabels labels.Labels, skipLabels2Id bool) {
	d.deleteIDs = append(d.deleteIDs, uint64(id))

	if sortedLabels != nil && !skipLabels2Id {
		sortedLabelsString := sortedLabels.String()
		d.deleteLabels = append(d.deleteLabels, sortedLabelsString)
	}

	for _, lbl := range sortedLabels {
		d.invalidateKey = append(d.invalidateKey, postingsCacheKey{
			Shard: globalShardNumber,
			Name:  lbl.Name,
			Value: lbl.Value,
		})
	}

	labelsList := make(labels.Labels, 0, len(sortedLabels)*2)
	labelsList = append(labelsList, sortedLabels...)

	for _, label := range sortedLabels {
		labelsList = append(labelsList, labels.Label{
			Name:  postinglabelName,
			Value: label.Name,
		})
	}

	for _, label := range labelsList {
		m, ok := d.labelToPostingUpdates[label.Name]
		if !ok {
			m = make(map[string]int)
			d.labelToPostingUpdates[label.Name] = m
		}

		idx, ok := m[label.Value]
		if !ok {
			idx = len(d.unshardedPostingUpdates)
			d.unshardedPostingUpdates = append(d.unshardedPostingUpdates, postingUpdateRequest{
				Label: label,
			})
			m[label.Value] = idx
		}

		d.unshardedPostingUpdates[idx].RemoveIDs = append(d.unshardedPostingUpdates[idx].RemoveIDs, uint64(id))
	}
}

// Delete perform the deletion and REQUIRE the newMetricLockName.
//
// The method should be called only once, a new deleter should be created to reuse it.
//
// This method will also remove IDs local in-memory cache
//
// Deletion is performed in reverse order of the creation of metrics:
// * First we delete from labels2id (this cause any new write to create a new metrics)
// * drop id from every sharded postings (TODO: during those deletes, Search() may wrongly
//   return the metric ID in cause of negative search.)
// * drop the id2labels (at this points, Search() can elimitate wrong metric ID)
// * the remote the id from all postings, making the ID free
//
// Note: it's not this function which clear the expiration table, this is done elsewhere.
func (d *deleter) Delete(ctx context.Context) error { //nolint:gocognit,gocyclo,cyclop
	if len(d.deleteIDs) == 0 {
		return nil
	}

	// Delete metrics from cache *before* processing to Cassandra.
	// Doing this ensure that if a write for a metric that in being delete will
	// trigger the creation of a new metrics (which will wait for complet delete since we hold the lock).
	// Note: in case of multiple SquirrelDB, this race-condition may still happen, but the only consequence
	// is leaving an orphaned id2labels entry that will not be used and will eventually be purged when its
	// expiration is reached.
	d.c.deleteIDsFromCache(d.deleteIDs)

	err := d.c.concurrentTasks(ctx, func(ctx context.Context, work chan<- func() error) error {
		for _, sortedLabelsString := range d.deleteLabels {
			sortedLabelsString := sortedLabelsString
			task := func() error {
				return d.c.store.DeleteLabels2ID(ctx, sortedLabelsString)
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	shards, err := d.c.postings(ctx, []int32{globalShardNumber}, existingShardsLabel, existingShardsLabel, false)
	if err != nil {
		return err
	}

	maybePresent, err := d.c.getMaybePresent(ctx, shards.Slice())
	if err != nil {
		return err
	}

	shardedUpdates := make([]postingUpdateRequest, 0, len(d.unshardedPostingUpdates)*int(shards.Count()))
	presenceUpdates := make([]postingUpdateRequest, 0, len(maybePresent))
	maybePresenceUpdates := make([]postingUpdateRequest, 0, len(maybePresent))

	for _, shard := range shards.Slice() {
		shard := int32(shard)

		it := maybePresent[shard]
		if it == nil || !it.Any() {
			continue
		}

		presenceUpdates = append(presenceUpdates, postingUpdateRequest{
			Shard: shard,
			Label: labels.Label{
				Name:  allPostingLabel,
				Value: allPostingLabel,
			},
			RemoveIDs: d.deleteIDs,
		})

		for _, req := range d.unshardedPostingUpdates {
			ids := roaring.NewBitmap(req.RemoveIDs...)
			if ids.IntersectionCount(it) == 0 {
				continue
			}

			req.Shard = shard
			shardedUpdates = append(shardedUpdates, req)
		}

		maybePresenceUpdates = append(maybePresenceUpdates, postingUpdateRequest{
			Shard: shard,
			Label: labels.Label{
				Name:  maybePostingLabel,
				Value: maybePostingLabel,
			},
			RemoveIDs: d.deleteIDs,
		})
	}

	d.c.invalidatePostings(ctx, d.invalidateKey)

	err = d.c.concurrentTasks(ctx, func(ctx context.Context, work chan<- func() error) error {
		for _, req := range presenceUpdates {
			req := req
			task := func() error {
				bitmap, err := d.c.postingUpdate(ctx, req)
				if err != nil && !errors.Is(err, gocql.ErrNotFound) {
					return err
				}

				d.c.lookupIDMutex.Lock()
				if bitmap == nil {
					delete(d.c.idInShard, req.Shard)
				} else {
					d.c.idInShard[req.Shard] = bitmap
				}
				d.c.lookupIDMutex.Unlock()

				return nil
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = d.c.concurrentTasks(ctx, func(ctx context.Context, work chan<- func() error) error {
		for _, req := range shardedUpdates {
			req := req
			task := func() error {
				_, err := d.c.postingUpdate(ctx, req)
				if err != nil && !errors.Is(err, gocql.ErrNotFound) {
					return err
				}

				return nil
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	var l sync.Mutex

	shardsListUpdate := postingUpdateRequest{
		Shard: globalShardNumber,
		Label: labels.Label{
			Name:  existingShardsLabel,
			Value: existingShardsLabel,
		},
	}

	err = d.c.concurrentTasks(ctx, func(ctx context.Context, work chan<- func() error) error {
		for _, req := range maybePresenceUpdates {
			req := req
			task := func() error {
				it, err := d.c.postingUpdate(ctx, req)
				if err != nil && !errors.Is(err, gocql.ErrNotFound) {
					return err
				}

				// it is nil iff it's empty (or an error occure)
				if it == nil {
					l.Lock()
					shardsListUpdate.RemoveIDs = append(shardsListUpdate.RemoveIDs, uint64(req.Shard))
					l.Unlock()
				}

				return nil
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if len(shardsListUpdate.RemoveIDs) > 0 {
		_, err := d.c.postingUpdate(ctx, shardsListUpdate)
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
			return err
		}
	}

	err = d.c.concurrentTasks(ctx, func(ctx context.Context, work chan<- func() error) error {
		for _, id := range d.deleteIDs {
			id := types.MetricID(id)
			task := func() error {
				return d.c.store.DeleteID2Labels(ctx, id)
			}
			select {
			case work <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	_, err = d.c.postingUpdate(ctx, postingUpdateRequest{
		Shard:     globalShardNumber,
		Label:     labels.Label{Name: globalAllPostingLabel, Value: globalAllPostingLabel},
		RemoveIDs: d.deleteIDs,
	})
	if err != nil {
		return err
	}

	return nil
}
