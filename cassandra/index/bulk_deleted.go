package index

import (
	"context"
	"squirreldb/types"

	"github.com/prometheus/prometheus/pkg/labels"
)

type deleter struct {
	c                     *CassandraIndex
	deleteLabels          []string
	deleteIDs             []uint64
	postingUpdates        []postingUpdateRequest
	labelToPostingUpdates map[string]map[string]int
}

func newBulkDeleter(c *CassandraIndex) *deleter {
	return &deleter{
		labelToPostingUpdates: make(map[string]map[string]int),
		c:                     c,
	}
}

// PrepareDelete add a metric to be deleted. Not read is performed at this point and no lock is required
// sortedLabels may be nil if labels are unknown (so only ID is deleted from all postings).
// Skipping labels2id is used by index verification & fix. It shouldn't be used in normal condition.
func (d *deleter) PrepareDelete(id types.MetricID, sortedLabels labels.Labels, skipLabels2Id bool) { // nolint: interfacer
	d.deleteIDs = append(d.deleteIDs, uint64(id))

	if sortedLabels != nil && !skipLabels2Id {
		sortedLabelsString := sortedLabels.String()
		d.deleteLabels = append(d.deleteLabels, sortedLabelsString)
	}

	for _, label := range sortedLabels {
		m, ok := d.labelToPostingUpdates[label.Name]
		if !ok {
			m = make(map[string]int)
			d.labelToPostingUpdates[label.Name] = m
		}

		idx, ok := m[label.Value]
		if !ok {
			idx = len(d.postingUpdates)
			d.postingUpdates = append(d.postingUpdates, postingUpdateRequest{
				Label: label,
			})
			m[label.Value] = idx
		}

		d.postingUpdates[idx].RemoveIDs = append(d.postingUpdates[idx].RemoveIDs, uint64(id))
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
// * drop id from every postings (TODO: during those deletes, Search() may wrongly return the metric ID in cause of negative search.)
// * drop the id2labels (at this points, Search() can elimitate wrong metric ID)
// * the remote the id from all postings, making the ID free
//
// Note: it's not this function which clear the expiration table, this is done elsewhere.
func (d *deleter) Delete() error {
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

	err := d.c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, sortedLabelsString := range d.deleteLabels {
			sortedLabelsString := sortedLabelsString
			task := func() error {
				return d.c.store.DeleteLabels2ID(sortedLabelsString)
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

	err = d.c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, req := range d.postingUpdates {
			req := req
			task := func() error {
				return d.c.postingUpdate(req)
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

	err = d.c.concurrentTasks(func(ctx context.Context, work chan<- func() error) error {
		for _, id := range d.deleteIDs {
			id := types.MetricID(id)
			task := func() error {
				return d.c.store.DeleteID2Labels(id)
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

	err = d.c.postingUpdate(postingUpdateRequest{
		Label:     labels.Label{Name: allPostingLabelName, Value: allPostingLabelValue},
		RemoveIDs: d.deleteIDs,
	})
	if err != nil {
		return err
	}

	return nil
}
