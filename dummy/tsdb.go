package dummy

import "squirreldb/types"

// DiscardTSDB will write metrics to /dev/null :)
type DiscardTSDB struct{}

type emptyResult struct{}

// ReadIter return an empty result
func (d DiscardTSDB) ReadIter(request types.MetricRequest) (types.MetricDataSet, error) {
	return emptyResult{}, nil
}

// Write discard metrics
func (d DiscardTSDB) Write(metrics []types.MetricData) error {
	return nil
}

func (r emptyResult) Next() bool {
	return false
}

func (r emptyResult) At() types.MetricData {
	panic("At() shouldn't be called on emptyResult")
}

func (r emptyResult) Err() error {
	return nil
}
