package promql

import (
	"errors"
	"squirreldb/types"
	"sync/atomic"
)

type limitingReader struct {
	reader         types.MetricReader
	maxTotalPoints uint64
	returnedPoints uint64
}

func (rdr *limitingReader) ReadIter(req types.MetricRequest) (types.MetricDataSet, error) {
	r, err := rdr.reader.ReadIter(req)
	if err != nil {
		return nil, err
	}

	return limitDataSet{
		rdr: rdr,
		set: r,
	}, nil
}

type limitDataSet struct {
	rdr *limitingReader
	set types.MetricDataSet
}

func (d limitDataSet) Next() bool {
	if atomic.LoadUint64(&d.rdr.returnedPoints) > d.rdr.maxTotalPoints {
		return false
	}

	if !d.set.Next() {
		return false
	}

	count := len(d.set.At().Points)

	return atomic.AddUint64(&d.rdr.returnedPoints, uint64(count)) <= d.rdr.maxTotalPoints
}

func (d limitDataSet) At() types.MetricData {
	return d.set.At()
}

func (d limitDataSet) Err() error {
	if atomic.LoadUint64(&d.rdr.returnedPoints) > d.rdr.maxTotalPoints {
		return errors.New("too many points evaluated by this PromQL")
	}

	return d.set.Err()
}
