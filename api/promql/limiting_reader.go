package promql

import (
	"context"
	"errors"
	"squirreldb/types"
	"sync/atomic"
)

type limitingReader struct {
	reader         types.MetricReader
	maxTotalPoints uint64
	returnedPoints uint64
}

func (rdr *limitingReader) ReadIter(ctx context.Context, req types.MetricRequest) (types.MetricDataSet, error) {
	r, err := rdr.reader.ReadIter(ctx, req)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return limitDataSet{
		rdr: rdr,
		set: r,
	}, nil
}

func (rdr *limitingReader) PointsRead() float64 {
	v := atomic.LoadUint64(&rdr.returnedPoints)

	return float64(v)
}

func (rdr *limitingReader) PointsCached() float64 {
	if reader, ok := rdr.reader.(metricReaderWithCache); ok {
		return reader.PointsCached()
	}

	return 0
}

type limitDataSet struct {
	rdr *limitingReader
	set types.MetricDataSet
}

func (d limitDataSet) Next() bool {
	if d.rdr.maxTotalPoints != 0 && atomic.LoadUint64(&d.rdr.returnedPoints) > d.rdr.maxTotalPoints {
		return false
	}

	if !d.set.Next() {
		return false
	}

	count := len(d.set.At().Points)

	newSize := atomic.AddUint64(&d.rdr.returnedPoints, uint64(count))

	return d.rdr.maxTotalPoints == 0 || newSize <= d.rdr.maxTotalPoints
}

func (d limitDataSet) At() types.MetricData {
	return d.set.At()
}

func (d limitDataSet) Err() error {
	if d.rdr.maxTotalPoints != 0 && atomic.LoadUint64(&d.rdr.returnedPoints) > d.rdr.maxTotalPoints {
		return errors.New("too many points evaluated by this PromQL")
	}

	return d.set.Err()
}
