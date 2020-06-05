package types

func MetricIterFromList(input []MetricData) MetricDataSet {
	return &readIter{
		input: input,
	}
}

type readIter struct {
	input      []MetricData
	nextOffset int
}

func (i *readIter) Err() error {
	return nil
}

func (i *readIter) Next() bool {
	if i.nextOffset == len(i.input) {
		return false
	}

	i.nextOffset++

	return true
}

func (i *readIter) At() MetricData {
	return i.input[i.nextOffset-1]
}
