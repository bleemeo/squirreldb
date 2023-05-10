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

// MetricIterToList convert a MetricDataSet to a list of MetricData.
// numberOfNextCall define the maximum number of call done to Next(). 0 means unlimited.
func MetricIterToList(i MetricDataSet, numberOfNextCall int) ([]MetricData, error) {
	results := make([]MetricData, 0)

	for i.Next() {
		tmp := i.At()
		results = append(results, tmp)

		if len(results) >= numberOfNextCall && numberOfNextCall > 0 {
			break
		}
	}

	return results, i.Err()
}
