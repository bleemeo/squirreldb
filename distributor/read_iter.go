package distributor

import "squirreldb/types"

type readIter struct {
	d              *Distributor
	members        []types.Node
	requestByShard []types.MetricRequest

	nextShard   int
	currentData types.MetricDataSet
	err         error
}

func (i *readIter) Next() bool {
	for {
		if i.currentData == nil {
			if i.nextShard >= len(i.requestByShard) {
				return false
			}

			if len(i.requestByShard[i.nextShard].IDs) == 0 {
				i.nextShard++
				continue
			}

			tmp, err := i.d.readShardPart(i.members, i.nextShard, i.requestByShard[i.nextShard])
			if err != nil {
				i.err = err

				return false
			}

			i.currentData = tmp
			i.nextShard++
		}

		if i.currentData == nil {
			continue
		}

		ok := i.currentData.Next()
		if !ok {
			err := i.currentData.Err()
			if err != nil {
				i.err = err

				return false
			}

			i.currentData = nil

			continue
		}

		return true
	}
}

func (i *readIter) At() types.MetricData {
	return i.currentData.At()
}

func (i *readIter) Err() error {
	return i.err
}
