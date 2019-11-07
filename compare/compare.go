package compare

import "squirreldb/types"

// Int64Min returns the smallest number between x and y
func Int64Min(x, y int64) int64 {
	if x < y {
		return x
	}

	return y
}

// Int64Max returns the largest number between x and y
func Int64Max(x, y int64) int64 {
	if x > y {
		return x
	}

	return y
}

// LabelsEqual verifies the equality between x and y label tables
func LabelsEqual(x, y types.MetricLabels) bool {
	if len(x) != len(y) {
		return false
	}

	for i, label := range x {
		if label != y[i] {
			return false
		}
	}

	return true
}
