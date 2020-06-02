package compare

// MinInt64 returns the smallest number between x and y.
func MinInt64(x, y int64) int64 {
	if x < y {
		return x
	}

	return y
}

// MaxInt64 returns the largest number between x and y.
func MaxInt64(x, y int64) int64 {
	if x > y {
		return x
	}

	return y
}
