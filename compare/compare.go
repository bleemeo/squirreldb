package compare

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
