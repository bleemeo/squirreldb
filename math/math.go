package math

import "math"

// TODO: Comment
func Float64Min(x, y float64) float64 {
	return math.Min(x, y)
}

// TODO: Comment
func Float64Max(x, y float64) float64 {
	return math.Max(x, y)
}

// TODO: Comment
func Int64Min(x, y int64) int64 {
	return int64(math.Min(float64(x), float64(y)))
}

// TODO: Comment
func Int64Max(x, y int64) int64 {
	return int64(math.Max(float64(x), float64(y)))
}
