package util

import "time"

func TimeBetween(time time.Time, from time.Time, to time.Time) bool {
	return (time.After(from) || time.Equal(from)) && (time.Before(to) || time.Equal(to))
}
