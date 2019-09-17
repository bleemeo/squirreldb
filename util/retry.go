package util

import (
	"log"
	"time"
)

func Retry(attempts int, delay time.Duration, function func() error) error {
	var err error

	for i := attempts; (i >= 0) || (i == -1); i-- {
		if err = function(); err != nil {
			log.Printf("|_ Retry in %v (remaining attempts: %d)"+"\n", delay, i)
		} else {
			return nil
		}

		if i == -1 {
			i += 1
		}

		time.Sleep(delay)
	}

	return err
}
