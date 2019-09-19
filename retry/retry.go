package retry

import (
	"log"
	"time"
)

func Endlessly(delay time.Duration, function func() error, logger *log.Logger) {
	for {
		if err := function(); err != nil {
			logger.Println("|__ Retry in", delay)
		} else {
			return
		}

		time.Sleep(delay)
	}
}
