package debug

import (
	"log"
	"os"
	"time"
)

var logger = log.New(os.Stdout, "[debug] ", log.LstdFlags)

type Performance struct {
	clock time.Time
}

func NewPerformance() *Performance {
	return &Performance{
		clock: time.Now(),
	}
}

func (p *Performance) Print(origin, verb, unit string) {
	duration := time.Since(p.clock)

	logger.Printf("[%s] %s %s in %v"+"\n", origin, verb, unit, duration)
}

func (p *Performance) PrintValue(origin, verb, unit string, value float64) {
	duration := time.Since(p.clock)
	perSeconds := value / duration.Seconds()

	logger.Printf("[%s] %s %f %s(s) in %v (%v %s(s)/sec)"+"\n", origin, verb, value, unit, duration, perSeconds, unit)
}
