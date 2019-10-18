package debug

import (
	"fmt"
	"time"
)

type Performance struct {
	clock time.Time
}

func NewPerformance() *Performance {
	return &Performance{
		clock: time.Now(),
	}
}

func (p *Performance) Duration(message string) {
	fmt.Println(message, time.Since(p.clock))
}

func (p *Performance) Value(message string, value float64) {
	fmt.Println(message, value, time.Since(p.clock), value/time.Since(p.clock).Seconds())
}
