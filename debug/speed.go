package debug

import (
	"log"
	"os"
	"time"
)

var logger = log.New(os.Stdout, "[debug] ", log.LstdFlags)

type Speed struct {
	clock    time.Time
	duration time.Duration
	values   float64
	run      bool
}

func NewSpeed() *Speed {
	return &Speed{}
}

func (p *Speed) Add(value float64) {
	p.values += value
}

func (p *Speed) Seconds() float64 {
	return p.duration.Seconds()
}

func (p *Speed) Start() {
	if !p.run {
		p.clock = time.Now()
		p.run = true
	}
}

func (p *Speed) Stop(value float64) {
	if p.run {
		p.duration += time.Since(p.clock)
		p.values += value
		p.run = false
	}
}

func (p *Speed) Print(origin, verb, unit string) {
	perSeconds := p.values / p.duration.Seconds()

	logger.Printf("[%s] %s %f %s(s) in %v (%v %s(s)/sec)"+"\n", origin, verb, p.values, unit, p.duration, perSeconds, unit)
}
