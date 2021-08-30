package promql

import (
	"github.com/prometheus/client_golang/prometheus"
)

type PromQL struct {
	metrics *metrics
}

// New returns a new initialized PromQL manager.
func New(reg prometheus.Registerer) PromQL {
	p := PromQL{
		metrics: newMetrics(reg),
	}

	return p
}
