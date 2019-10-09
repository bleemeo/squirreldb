package types

type MetricReader interface {
	Read(request MetricRequest) (map[MetricUUID]MetricData, error)
}

type MetricWriter interface {
	Write(metrics map[MetricUUID]MetricData) error
}
