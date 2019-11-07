package types

type MetricIndexer interface {
	Pairs(matchers MetricLabels) map[MetricUUID]MetricLabels
	UUID(labels MetricLabels) MetricUUID
	UUIDs() MetricUUIDs
}

type MetricReader interface {
	Read(request MetricRequest) (Metrics, error)
}

type MetricWriter interface {
	Write(metrics Metrics) error
}
