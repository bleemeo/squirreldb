package types

type MetricIndexer interface {
	UUID(labels MetricLabels) MetricUUID
	UUIDs(labels MetricLabels) map[MetricUUID]MetricLabels
}

type MetricReader interface {
	Read(request MetricRequest) (Metrics, error)
}

type MetricWriter interface {
	Write(metrics Metrics) error
}
