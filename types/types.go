package types

type MetricMatcher interface {
	Match(labels MetricLabels) (MetricUUID, error)
	Matches(labels MetricLabels) ([]MetricUUID, error)
}

type MetricReader interface {
	Read(request MetricRequest) (Metrics, error)
}

type MetricWriter interface {
	Write(metrics Metrics) error
}
