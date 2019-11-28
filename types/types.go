package types

type MetricIndexer interface {
	Labels(uuid MetricUUID) []MetricLabel
	UUID(labels []MetricLabel) MetricUUID
	UUIDs(matchers []MetricLabelMatcher, all bool) []MetricUUID
}

type MetricReader interface {
	Read(request MetricRequest) (map[MetricUUID]MetricData, error)
}

type MetricWriter interface {
	Write(metrics map[MetricUUID]MetricData) error
}

type Instance struct {
	Hostname string
	UUID     string
}
