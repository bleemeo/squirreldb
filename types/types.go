package types

type Indexer interface {
	AllUUIDs() ([]MetricUUID, error)
	Labels(uuid MetricUUID) ([]MetricLabel, error)
	UUID(labels []MetricLabel) (MetricUUID, error)
	UUIDs(matchers []MetricLabelMatcher) ([]MetricUUID, error)
}

type MetricReader interface {
	Read(request MetricRequest) (map[MetricUUID]MetricData, error)
}

type MetricWriter interface {
	Write(metrics map[MetricUUID]MetricData)
}

type Locker interface {
	Delete(name string) error
	Write(name string, timeToLive int64) (bool, error)
	Update(name string, timeToLive int64) error
}

type Stater interface {
	Read(name string, value interface{}) error
	Update(name string, value interface{}) error
	Write(name string, value interface{}) error
}

type Instance struct {
	Hostname string
	UUID     string
}
