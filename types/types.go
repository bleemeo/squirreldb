package types

type Storer interface {
	Get(key string) (MetricPoints, error)
	Set(newMsPoints map[string]MetricPoints, currentMsPoints map[string]MetricPoints) error
}

type Reader interface {
	Read(mRequest MetricRequest) ([]MetricPoints, error)
}

type Writer interface {
	Write(msPoints []MetricPoints) error
}
