package types

type MetricReader interface {
	Read(mRequest MetricRequest) ([]MetricPoints, error)
}

type MetricWriter interface {
	Write(msPoints []MetricPoints) error
}
