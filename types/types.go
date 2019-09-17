package types

type Reader interface {
	Read(mRequest MetricRequest) ([]MetricPoints, error)
}

type Writer interface {
	Write(msPoints []MetricPoints) error
}
