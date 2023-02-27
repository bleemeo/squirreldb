package connection

import (
	"sync"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

type connectObserver struct {
	l                 sync.Mutex
	lastObservedError connectError
	logger            zerolog.Logger
	wakeRunLoop       chan interface{}
}

type connectError struct {
	err         error
	hostAndPort string
}

func (obs *connectObserver) GetAndClearLastObservation() connectError {
	obs.l.Lock()
	defer obs.l.Unlock()

	value := obs.lastObservedError
	obs.lastObservedError = connectError{}

	return value
}

func (obs *connectObserver) ObserveConnect(msg gocql.ObservedConnect) {
	if msg.Err != nil {
		obs.l.Lock()
		obs.lastObservedError = connectError{
			err:         msg.Err,
			hostAndPort: msg.Host.HostnameAndPort(),
		}
		obs.l.Unlock()

		select {
		case obs.wakeRunLoop <- nil:
		default:
		}

		obs.logger.Debug().
			Err(msg.Err).
			Str("HostnameAndPort", msg.Host.HostnameAndPort()).
			Msg("ObserveConnect see an error")
	}
}
