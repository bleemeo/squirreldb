package connection

import "github.com/gocql/gocql"

type connectObserver struct {
	connection *Connection
}

type connectError struct {
	err         error
	hostAndPort string
}

func (obs connectObserver) ObserveConnect(msg gocql.ObservedConnect) {
	if msg.Err != nil {
		obs.connection.l.Lock()
		obs.connection.lastObservedError = connectError{
			err:         msg.Err,
			hostAndPort: msg.Host.HostnameAndPort(),
		}
		obs.connection.l.Unlock()

		select {
		case obs.connection.wakeRunLoop <- nil:
		default:
		}

		obs.connection.logger.Debug().
			Err(msg.Err).
			Str("HostnameAndPort", msg.Host.HostnameAndPort()).
			Msg("ObserveConnect see an error")
	}
}
