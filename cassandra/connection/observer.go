package connection

import "github.com/gocql/gocql"

type connectObserver struct {
	connection *Connection
}

func (obs connectObserver) ObserveConnect(msg gocql.ObservedConnect) {
	obs.connection.logger.Info().
		Time("start", msg.Start).
		Time("end", msg.End).
		Err(msg.Err).
		Str("HostInfo", msg.Host.String()).
		Msg("ObserveConnect")
}
