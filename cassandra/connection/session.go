package connection

import (
	"sync"

	"github.com/gocql/gocql"
)

// SessionWrapper contains a Cassandra gocql session that could be used exactly like a normal gocql.Session.
// Like a normal gocql.Session is should be closed when no longer used. Unlike a gocql.Session when closed, the
// true gocql.Session might be really closed or not.
// Unlike gocql.Session, this session should be short-lived. A new SessionWrapper should be acquired from Connection
// regularly. Especially if a query is retried, every retry should use a new SessionWrapper.
type SessionWrapper struct {
	*gocql.Session

	l          sync.Mutex
	sessionID  int
	connection *Connection
	closed     bool
}

func (s *SessionWrapper) Close() {
	s.l.Lock()
	defer s.l.Unlock()

	if !s.closed {
		s.connection.wrapperCloseSession(s.sessionID)
	}

	// Make sure the session isn't used after this point.
	s.Session = nil

	s.closed = true
}
