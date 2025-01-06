// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
