package connection

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"squirreldb/config"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

// Connection wraps a Cassandra connection (wrapper around gocql.ClusterConfig) and allow to acquire
// a (cached) gocql.Session. The session is returned to every call to Session() unless Connection
// think the session is no longer valid. In this case a new session is created on used for
// subsequent Session() call.
type Connection struct {
	l                sync.Mutex
	logger           zerolog.Logger
	cluster          *gocql.ClusterConfig
	currentSessionID int
	sessionUserCount map[int]int
	sessions         map[int]*gocql.Session
}

// New creates a new Cassandra session and return if the keyspace was create by this instance.
func New(ctx context.Context, options config.Cassandra, logger zerolog.Logger) (*Connection, bool, error) {
	cluster := gocql.NewCluster(options.Addresses...)
	cluster.Timeout = 5 * time.Second
	cluster.Consistency = gocql.All

	if options.CertPath != "" || options.KeyPath != "" || options.CAPath != "" {
		cluster.SslOpts = &gocql.SslOptions{
			CertPath:               options.CertPath,
			KeyPath:                options.KeyPath,
			CaPath:                 options.CAPath,
			EnableHostVerification: options.EnableHostVerification,
		}
	}

	if options.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: options.Username,
			Password: options.Password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, false, fmt.Errorf("create session: %w", err)
	}

	keyspaceCreated := false

	if !keyspaceExists(ctx, session, options.Keyspace) {
		// Not sure if we are allowed to create keyspace concurrently. Add a random jitter to
		// reduce change of concurrent keyspace creation
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) //nolint:gosec

		replicationFactor := strconv.FormatInt(int64(options.ReplicationFactor), 10)

		err = keyspaceCreate(ctx, session, options.Keyspace, replicationFactor)

		//nolint:gocritic
		if errors.Is(err, &gocql.RequestErrAlreadyExists{}) {
			keyspaceCreated = false
		} else if err != nil {
			session.Close()

			return nil, false, fmt.Errorf("create keyspace: %w", err)
		} else {
			keyspaceCreated = true
			logger.Debug().Msgf("Keyspace %s created", options.Keyspace)
		}
	}

	session.Close()

	cluster.Keyspace = options.Keyspace
	cluster.Consistency = gocql.LocalQuorum

	manager := &Connection{
		logger:           logger,
		cluster:          cluster,
		sessionUserCount: make(map[int]int),
		sessions:         make(map[int]*gocql.Session),
	}

	cluster.ConnectObserver = connectObserver{connection: manager}

	return manager, keyspaceCreated, nil
}

// Session return a *gocql.Session, possibly using a cached one. The returned session should be short-lived.
func (c *Connection) Session() (*SessionWrapper, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.sessions[c.currentSessionID] == nil {
		session, err := c.cluster.CreateSession()
		if err != nil {
			return nil, fmt.Errorf("create session: %w", err)
		}

		c.currentSessionID++
		c.sessions[c.currentSessionID] = session
		c.logger.Debug().Int("sessionID", c.currentSessionID).Msg("openned session")
	}

	c.sessionUserCount[c.currentSessionID]++

	return &SessionWrapper{connection: c, Session: c.sessions[c.currentSessionID], sessionID: c.currentSessionID}, nil
}

// Close closes all Session still open. All Session acquired by Session() should be closed before this call.
func (c *Connection) Close() {
	c.l.Lock()
	defer c.l.Unlock()

	if _, ok := c.sessions[c.currentSessionID]; ok {
		c.closeSession(c.currentSessionID)
	}

	for sessionID := range c.sessions {
		if c.sessionUserCount[sessionID] > 0 {
			c.logger.Warn().
				Int("sessionID", sessionID).
				Int("userCount", c.sessionUserCount[sessionID]).
				Msg("session not closed by user")
		}

		c.closeSession(sessionID)
	}

	c.sessions = make(map[int]*gocql.Session)
	c.sessionUserCount = make(map[int]int)
	c.currentSessionID = 0
}

func (c *Connection) wrapperCloseSession(sessionID int) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.sessionUserCount[sessionID] == 0 {
		c.logger.Error().Int("sessionID", sessionID).Msg("Trying to close a gocql.Session which is already closed")

		return
	}

	c.sessionUserCount[sessionID]--

	if sessionID != c.currentSessionID && c.sessionUserCount[sessionID] == 0 {
		c.closeSession(sessionID)
	}
}

func (c *Connection) closeSession(sessionID int) {
	if c.sessions[sessionID] == nil {
		c.logger.Error().Int("sessionID", sessionID).Msg("Trying to close a non-existing session")

		return
	}

	c.logger.Debug().Int("sessionID", sessionID).Msg("closing session")

	c.sessions[sessionID].Close()
	delete(c.sessions, sessionID)
	delete(c.sessionUserCount, sessionID)
}

func keyspaceExists(ctx context.Context, session *gocql.Session, keyspace string) bool {
	var name string

	err := session.Query(
		"SELECT keyspace_name FROM system_schema.keyspaces where keyspace_name = ?",
		keyspace,
	).WithContext(ctx).Scan(&name)

	return err == nil
}

// Returns keyspace create query.
func keyspaceCreate(ctx context.Context, session *gocql.Session, keyspace, replicationFactor string) error {
	replacer := strings.NewReplacer("$KEYSPACE", keyspace, "$REPLICATION_FACTOR", replicationFactor)
	query := session.Query(replacer.Replace(`
		CREATE KEYSPACE $KEYSPACE
		WITH REPLICATION = {
			'class': 'SimpleStrategy',
			'replication_factor': $REPLICATION_FACTOR
		};
	`)).WithContext(ctx)

	return query.Exec()
}
