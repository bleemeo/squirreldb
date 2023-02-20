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

var ErrConnectionClosed = errors.New("connection closed")

// Connection wraps a Cassandra connection (wrapper around gocql.ClusterConfig) and allow to acquire
// a (cached) gocql.Session. The session is returned to every call to Session() unless Connection
// think the session is no longer valid. In this case a new session is created on used for
// subsequent Session() call.
type Connection struct {
	l                         sync.Mutex
	logger                    zerolog.Logger
	cluster                   *gocql.ClusterConfig
	currentSessionID          int
	sessionUserCount          map[int]int
	sessions                  map[int]*gocql.Session
	closed                    bool
	cancel                    context.CancelFunc
	wg                        sync.WaitGroup
	wakeRunLoop               chan interface{}
	lastConnectionEstablished time.Time
	lastObservedError         connectError
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

	runCtx, cancel := context.WithCancel(ctx)

	manager := &Connection{
		logger:           logger,
		cluster:          cluster,
		sessionUserCount: make(map[int]int),
		sessions:         make(map[int]*gocql.Session),
		cancel:           cancel,
		wakeRunLoop:      make(chan interface{}),
	}

	manager.wg.Add(1)

	go manager.run(runCtx)

	cluster.ConnectObserver = connectObserver{connection: manager}

	return manager, keyspaceCreated, nil
}

// Session return a *gocql.Session, possibly using a cached one. The returned session should be short-lived.
func (c *Connection) Session() (*SessionWrapper, error) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.closed {
		return nil, ErrConnectionClosed
	}

	if c.sessions[c.currentSessionID] == nil {
		err := c.openSession(true)
		if err != nil {
			return nil, err
		}
	}

	c.sessionUserCount[c.currentSessionID]++

	return &SessionWrapper{connection: c, Session: c.sessions[c.currentSessionID], sessionID: c.currentSessionID}, nil
}

func (c *Connection) openSession(lockAlreadyHeld bool) error {
	session, err := c.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	if !lockAlreadyHeld {
		c.l.Lock()
		defer c.l.Unlock()
	}

	if !c.lastConnectionEstablished.IsZero() && c.sessionUserCount[c.currentSessionID] == 0 {
		c.closeSession(c.currentSessionID)
	}

	c.currentSessionID++
	c.sessions[c.currentSessionID] = session
	c.lastConnectionEstablished = time.Now()
	c.logger.Debug().Int("sessionID", c.currentSessionID).Msg("openned session")

	return nil
}

// Close closes all Session still open. All Session acquired by Session() should be closed before this call.
func (c *Connection) Close() {
	c.l.Lock()

	closed := c.closed
	c.closed = true

	c.l.Unlock()

	if closed {
		return
	}

	c.cancel()
	c.wg.Wait()
}

func (c *Connection) run(ctx context.Context) {
	defer c.wg.Done()

	var lastRunOnce time.Time

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			continue
		case <-ticker.C:
		case <-c.wakeRunLoop:
		}

		if time.Since(lastRunOnce) < 10*time.Second {
			// Avoid any flooding of runOnce. Missing one call to runOnce isn't an issue
			// since we will be re-called at most in one minute.
			continue
		}

		lastRunOnce = time.Now()

		reopenConnection := c.runOnce(ctx)
		for reopenConnection && ctx.Err() == nil {
			err := c.openSession(false)
			if err != nil {
				c.logger.Debug().Err(err).Msg("failed to openSession. Retry in 10 seconds")

				time.Sleep(10 * time.Second)
			} else {
				reopenConnection = false
				c.logger.Info().Msg("Cassandra connection re-established")
			}
		}
	}

	c.shutdown()
}

func (c *Connection) runOnce(ctx context.Context) bool {
	c.l.Lock()
	defer c.l.Unlock()

	if c.lastConnectionEstablished.IsZero() {
		// runOnce never open the connection
		return false
	}

	session := c.sessions[c.currentSessionID]
	if session == nil {
		c.logger.Warn().Int("sessionID", c.currentSessionID).Msg("session is nil in runOnce")

		return false
	}

	c.l.Unlock()

	err := session.Query("SELECT host_id FROM system.local WHERE key='local'").WithContext(ctx).Exec()

	c.l.Lock()

	reopenConnection := false

	if err != nil {
		c.logger.Info().Err(err).Msg("Cassandra connection is no longer valid, reopening one")

		reopenConnection = true
	} else if c.lastObservedError.err != nil && time.Since(c.lastConnectionEstablished) > 15*time.Minute {
		c.logger.Info().
			Err(c.lastObservedError.err).
			Str("HostnameAndPort", c.lastObservedError.hostAndPort).
			Msg("Observed connection and last reconnection is more than 15 minutes old")

		reopenConnection = true
	}

	c.lastObservedError = connectError{}

	return reopenConnection
}

func (c *Connection) shutdown() {
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
