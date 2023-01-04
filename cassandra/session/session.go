package session

import (
	"errors"
	"fmt"
	"math/rand"
	"squirreldb/config"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

// New creates a new Cassandra session and return if the keyspace was create by this instance.
func New(options config.Cassandra, logger zerolog.Logger) (*gocql.Session, bool, error) {
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

	if !keyspaceExists(session, options.Keyspace) {
		// Not sure if we are allowed to create keyspace concurrently. Add a random jitter to
		// reduce change of concurrent keyspace creation
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) //nolint:gosec

		replicationFactor := strconv.FormatInt(int64(options.ReplicationFactor), 10)
		query := keyspaceCreateQuery(session, options.Keyspace, replicationFactor)

		err = query.Exec()

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

	finalSession, err := cluster.CreateSession()
	if err != nil {
		return nil, false, fmt.Errorf("create session: %w", err)
	}

	return finalSession, keyspaceCreated, nil
}

func keyspaceExists(session *gocql.Session, keyspace string) bool {
	var name string

	err := session.Query("SELECT keyspace_name FROM system_schema.keyspaces where keyspace_name = ?", keyspace).Scan(&name)

	return err == nil
}

// Returns keyspace create query.
func keyspaceCreateQuery(session *gocql.Session, keyspace, replicationFactor string) *gocql.Query {
	replacer := strings.NewReplacer("$KEYSPACE", keyspace, "$REPLICATION_FACTOR", replicationFactor)
	query := session.Query(replacer.Replace(`
		CREATE KEYSPACE $KEYSPACE
		WITH REPLICATION = {
			'class': 'SimpleStrategy',
			'replication_factor': $REPLICATION_FACTOR
		};
	`))

	return query
}
