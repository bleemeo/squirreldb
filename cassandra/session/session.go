package session

import (
	"github.com/gocql/gocql"
	"strconv"
	"strings"
)

type Options struct {
	Addresses         []string
	ReplicationFactor int
	Keyspace          string
}

type Cassandra struct {
	Session *gocql.Session
}

// New creates a new Cassandra object
func New(options Options) (*Cassandra, error) {
	cluster := gocql.NewCluster(options.Addresses...)
	session, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	session.SetConsistency(gocql.LocalQuorum)

	replicationFactor := strconv.FormatInt(int64(options.ReplicationFactor), 10)
	createKeyspace := createKeyspaceQuery(session, options.Keyspace, replicationFactor)

	if err := createKeyspace.Exec(); err != nil {
		session.Close()
		return nil, err
	}

	cassandra := Cassandra{
		Session: session,
	}

	return &cassandra, nil
}

// Close closes the Cassandra session
func (c *Cassandra) Close() {
	c.Session.Close()
}

// Returns keyspace create query
func createKeyspaceQuery(session *gocql.Session, keyspace, replicationFactor string) *gocql.Query {
	replacer := strings.NewReplacer("$KEYSPACE", keyspace, "$REPLICATION_FACTOR", replicationFactor)
	query := session.Query(replacer.Replace(`
		CREATE KEYSPACE IF NOT EXISTS $KEYSPACE
		WITH REPLICATION = {
			'class': 'SimpleStrategy',
			'replication_factor': $REPLICATION_FACTOR
		};
	`))

	return query
}
