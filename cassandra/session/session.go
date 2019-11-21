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

// New creates a new Cassandra object
func New(options Options) (*gocql.Session, error) {
	cluster := gocql.NewCluster(options.Addresses...)
	session, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	session.SetConsistency(gocql.LocalQuorum)

	replicationFactor := strconv.FormatInt(int64(options.ReplicationFactor), 10)
	keyspaceCreateQuery := keyspaceCreateQuery(session, options.Keyspace, replicationFactor)

	if err := keyspaceCreateQuery.Exec(); err != nil {
		session.Close()
		return nil, err
	}

	return session, nil
}

// Returns keyspace create query
func keyspaceCreateQuery(session *gocql.Session, keyspace, replicationFactor string) *gocql.Query {
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
