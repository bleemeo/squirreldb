package locks

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"

	"squirreldb/retry"
	"squirreldb/types"
	"strings"
)

const tableName = "locks"

const retryMaxDelay = 30 * time.Second

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[locks] ", log.LstdFlags)

type CassandraLocks struct {
	session    *gocql.Session
	locksTable string

	instance types.Instance
}

type Lock struct {
	name       string
	timeToLive time.Duration
	c          CassandraLocks

	mutex    sync.Mutex
	acquired bool
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

// New creates a new CassandraLocks object
func New(session *gocql.Session, keyspace string, instance types.Instance) (*CassandraLocks, error) {
	locksTable := keyspace + "." + tableName

	locksTableCreateQuery := locksTableCreateQuery(session, locksTable)

	if err := locksTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	locks := &CassandraLocks{
		session:    session,
		locksTable: locksTable,
		instance:   instance,
	}

	return locks, nil
}

// CreateLock return a Locker for given name.
// This Locker will be common across all SquirrelDB instance connected to the
// same Cassandra.
// If the instance holder crash, the lock will be released after timeToLive
func (c CassandraLocks) CreateLock(name string, timeToLive time.Duration) types.TryLocker {
	return &Lock{
		name:       name,
		timeToLive: timeToLive,
		c:          c,
	}
}

// TryLock try to acquire the Lock and return true if acquire
func (l *Lock) TryLock() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.acquired {
		return false
	}

	locksTableInsertLockQuery := l.c.locksTableInsertLockQuery(l.name, int64(l.timeToLive.Seconds()))
	locksTableInsertLockQuery.SerialConsistency(gocql.LocalSerial)
	applied, err := locksTableInsertLockQuery.ScanCAS(nil, nil, nil, nil)

	if err != nil {
		logger.Printf("Unable to acquire lock: %v", err)
		return false
	}

	l.acquired = true

	ctx, cancel := context.WithCancel(context.Background())

	l.cancel = cancel

	l.wg.Add(1)
	go func() { // nolint: wsl
		defer l.wg.Done()
		l.updateLock(ctx)
	}()

	return applied
}

// Lock will call TryLock every minute until is successed
func (l *Lock) Lock() {
	for {
		ok := l.TryLock()
		if ok {
			return
		}

		time.Sleep(time.Minute)
	}
}

// Unlock free a Lock
func (l *Lock) Unlock() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.acquired {
		panic("unlock of unlocked mutex")
	}

	l.cancel()
	l.wg.Wait()

	locksTableDeleteLockQuery := l.c.locksTableDeleteLockQuery(l.name)

	locksTableDeleteLockQuery.SerialConsistency(gocql.LocalSerial)
	retry.Print(locksTableDeleteLockQuery.Exec, retry.NewExponentialBackOff(retryMaxDelay), logger, "free lock")

	l.acquired = false
}

func (l *Lock) updateLock(ctx context.Context) {
	interval := l.timeToLive / 2
	ticker := time.NewTicker(interval)

	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ticker.C:
			retry.Print(func() error {
				return l.c.Update(l.name, int64(l.timeToLive.Seconds()))
			}, retry.NewExponentialBackOff(retryMaxDelay), logger,
				"refresh lock "+l.name,
			)
		case <-ctx.Done():
			return
		}
	}
}

// Update updates a lock
func (c *CassandraLocks) Update(name string, timeToLive int64) error {
	locksTableUpdateLockQuery := c.locksTableUpdateLockQuery(name, timeToLive)

	locksTableUpdateLockQuery.SerialConsistency(gocql.LocalSerial)

	err := locksTableUpdateLockQuery.Exec()

	return err
}

// Returns locks table delete lock Query
func (c *CassandraLocks) locksTableDeleteLockQuery(name string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		DELETE FROM $LOCKS_TABLE
		WHERE name = ?
		IF EXISTS
	`), name)

	return query
}

// Returns locks table insert lock Query
func (c *CassandraLocks) locksTableInsertLockQuery(name string, timeToLive int64) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		INSERT INTO $LOCKS_TABLE (name, instance_hostname, instance_uuid, timestamp)
		VALUES (?, ?, ?, toUnixTimestamp(now()))
		IF NOT EXISTS
		USING TTL ?
	`), name, c.instance.Hostname, c.instance.UUID, timeToLive)

	return query
}

// Returns locks table update lock Query
func (c *CassandraLocks) locksTableUpdateLockQuery(name string, timeToLive int64) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", c.locksTable)
	query := c.session.Query(replacer.Replace(`
		UPDATE $LOCKS_TABLE USING TTL ?
		SET instance_hostname = ?, instance_uuid = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF EXISTS
	`), timeToLive, c.instance.Hostname, c.instance.UUID, name)

	return query
}

// Returns locks table create Query
func locksTableCreateQuery(session *gocql.Session, locksTable string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", locksTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $LOCKS_TABLE (
			name text,
			instance_hostname text,
			instance_uuid uuid,
			timestamp timestamp,
			PRIMARY KEY (name)
		)
	`))

	return query
}
