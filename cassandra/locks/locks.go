package locks

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
}

type Lock struct {
	name       string
	timeToLive time.Duration
	c          CassandraLocks
	lockID     string

	mutex    sync.Mutex
	acquired bool
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

// New creates a new CassandraLocks object
func New(session *gocql.Session, keyspace string) (*CassandraLocks, error) {
	locksTable := keyspace + "." + tableName

	locksTableCreateQuery := locksTableCreateQuery(session, locksTable)

	if err := locksTableCreateQuery.Exec(); err != nil {
		return nil, err
	}

	locks := &CassandraLocks{
		session:    session,
		locksTable: locksTable,
	}

	return locks, nil
}

// CreateLock return a Locker for given name.
// This Locker will be common across all SquirrelDB instance connected to the
// same Cassandra.
// If the instance holder crash, the lock will be released after timeToLive
func (c CassandraLocks) CreateLock(name string, timeToLive time.Duration) types.TryLocker {
	hostname, _ := os.Hostname()

	return &Lock{
		name:       name,
		timeToLive: timeToLive,
		c:          c,
		lockID:     fmt.Sprintf("%s-PID-%d-RND-%d", hostname, os.Getpid(), rand.Intn(65536)),
	}
}

// TryLock try to acquire the Lock and return true if acquire
func (l *Lock) TryLock() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.acquired {
		return false
	}

	locksTableInsertLockQuery := l.locksTableInsertLockQuery()
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

	locksTableDeleteLockQuery := l.locksTableDeleteLockQuery()

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
				locksTableUpdateLockQuery := l.locksTableUpdateLockQuery()

				locksTableUpdateLockQuery.SerialConsistency(gocql.LocalSerial)

				err := locksTableUpdateLockQuery.Exec()

				return err
			}, retry.NewExponentialBackOff(retryMaxDelay), logger,
				"refresh lock "+l.name,
			)
		case <-ctx.Done():
			return
		}
	}
}

// Returns locks table delete lock Query
func (l *Lock) locksTableDeleteLockQuery() *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", l.c.locksTable)
	query := l.c.session.Query(replacer.Replace(`
		DELETE FROM $LOCKS_TABLE
		WHERE name = ?
		IF lock_id = ?
	`), l.name, l.lockID)

	return query
}

// Returns locks table insert lock Query
func (l *Lock) locksTableInsertLockQuery() *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", l.c.locksTable)
	query := l.c.session.Query(replacer.Replace(`
		INSERT INTO $LOCKS_TABLE (name, lock_id, timestamp)
		VALUES (?, ?, toUnixTimestamp(now()))
		IF NOT EXISTS
		USING TTL ?
	`), l.name, l.lockID, int64(l.timeToLive.Seconds()))

	return query
}

// Returns locks table update lock Query
func (l *Lock) locksTableUpdateLockQuery() *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", l.c.locksTable)
	query := l.c.session.Query(replacer.Replace(`
		UPDATE $LOCKS_TABLE USING TTL ?
		SET lock_id = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF lock_id = ?
	`), int64(l.timeToLive.Seconds()), l.lockID, l.name, l.lockID)

	return query
}

// Returns locks table create Query
func locksTableCreateQuery(session *gocql.Session, locksTable string) *gocql.Query {
	replacer := strings.NewReplacer("$LOCKS_TABLE", locksTable)
	query := session.Query(replacer.Replace(`
		CREATE TABLE IF NOT EXISTS $LOCKS_TABLE (
			name text,
			lock_id text,
			timestamp timestamp,
			PRIMARY KEY (name)
		)
	`))

	return query
}
