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

	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
)

const retryMaxDelay = 30 * time.Second

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[locks] ", log.LstdFlags)

type CassandraLocks struct {
	session *gocql.Session
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
func New(session *gocql.Session, createdKeySpace bool) (*CassandraLocks, error) {
	var err error

	// Creation of tables concurrently is not possible on Cassandra.
	// But we don't yet have lock, so use random jitter to reduce change of
	// collision.
	// Improve a bit, and make sure the one which created the keyspace
	// try first to create the tables.
	if createdKeySpace {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)

		err = initTable(session)
	} else if !tableExists(session) {
		time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
		debug.Print(1, logger, "created lock tables")

		err = initTable(session)
	}

	if err != nil {
		return nil, err
	}

	locks := &CassandraLocks{
		session: session,
	}

	return locks, nil
}

func initTable(session *gocql.Session) error {
	locksTableCreateQuery := locksTableCreateQuery(session)
	locksTableCreateQuery.Consistency(gocql.All)

	return locksTableCreateQuery.Exec()
}

func tableExists(session *gocql.Session) bool {
	err := session.Query("SELECT name FROM locks WHERE name='test'").Exec()
	return err == nil
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

// SchemaLock return a lock to modify the Cassandra schema
func (c CassandraLocks) SchemaLock() types.TryLocker {
	return c.CreateLock("cassandra-schema", 10*time.Second)
}

// TryLock try to acquire the Lock and return true if acquire
func (l *Lock) TryLock() bool {
	start := time.Now()
	defer func() {
		locksTryLockSeconds.Observe(time.Since(start).Seconds())
	}()

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.acquired {
		locksAlreadyAcquireSeconds.Inc()
		debug.Print(debug.Level1, logger, "lock %s is already acquired by local instance", l.name)

		return false
	}

	var holder string

	locksTableInsertLockQuery := l.locksTableInsertLockQuery()
	locksTableInsertLockQuery.SerialConsistency(gocql.LocalSerial)
	acquired, err := locksTableInsertLockQuery.ScanCAS(nil, &holder, nil)

	if err != nil {
		logger.Printf("Unable to acquire lock: %v", err)
		return false
	}

	if !acquired {
		locksAlreadyAcquireSeconds.Inc()
		debug.Print(debug.Level1, logger, "lock %s is already acquired by %s", l.name, holder)

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

	return true
}

// Lock will call TryLock every 10 seconds until is successed
func (l *Lock) Lock() {
	start := time.Now()
	defer func() {
		locksLockSeconds.Observe(time.Since(start).Seconds())
	}()

	for {
		ok := l.TryLock()
		if ok {
			return
		}

		time.Sleep(10 * time.Second)
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

	start := time.Now()

	locksTableDeleteLockQuery := l.locksTableDeleteLockQuery()

	locksTableDeleteLockQuery.SerialConsistency(gocql.LocalSerial)
	retry.Print(locksTableDeleteLockQuery.Exec, retry.NewExponentialBackOff(retryMaxDelay), logger, "free lock")

	locksUnlockSeconds.Observe(time.Since(start).Seconds())

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
				start := time.Now()

				locksTableUpdateLockQuery := l.locksTableUpdateLockQuery()

				locksTableUpdateLockQuery.SerialConsistency(gocql.LocalSerial)

				err := locksTableUpdateLockQuery.Exec()

				locksRefreshSeconds.Observe(time.Since(start).Seconds())

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
	query := l.c.session.Query(`
		DELETE FROM "locks"
		WHERE name = ?
		IF lock_id = ?
	`, l.name, l.lockID)

	return query
}

// Returns locks table insert lock Query
func (l *Lock) locksTableInsertLockQuery() *gocql.Query {
	query := l.c.session.Query(`
		INSERT INTO locks (name, lock_id, timestamp)
		VALUES (?, ?, toUnixTimestamp(now()))
		IF NOT EXISTS
		USING TTL ?
	`, l.name, l.lockID, int64(l.timeToLive.Seconds()))

	return query
}

// Returns locks table update lock Query
func (l *Lock) locksTableUpdateLockQuery() *gocql.Query {
	query := l.c.session.Query(`
		UPDATE locks USING TTL ?
		SET lock_id = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF lock_id = ?
	`, int64(l.timeToLive.Seconds()), l.lockID, l.name, l.lockID)

	return query
}

// Returns locks table create Query
func locksTableCreateQuery(session *gocql.Session) *gocql.Query {
	query := session.Query(`
		CREATE TABLE IF NOT EXISTS locks (
			name text,
			lock_id text,
			timestamp timestamp,
			PRIMARY KEY (name)
		)
	`)

	return query
}
