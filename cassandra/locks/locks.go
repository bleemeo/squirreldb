package locks

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"squirreldb/debug"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/gocql/gocql"
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

// New creates a new CassandraLocks object.
func New(session *gocql.Session, createdKeySpace bool) (*CassandraLocks, error) {
	var err error

	// Creation of tables concurrently is not possible on Cassandra.
	// But we don't yet have lock, so use random jitter to reduce change of
	// collision.
	// Improve a bit, and make sure the one which created the keyspace
	// try first to create the tables.
	if createdKeySpace {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond) // nolint: gosec

		err = initTable(session)
	} else if !tableExists(session) {
		time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond) // nolint: gosec
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
// If the instance holder crash, the lock will be released after timeToLive.
func (c CassandraLocks) CreateLock(name string, timeToLive time.Duration) types.TryLocker {
	hostname, _ := os.Hostname()

	if timeToLive < 2*time.Second {
		logger.Printf("Warning: lock TTL = %v but should be at least 2s or two SquirrelDB may acquire the lock", timeToLive)
	}

	return &Lock{
		name:       name,
		timeToLive: timeToLive,
		c:          c,
		lockID:     fmt.Sprintf("%s-PID-%d-RND-%d", hostname, os.Getpid(), rand.Intn(65536)), // nolint: gosec
	}
}

// tryLock try to acquire the Lock and return true if acquire.
func (l *Lock) tryLock(ctx context.Context) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.acquired {
		debug.Print(debug.Level1, logger, "lock %s is already acquired by local instance", l.name)

		return false
	}

	var holder string

	start := time.Now()

	locksTableInsertLockQuery := l.locksTableInsertLockQuery().WithContext(ctx)
	locksTableInsertLockQuery.SerialConsistency(gocql.LocalSerial)
	acquired, err := locksTableInsertLockQuery.ScanCAS(nil, &holder, nil)

	cassandraQueriesSeconds.WithLabelValues("lock").Observe(time.Since(start).Seconds())

	if err != nil {
		logger.Printf("Unable to acquire lock: %v", err)

		return false
	}

	if !acquired {
		debug.Print(debug.Level1, logger, "lock %s is already acquired by %s", l.name, holder)

		return false
	}

	l.acquired = true

	subCtx, cancel := context.WithCancel(context.Background())

	l.cancel = cancel

	l.wg.Add(1)
	go func() { // nolint: wsl
		defer l.wg.Done()
		l.updateLock(subCtx)
	}()

	return true
}

// TryLock will try to acquire the lock until ctx expire.
// If retryDelay is non-zero, retry acquire the lock after a delay which is capped by retryDelay.
func (l *Lock) TryLock(ctx context.Context, retryDelay time.Duration) bool {
	start := time.Now()

	pendingLock.Inc()

	defer pendingLock.Dec()
	defer func() {
		locksLockSeconds.Observe(time.Since(start).Seconds())
	}()

	currentDelay := retryDelay / 100

	for {
		ok := l.tryLock(ctx)
		if ok {
			locksLockSuccess.Inc()

			return true
		}

		if currentDelay == 0 {
			return false
		}

		jitter := currentDelay.Seconds() * (1 + rand.Float64()/5) // nolint: gosec
		select {
		case <-time.After(time.Duration(jitter) * time.Second):
		case <-ctx.Done():
			return false
		}

		currentDelay *= 2
		if currentDelay > retryDelay {
			currentDelay = retryDelay
		}
	}
}

// Lock will call LockCtx with context.Background().
func (l *Lock) Lock() {
	l.TryLock(context.Background(), 30*time.Second)
}

// Unlock free a Lock.
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
	retry.Print(func() error {
		cassStart := time.Now()

		applied, err := locksTableDeleteLockQuery.ScanCAS(nil)
		if err == nil && !applied {
			logger.Printf("Unable to clear lock %s, this should mean someone took the lock while I held it", l.name)
		}

		cassandraQueriesSeconds.WithLabelValues("unlock").Observe(time.Since(cassStart).Seconds())

		return err // nolint: wrapcheck
	},
		retry.NewExponentialBackOff(context.Background(), retryMaxDelay), logger, "free lock")

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

				cassandraQueriesSeconds.WithLabelValues("refresh").Observe(time.Since(start).Seconds())

				return err // nolint: wrapcheck
			}, retry.NewExponentialBackOff(ctx, retryMaxDelay), logger,
				"refresh lock "+l.name,
			)
		case <-ctx.Done():
			return
		}
	}
}

// Returns locks table delete lock Query.
func (l *Lock) locksTableDeleteLockQuery() *gocql.Query {
	query := l.c.session.Query(`
		DELETE FROM "locks"
		WHERE name = ?
		IF lock_id = ?
	`, l.name, l.lockID)

	return query
}

// Returns locks table insert lock Query.
func (l *Lock) locksTableInsertLockQuery() *gocql.Query {
	query := l.c.session.Query(`
		INSERT INTO locks (name, lock_id, timestamp)
		VALUES (?, ?, toUnixTimestamp(now()))
		IF NOT EXISTS
		USING TTL ?
	`, l.name, l.lockID, int64(l.timeToLive.Seconds()))

	return query
}

// Returns locks table update lock Query.
func (l *Lock) locksTableUpdateLockQuery() *gocql.Query {
	query := l.c.session.Query(`
		UPDATE locks USING TTL ?
		SET lock_id = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF lock_id = ?
	`, int64(l.timeToLive.Seconds()), l.lockID, l.name, l.lockID)

	return query
}

// Returns locks table create Query.
func locksTableCreateQuery(session *gocql.Session) *gocql.Query {
	query := session.Query(`
		CREATE TABLE IF NOT EXISTS locks (
			name text,
			lock_id text,
			timestamp timestamp,
			PRIMARY KEY (name)
		)
		WITH memtable_flush_period_in_ms = 60000
	`)

	return query
}
