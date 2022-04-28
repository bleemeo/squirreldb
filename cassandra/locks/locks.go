package locks

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"squirreldb/logger"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const retryMaxDelay = 30 * time.Second

type CassandraLocks struct {
	session *gocql.Session
	metrics *metrics
}

type Lock struct {
	name       string
	timeToLive time.Duration
	c          CassandraLocks
	lockID     string

	mutex            sync.Mutex
	cond             *sync.Cond // cond use the mutex as Locker
	localTryingCount int
	localLocking     bool
	cassAcquired     bool
	acquired         bool
	wg               sync.WaitGroup
	cancel           context.CancelFunc
}

// New creates a new CassandraLocks object.
func New(reg prometheus.Registerer, session *gocql.Session, createdKeySpace bool) (*CassandraLocks, error) {
	var err error

	// Creation of tables concurrently is not possible on Cassandra.
	// But we don't yet have lock, so use random jitter to reduce change of
	// collision.
	// Improve a bit, and make sure the one which created the keyspace
	// try first to create the tables.
	if createdKeySpace {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond) //nolint:gosec

		err = initTable(session)
	} else if !tableExists(session) {
		time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond) //nolint:gosec
		log.Debug().Msg("Created lock tables")

		err = initTable(session)
	}

	if err != nil {
		return nil, err
	}

	locks := &CassandraLocks{
		session: session,
		metrics: newMetrics(reg),
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
		log.Warn().Msgf("Lock TTL = %v but should be at least 2s or two SquirrelDB may acquire the lock", timeToLive)
	}

	l := &Lock{
		name:       name,
		timeToLive: timeToLive,
		c:          c,
		lockID:     fmt.Sprintf("%s-PID-%d-RND-%d", hostname, os.Getpid(), rand.Intn(65536)), //nolint:gosec
	}

	l.cond = sync.NewCond(&l.mutex)

	return l
}

// tryLock try to acquire the Lock and return true if acquire.
func (l *Lock) tryLock(ctx context.Context) bool {
	if l.acquired {
		panic("impossible case: tryLock should never be called when lock is acquired")
	}

	if l.cassAcquired {
		panic("impossible case: tryLock should never be called when Cassandra lock is acquired")
	}

	var holder string

	start := time.Now()

	locksTableInsertLockQuery := l.locksTableInsertLockQuery().WithContext(ctx)
	locksTableInsertLockQuery.SerialConsistency(gocql.LocalSerial)

	acquired, err := locksTableInsertLockQuery.ScanCAS(&holder)

	l.c.metrics.CassandraQueriesSeconds.WithLabelValues("lock").Observe(time.Since(start).Seconds())

	if err != nil {
		log.Err(err).Msgf("Unable to acquire lock")

		// Be careful, it indeed a pointer and we took the address...
		// We have to guess that gocql will return a pointer to this error
		// or errors.As won't work :(
		var unused *gocql.RequestErrWriteTimeout

		// The context *may* be canceled after C* took the look. So try to unlock it.
		// Also if C* reply with timeout, we actually don't known if write succeeded or not.
		if ctx.Err() != nil || errors.As(err, &unused) {
			locksTableDeleteLockQuery := l.locksTableDeleteLockQuery()

			locksTableDeleteLockQuery.SerialConsistency(gocql.LocalSerial)

			cassStart := time.Now()

			_, err = locksTableDeleteLockQuery.ScanCAS(nil)
			if err != nil {
				log.Debug().Err(err).Msg("Opportunistic unlock failed")
			}

			l.c.metrics.CassandraQueriesSeconds.WithLabelValues("unlock").Observe(time.Since(cassStart).Seconds())
		}

		return false
	}

	if !acquired {
		log.Debug().Msgf("Lock %s is already acquired by %s (i'm %s)", l.name, holder, l.lockID)

		return false
	}

	l.acquired = true
	l.cassAcquired = true

	subCtx, cancel := context.WithCancel(context.Background())

	l.cancel = cancel

	l.wg.Add(1)

	go func() {
		defer logger.ProcessPanic()
		defer l.wg.Done()

		l.updateLock(subCtx)
	}()

	return true
}

// TryLock will try to acquire the lock until ctx expire.
// If retryDelay is non-zero, retry acquire the lock after a delay which is capped by retryDelay.
func (l *Lock) TryLock(ctx context.Context, retryDelay time.Duration) bool {
	start := time.Now()

	l.c.metrics.PendingLock.Inc()

	defer l.c.metrics.PendingLock.Dec()
	defer func() {
		l.c.metrics.LocksLockSeconds.Observe(time.Since(start).Seconds())
	}()

	currentDelay := retryDelay / 100

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if (l.acquired || l.localLocking) && currentDelay == 0 {
		return false
	}

	l.localTryingCount++

	defer func() {
		l.localTryingCount--
	}()

	for l.acquired || l.localLocking {
		l.cond.Wait()
	}

	if l.cassAcquired {
		l.acquired = true

		return true
	}

	l.localLocking = true

	defer func() {
		l.localLocking = false

		l.cond.Signal()
	}()

	if ctx.Err() != nil {
		return false
	}

	for {
		ok := l.tryLock(ctx)
		if ok {
			l.c.metrics.LocksLockSuccess.Inc()

			return true
		}

		if currentDelay == 0 {
			return false
		}

		jitter := currentDelay.Seconds() * (1 + rand.Float64()/5) //nolint:gosec
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

	l.acquired = false

	if l.localTryingCount > 0 {
		l.cond.Signal()

		return
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
			log.Warn().Msgf("Unable to clear lock %s, this should mean someone took the lock while I held it", l.name)
		}

		l.c.metrics.CassandraQueriesSeconds.WithLabelValues("unlock").Observe(time.Since(cassStart).Seconds())

		return err //nolint:wrapcheck
	},
		retry.NewExponentialBackOff(context.Background(), retryMaxDelay), "free lock")

	l.c.metrics.LocksUnlockSeconds.Observe(time.Since(start).Seconds())

	l.cassAcquired = false

	l.cond.Signal()
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

				l.c.metrics.CassandraQueriesSeconds.WithLabelValues("refresh").Observe(time.Since(start).Seconds())

				return err //nolint:wrapcheck
			}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
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
		UPDATE locks
		USING TTL ?
		SET lock_id = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF lock_id in (?, NULL)
	`, int64(l.timeToLive.Seconds()), l.lockID, l.name, l.lockID)

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
