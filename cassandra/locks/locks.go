package locks

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"squirreldb/cassandra/connection"
	"squirreldb/logger"
	"squirreldb/retry"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

const (
	retryMaxDelay = 30 * time.Second
	unlockTimeout = 15 * time.Second
)

type CassandraLocks struct {
	connection *connection.Connection
	metrics    *metrics
	logger     zerolog.Logger
}

type Lock struct {
	name       string
	timeToLive time.Duration
	c          CassandraLocks
	lockID     string
	logger     zerolog.Logger

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
func New(
	ctx context.Context,
	reg prometheus.Registerer,
	connection *connection.Connection,
	createdKeySpace bool,
	logger zerolog.Logger,
) (*CassandraLocks, error) {
	var err error

	// Creation of tables concurrently is not possible on Cassandra.
	// But we don't yet have lock, so use random jitter to reduce change of
	// collision.
	// Improve a bit, and make sure the one which created the keyspace
	// try first to create the tables.
	if createdKeySpace {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond) //nolint:gosec

		err = initTable(ctx, connection)
	} else if !tableExists(ctx, connection) {
		time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond) //nolint:gosec
		logger.Debug().Msg("Created lock tables")

		err = initTable(ctx, connection)
	}

	if err != nil {
		return nil, err
	}

	locks := &CassandraLocks{
		connection: connection,
		metrics:    newMetrics(reg),
		logger:     logger,
	}

	return locks, nil
}

func initTable(ctx context.Context, connection *connection.Connection) error {
	return locksTableCreate(ctx, connection)
}

func tableExists(ctx context.Context, connection *connection.Connection) bool {
	session, err := connection.Session()
	if err != nil {
		return false
	}

	defer session.Close()

	err = session.Query("SELECT name FROM locks WHERE name='test'").WithContext(ctx).Exec()

	return err == nil
}

// CreateLock return a Locker for given name.
// This Locker will be common across all SquirrelDB instance connected to the
// same Cassandra.
// If the instance holder crash, the lock will be released after timeToLive.
func (c CassandraLocks) CreateLock(name string, timeToLive time.Duration) types.TryLocker {
	hostname, _ := os.Hostname()

	if timeToLive < 3*time.Second {
		c.logger.Warn().Msgf("Lock TTL = %v but should be at least 3s or two SquirrelDB may acquire the lock", timeToLive)
	}

	l := &Lock{
		name:       name,
		timeToLive: timeToLive,
		c:          c,
		lockID:     fmt.Sprintf("%s-PID-%d-RND-%d", hostname, os.Getpid(), rand.Intn(65536)), //nolint:gosec
		logger:     c.logger,
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

	start := time.Now()

	holder, acquired, err := l.locksTableInsertLock(ctx)

	l.c.metrics.CassandraQueriesSeconds.WithLabelValues("lock").Observe(time.Since(start).Seconds())

	if err != nil {
		l.logger.Err(err).Msgf("Unable to acquire lock")

		// Be careful, it indeed a pointer and we took the address...
		// We have to guess that gocql will return a pointer to this error
		// or errors.As won't work :(
		var unused *gocql.RequestErrWriteTimeout

		// The context *may* be canceled after C* took the look. So try to unlock it.
		// Also if C* reply with timeout, we actually don't known if write succeeded or not.
		if ctx.Err() != nil || errors.As(err, &unused) {
			cassStart := time.Now()

			subCtx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
			defer cancel()

			// It will try to delete the lock even if main context is cancelled, so use
			// a new background context for this.
			_, err = l.locksTableDeleteLock(subCtx) //nolint: contextcheck
			if err != nil {
				l.logger.Debug().Err(err).Msg("Opportunistic unlock failed")
			}

			l.c.metrics.CassandraQueriesSeconds.WithLabelValues("unlock").Observe(time.Since(cassStart).Seconds())
		}

		return false
	}

	if !acquired {
		l.logger.Debug().Msgf("tryLock: can't acquire lock %s as it is held by %s (i'm %s)", l.name, holder, l.lockID)

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

	retry.Print(func() error {
		cassStart := time.Now()

		ctx, cancel := context.WithTimeout(context.Background(), unlockTimeout)
		defer cancel()

		applied, err := l.locksTableDeleteLock(ctx)
		if err == nil && !applied {
			l.logger.Warn().Msgf("Unable to clear lock %s, this should mean someone took the lock while I held it", l.name)
		}

		l.c.metrics.CassandraQueriesSeconds.WithLabelValues("unlock").Observe(time.Since(cassStart).Seconds())

		return err //nolint:wrapcheck
	},
		retry.NewExponentialBackOff(context.Background(), retryMaxDelay),
		l.logger,
		"free lock",
	)

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

				err := l.locksTableUpdateLock(ctx)

				l.c.metrics.CassandraQueriesSeconds.WithLabelValues("refresh").Observe(time.Since(start).Seconds())

				return err //nolint:wrapcheck
			}, retry.NewExponentialBackOff(ctx, retryMaxDelay),
				l.logger,
				"refresh lock "+l.name,
			)
		case <-ctx.Done():
			return
		}
	}
}

func (l *Lock) locksTableDeleteLock(ctx context.Context) (bool, error) {
	session, err := l.c.connection.Session()
	if err != nil {
		return false, err
	}

	defer session.Close()

	query := session.Query(`
		DELETE FROM "locks"
		WHERE name = ?
		IF lock_id = ?`,
		l.name, l.lockID,
	).SerialConsistency(gocql.LocalSerial).WithContext(ctx)

	return query.ScanCAS(nil)
}

func (l *Lock) locksTableInsertLock(ctx context.Context) (string, bool, error) {
	var holder string

	session, err := l.c.connection.Session()
	if err != nil {
		return "", false, err
	}

	defer session.Close()

	query := session.Query(`
		UPDATE locks
		USING TTL ?
		SET lock_id = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF lock_id in (?, NULL)`,
		int64(l.timeToLive.Seconds()), l.lockID, l.name, l.lockID,
	).SerialConsistency(gocql.LocalSerial).WithContext(ctx)

	acquired, err := query.ScanCAS(&holder)

	return holder, acquired, err
}

// Returns locks table update lock Query.
func (l *Lock) locksTableUpdateLock(ctx context.Context) error {
	session, err := l.c.connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		UPDATE locks USING TTL ?
		SET lock_id = ?, timestamp = toUnixTimestamp(now())
		WHERE name = ?
		IF lock_id = ?`,
		int64(l.timeToLive.Seconds()), l.lockID, l.name, l.lockID,
	).SerialConsistency(gocql.LocalSerial).WithContext(ctx)

	return query.Exec()
}

// Returns locks table create Query.
func locksTableCreate(ctx context.Context, connection *connection.Connection) error {
	session, err := connection.Session()
	if err != nil {
		return err
	}

	defer session.Close()

	query := session.Query(`
		CREATE TABLE IF NOT EXISTS locks (
			name text,
			lock_id text,
			timestamp timestamp,
			PRIMARY KEY (name)
		)
		WITH memtable_flush_period_in_ms = 60000`,
	).Consistency(gocql.All).WithContext(ctx)

	return query.Exec()
}
