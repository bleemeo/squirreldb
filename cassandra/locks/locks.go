// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package locks

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/cassandra/connection"
	"github.com/bleemeo/squirreldb/logger"
	"github.com/bleemeo/squirreldb/retry"
	"github.com/bleemeo/squirreldb/types"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

const (
	retryMaxDelay = 30 * time.Second
	unlockTimeout = 15 * time.Second
	blockLockID   = "this-lock-is-blocked"
)

var errBlockExpired = errors.New("unable to clear lock, this should mean the block TTL expired")

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
	readOnly bool,
	logger zerolog.Logger,
) (*CassandraLocks, error) {
	var err error

	if readOnly {
		logger.Debug().Msg("Read-only mode is activated. Not trying to create tables and assuming they exist")
	} else {
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
	session, err := connection.SessionReadOnly()
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
func (l *Lock) tryLock(ctx context.Context) bool { //nolint:contextcheck
	if l.acquired {
		panic("impossible case: tryLock should never be called when lock is acquired")
	}

	if l.cassAcquired {
		panic("impossible case: tryLock should never be called when Cassandra lock is acquired")
	}

	acquired, holder := l.tryLockCassandra(ctx, l.lockID, l.timeToLive)
	if !acquired {
		l.logger.
			Debug().
			Str("lock-name", l.name).
			Msgf("tryLock: can't acquire lock %s as it is held by %s (i'm %s)", l.name, holder, l.lockID)

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

// tryLockCassandra try to acquire the Lock on Cassandra.
func (l *Lock) tryLockCassandra(ctx context.Context, lockID string, ttl time.Duration) (bool, string) {
	start := time.Now()

	holder, acquired, err := l.locksTableInsertLock(ctx, lockID, ttl)

	l.c.metrics.CassandraQueriesSeconds.WithLabelValues("lock").Observe(time.Since(start).Seconds())

	if err != nil {
		l.logger.Err(err).Str("lock-name", l.name).Msgf("Unable to acquire lock")

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
			_, err = l.locksTableDeleteLock(subCtx, lockID) //nolint: contextcheck
			if err != nil {
				l.logger.Debug().Err(err).Str("lock-name", l.name).Msg("Opportunistic unlock failed")
			}

			l.c.metrics.CassandraQueriesSeconds.WithLabelValues("unlock").Observe(time.Since(cassStart).Seconds())
		}

		return false, ""
	}

	return acquired, holder
}

// BlockLock will block this locks on Cassandra for at least TTL.
// This method is mostly taking the lock, but in a way that both refreshing it or unlocking it
// could be done by any SquirrelDB in the cluster.
// It's usage is to temporary block a process that use a lock, for example by calling a curl on dedicated URL.
// Refreshing the lock also use this same function, so that function either take the lock or if already taken
// (by a previous call to BlockLock) it will refresh it.
func (l *Lock) BlockLock(ctx context.Context, blockTTL time.Duration) error {
	for ctx.Err() == nil {
		acquired, _ := l.tryLockCassandra(ctx, blockLockID, blockTTL)

		if acquired {
			break
		}

		jitter := 1 + rand.Float64()/5 //nolint:gosec
		select {
		case <-time.After(time.Duration(jitter) * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// UnblockLock will unblock a lock previously blocked by BlockLock.
func (l *Lock) UnblockLock(ctx context.Context) error {
	applied, err := l.locksTableDeleteLock(ctx, blockLockID)
	if err == nil && !applied {
		return errBlockExpired
	}

	return err
}

// BlockStatus returns whether the lock is block and the remaining TTL duration.
func (l *Lock) BlockStatus(ctx context.Context) (bool, time.Duration, error) {
	holder, ttl, err := l.locksReadLock(ctx)
	if err != nil {
		return false, 0, err
	}

	if holder == blockLockID {
		return true, ttl, nil
	}

	return false, 0, nil
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

		applied, err := l.locksTableDeleteLock(ctx, l.lockID)
		if err == nil && !applied {
			l.logger.
				Warn().
				Str("lock-name", l.name).
				Msgf("Unable to clear lock %s, this should mean someone took the lock while I held it", l.name)
		}

		l.c.metrics.CassandraQueriesSeconds.WithLabelValues("unlock").Observe(time.Since(cassStart).Seconds())

		return err //nolint:wrapcheck
	},
		retry.NewExponentialBackOff(context.Background(), retryMaxDelay),
		l.logger.With().Str("lock-name", l.name).Logger(),
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
				l.logger.With().Str("lock-name", l.name).Logger(),
				"refresh lock "+l.name,
			)
		case <-ctx.Done():
			return
		}
	}
}

func (l *Lock) locksTableDeleteLock(ctx context.Context, lockID string) (bool, error) {
	session, err := l.c.connection.Session()
	if err != nil {
		return false, err
	}

	defer session.Close()

	query := session.Query(`
		DELETE FROM "locks"
		WHERE name = ?
		IF lock_id = ?`,
		l.name, lockID,
	).SerialConsistency(gocql.LocalSerial).WithContext(ctx)

	return query.ScanCAS(nil)
}

// locksReadLock return the lock holder and the remaining TTL.
// Lock holder is the empty string if lock is no acquired.
func (l *Lock) locksReadLock(ctx context.Context) (string, time.Duration, error) {
	var (
		holder     string
		ttlSeconds int
	)

	session, err := l.c.connection.Session()
	if err != nil {
		return "", 0, err
	}

	defer session.Close()

	query := session.Query(`
		SELECT lock_id, ttl(lock_id)
		FROM "locks"
		WHERE name = ?`,
		l.name,
	).WithContext(ctx)

	err = query.Scan(&holder, &ttlSeconds)
	if errors.Is(err, gocql.ErrNotFound) {
		return "", 0, nil
	}

	if err != nil {
		return "", 0, err
	}

	return holder, time.Duration(ttlSeconds) * time.Second, nil
}

func (l *Lock) locksTableInsertLock(ctx context.Context, lockID string, ttl time.Duration) (string, bool, error) {
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
		int64(ttl.Seconds()), lockID, l.name, lockID,
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
