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

package dummy

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/bleemeo/squirreldb/types"
)

// Locks is a non-distributed lock factory. It should only be used for single-node.
type Locks struct {
	locks       map[string]*tryLocker
	globalMutex sync.Mutex
}

type tryLocker struct {
	l        sync.Mutex
	acquired bool
	blocked  bool
}

// CreateLock return a TryLocker for given name.
// This locker is NOT common across all SquirrelDB and not even this process. It's only common to this factory.
// timeToLive is ignored, as it normally only used in case of crash to release the lock.
func (l *Locks) CreateLock(name string, _ time.Duration) types.TryLocker {
	l.globalMutex.Lock()
	defer l.globalMutex.Unlock()

	if l.locks == nil {
		l.locks = make(map[string]*tryLocker)
	}

	locker := l.locks[name]
	if locker == nil {
		locker = &tryLocker{}
	}

	l.locks[name] = locker

	return locker
}

func (l *tryLocker) tryLock() bool {
	l.l.Lock()
	defer l.l.Unlock()

	if !l.acquired {
		l.acquired = true

		return true
	}

	return false
}

func (l *tryLocker) TryLock(ctx context.Context, retryDelay time.Duration) bool {
	for {
		ok := l.tryLock()
		if ok {
			return true
		}

		if retryDelay == 0 {
			return false
		}

		jitter := retryDelay.Seconds() * (1 + rand.Float64()/2) //nolint:gosec
		select {
		case <-time.After(time.Duration(jitter) * time.Second):
		case <-ctx.Done():
			return false
		}
	}
}

func (l *tryLocker) Unlock() {
	l.l.Lock()
	defer l.l.Unlock()

	if !l.acquired {
		panic("unlock of unlocked mutex")
	}

	l.acquired = false
}

func (l *tryLocker) Lock() {
	l.TryLock(context.Background(), 10*time.Second)
}

// BlockLock block Lock() from other thread. blockTTL isn't used on dummy Lock.
func (l *tryLocker) BlockLock(ctx context.Context, blockTTL time.Duration) error {
	_ = blockTTL
	blockDone := false

	for ctx.Err() == nil && !blockDone {
		l.l.Lock()

		if l.blocked {
			// we can re-call BlockLock to refresh the blocking status
			blockDone = true
		}

		if !l.blocked && !l.acquired {
			l.blocked = true
			l.acquired = true
			blockDone = true
		}

		l.l.Unlock()

		time.Sleep(10 * time.Millisecond)
	}

	return ctx.Err()
}

func (l *tryLocker) UnblockLock(_ context.Context) error {
	l.l.Lock()
	defer l.l.Unlock()

	if l.blocked {
		l.acquired = false
		l.blocked = false
	}

	return nil
}

func (l *tryLocker) BlockStatus(_ context.Context) (bool, time.Duration, error) {
	l.l.Lock()
	defer l.l.Unlock()

	return l.blocked, time.Second, nil
}
