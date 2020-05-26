package dummy

import (
	"squirreldb/types"
	"sync"
	"time"
)

// Locks is a non-distributed lock factory. It should only be used for single-node
type Locks struct {
	globalMutex sync.Mutex
	locks       map[string]*tryLocker
}

type tryLocker struct {
	l        sync.Mutex
	acquired bool
}

// CreateLock return a TryLocker for given name.
// This locker is NOT common across all SquirrelDB and not even this process. It's only common this this factory.
// timeToLive is ignored, as it normally only used in case of crash to release the lock.
func (l *Locks) CreateLock(name string, timeToLive time.Duration) types.TryLocker {
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

func (l *tryLocker) TryLock() bool {
	l.l.Lock()
	defer l.l.Unlock()

	if !l.acquired {
		l.acquired = true
		return true
	}

	return false
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
	for {
		ok := l.TryLock()
		if ok {
			return
		}

		time.Sleep(time.Second)
	}
}
