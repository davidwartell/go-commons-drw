///
// Copyright (c) 2021. StealthMode Inc. All Rights Reserved
///

package timeoutlock

import (
	"context"
	"golang.org/x/sync/semaphore"
	"time"
)

// Mutex is a lock for long-running tasks where Lock can be interrupted by canceling a context.
type Mutex struct {
	sem *semaphore.Weighted
}

// Lock blocks forever until the lock is free, attempting every 100 ms until the context is cancelled.
// Lock must be unlocked with Unlock()
// On success returns true.  If context cancelled returns false.
func (l *Mutex) Lock(ctx context.Context) bool {
	for {
		acquired := l.sem.TryAcquire(1)
		if acquired {
			return true
		}
		select {
		case <-time.After(time.Millisecond * 100):
		case <-ctx.Done():
			return false
		}
	}
}

func (l *Mutex) Unlock() {
	l.sem.Release(1)
}

func NewMutex() Mutex {
	return Mutex{
		sem: semaphore.NewWeighted(1),
	}
}
