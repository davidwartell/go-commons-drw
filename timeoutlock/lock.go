/*
 * Copyright (c) 2022 by David Wartell. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
