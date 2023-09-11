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

package watchdog

import (
	"context"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/davidwartell/go-commons-drw/timer"
	"sync"
	"time"
)

type Instance struct {
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	maxRunTime        time.Duration
	heartbeat         Heartbeat
	heartbeatInterval time.Duration
}

type Heartbeat interface {
	StillRunning(ctx context.Context) (ok bool)
}

// NewWatchDog creates a new watchdog Instance. Watchdog will call the cancel function associated with the returned
// context when maxRunTime has been exceeded or Heartbeat.StillRunning return false.  Pass the context returned to a
// long-running tasks that can be interrupted by a cancelled context.
//
// Cancel() should be called on the returned Instance when you are done, or it will not be garbage collected until
// maxRunTime expires.
//
// I'm using watchdog to time limit background tasks running in golang on an IoT sensor.  The Heartbeat callback is used
// to reset (in)visibility of the task in a queue, so it is not dispatched to another sensor.  Could also be used for
// long-running tasks driven by an AWS SQS queue for example.
//
//goland:noinspection GoUnusedExportedFunction
func NewWatchDog(ctx context.Context, maxRunTime time.Duration) (*Instance, context.Context) {
	return NewWatchDogWithHeartbeat(ctx, maxRunTime, nil, 0)
}

// NewWatchDogWithHeartbeat creates a new watch dog same NewWatchDog except in addition a callback is called every heartbeatInterval.
// Cancel() should be called when you are done with the instance, or it will run until maxRunTime expires.
func NewWatchDogWithHeartbeat(ctx context.Context, maxRunTime time.Duration, heartbeat Heartbeat, heartbeatInterval time.Duration) (*Instance, context.Context) {
	wdCtx, wdCancel := context.WithCancel(ctx)
	dog := &Instance{
		ctx:               wdCtx,
		cancel:            wdCancel,
		maxRunTime:        maxRunTime,
		heartbeat:         heartbeat,
		heartbeatInterval: heartbeatInterval,
	}

	dog.wg.Add(1)
	go dog.run()
	return dog, wdCtx
}

func (i *Instance) Cancel() {
	i.cancel()
	i.wg.Wait()
}

func (i *Instance) run() {
	defer i.wg.Done()

	var sleepTimerPtr *timer.Timer
	var sleepTimerChan <-chan time.Time
	if i.heartbeat != nil && i.heartbeatInterval > 0 {
		var sleepTimer timer.Timer
		sleepTimerPtr = &sleepTimer
		defer sleepTimer.Stop()
		sleepTimerChan = sleepTimer.C
	}

	var maxRunTimeTimer timer.Timer
	defer maxRunTimeTimer.Stop()
	maxRunTimeTimer.Reset(i.maxRunTime)

	i.watchdogLoop(&maxRunTimeTimer, sleepTimerPtr, sleepTimerChan)
}

func (i *Instance) watchdogLoop(maxRunTimeTimer *timer.Timer, sleepTimerPtr *timer.Timer, sleepTimerChan <-chan time.Time) {
	for {
		if sleepTimerPtr != nil {
			sleepTimerPtr.Reset(i.heartbeatInterval)
		}
		select {
		case <-sleepTimerChan:
			// this case will block forever on read of nil channel if i.heartbeat == nil || i.heartbeatInterval <= 0
			sleepTimerPtr.Read = true
			if i.heartbeat != nil {
				if ok := i.heartbeat.StillRunning(i.ctx); !ok {
					logger.Instance().Error("watchdog heartbeat returned not ok - cancelling")
					i.cancel()
					return
				}
			}
		case <-maxRunTimeTimer.C:
			// if max run time expired log error and cancel
			maxRunTimeTimer.Read = true
			logger.Instance().Error("watchdog exceeded max run time - cancelling", logger.Duration("maxRunTime", i.maxRunTime))
			i.cancel()
			return
		case <-i.ctx.Done():
			return
		}
	}
}
