///
// Copyright (c) 2021. StealthMode Inc. All Rights Reserved
///

package observer

import (
	"context"
	"go.uber.org/atomic"
	"strconv"
	"testing"
	"time"
)

func TestClose(t *testing.T) {
	var o = NewBufferedSetObserver(0 * time.Second)
	o.Close()
}

func TestAddListener(t *testing.T) {
	var output string
	var o = NewBufferedSetObserver(0 * time.Second)
	defer o.Close()

	done := make(chan bool)
	defer close(done)

	o.AddListener(func(ctx context.Context, e []string) {
		output = e[0]
		done <- true
	})

	o.Emit("done")

	<-done // blocks until listener is triggered

	if output != "done" {
		t.Error("error Emitting strings.")
	}
}

func TestEmit(t *testing.T) {
	var output string
	var o = NewBufferedSetObserver(0 * time.Second)
	defer o.Close()

	done := make(chan bool)
	defer close(done)

	o.AddListener(func(ctx context.Context, e []string) {
		output = e[0]
		done <- true
	})

	o.Emit("done")

	<-done // blocks until listener is triggered

	if output != "done" {
		t.Error("error Emitting strings.")
	}
}

func TestEmitParallel(t *testing.T) {
	var o = NewBufferedSetObserver(0 * time.Second)
	defer o.Close()

	numRoutines := uint64(1000)
	done := make(chan bool)
	defer close(done)

	receivedCount := atomic.NewUint64(0)
	o.AddListener(func(ctx context.Context, e []string) {
		for range e {
			receivedCount.Add(1)
			if receivedCount.Load() == numRoutines {
				done <- true
			}
		}
	})

	for i := uint64(0); i < numRoutines; i++ {
		num := i
		go func() {
			o.Emit("done " + strconv.FormatUint(num, 10))
		}()
	}

	// blocks until listener is triggered numRoutines times
	<-done
}

func TestEmitParallelBuffered(t *testing.T) {
	bufferDuration := uint64(1)
	var o = NewBufferedSetObserver(time.Duration(bufferDuration) * time.Second)
	defer o.Close()

	numRoutines := uint64(1000)
	done := make(chan bool)
	defer close(done)

	receivedCount := atomic.NewUint64(0)
	o.AddListener(func(ctx context.Context, e []string) {
		for range e {
			receivedCount.Add(1)
			if receivedCount.Load() == numRoutines {
				done <- true
			}
		}
	})

	sleepMs := ((bufferDuration * uint64(2)) * uint64(1000)) / numRoutines
	for i := uint64(0); i < numRoutines; i++ {
		num := i
		go func() {
			o.Emit("done " + strconv.FormatUint(num, 10))
		}()
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}

	// blocks until listener is triggered numRoutines times
	<-done
}

func TestBufferedEvents(t *testing.T) {
	var output []string
	var o = NewBufferedSetObserver(1 * time.Second)
	defer o.Close()

	done := make(chan bool)
	defer close(done)

	o.AddListener(func(ctx context.Context, e []string) {
		output = e
		done <- true
	})

	o.Emit("done1")
	o.Emit("done2")

	<-done // blocks until listener is triggered

	if len(output) != 2 {
		t.Error("error sending 2 buffered events.")
	}

	o.Emit("done")
	o.Emit("done")
	o.Emit("done")
	o.Emit("done")

	<-done // blocks until listener is triggered

	if len(output) != 1 {
		t.Error("error sending 4 buffered identical events.")
	}
}
