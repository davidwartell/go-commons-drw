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

package shared

import (
	"context"
	"github.com/davidwartell/go-commons-drw/grpcpool"
	"github.com/davidwartell/go-commons-drw/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

const idleWatchDogWakeupSeconds = 30

type Pool struct {
	sync.Mutex
	conn           *grpc.ClientConn
	factory        grpcpool.ConnectionFactory
	idleTimeout    time.Duration
	timeIdleStart  time.Time
	watchDogWg     sync.WaitGroup
	watchDogCtx    context.Context
	watchDogCancel context.CancelFunc
	clientWg       sync.WaitGroup
	clientCount    uint64
	closing        bool
}

type ClientConn struct {
	conn     *grpc.ClientConn
	pool     *Pool
	returned bool
}

var ErrClientConnClosing = status.Error(codes.Canceled, "grpc: the client connection is closing")
var ErrClientConnNotOk = status.Error(codes.Unavailable, "grpc: error - connection not ok")

func New(factory grpcpool.ConnectionFactory, idleTimeout time.Duration) *Pool {
	if idleTimeout < idleWatchDogWakeupSeconds {
		idleTimeout = idleWatchDogWakeupSeconds
	}

	pool := &Pool{
		factory:       factory,
		idleTimeout:   idleTimeout,
		timeIdleStart: time.Now(),
		clientCount:   0,
	}

	pool.watchDogCtx, pool.watchDogCancel = context.WithCancel(context.Background())

	pool.watchDogWg.Add(1)
	go pool.watchDog(pool.watchDogCtx, &pool.watchDogWg)
	return pool
}

func (p *Pool) Close() {
	// stop new clients from borrowing connection
	p.Lock()
	p.closing = true
	if p.conn != nil {
		_ = p.conn.Close()
	}
	// cancel watchdog context
	if p.watchDogCancel != nil {
		p.watchDogCancel()
	}
	p.Unlock()

	// wait for clients to return connections
	p.clientWg.Wait()
	// wait for watch dog to exit
	p.watchDogWg.Wait()
}

func (p *Pool) watchDog(ctx context.Context, wg *sync.WaitGroup) {
	for {
		p.checkIdleConnection()
		select {
		case <-time.After(time.Second * idleWatchDogWakeupSeconds):
		case <-ctx.Done():
			logger.Instance().TraceUnstruct("pool.watchDog exiting")
			wg.Done()
			return
		}
	}
}

func (p *Pool) checkIdleConnection() {
	p.Lock()
	defer p.Unlock()
	logger.Instance().TraceUnstruct("pool.watchDog checking idle connections")
	if p.conn == nil {
		return
	}
	if p.clientCount > 0 {
		return
	}
	if p.timeIdleStart.Add(p.idleTimeout).Before(time.Now()) {
		_ = p.conn.Close()
		p.conn = nil
		logger.Instance().TraceUnstruct("Connection Idle Closed")
	}
}

func (p *Pool) Get(ctx context.Context) (grpcpool.PoolClientConn, error) {
	p.Lock()
	defer p.Unlock()

	if p.closing {
		return nil, ErrClientConnClosing
	}

	if p.conn == nil {
		var err error
		p.conn, err = p.factory.NewConnection(ctx)
		if err != nil {
			p.conn = nil
			return nil, err
		}
		logger.Instance().TraceUnstruct("Opened New Connection from Factory")
	} else {
		// test an existing connection
		err := p.factory.ConnectionOk(ctx, p.conn)
		if err != nil {
			logger.Instance().InfofUnstruct("existing connection not ok closing: %v", err)
			_ = p.conn.Close()
			p.conn = nil

			logger.Instance().InfoUnstruct("trying to reconnect")
			var err2 error
			p.conn, err2 = p.factory.NewConnection(ctx)
			if err2 != nil {
				p.conn = nil
				logger.Instance().InfofUnstruct("reconnect failed: %v", err2)
				return nil, err
			}

			err3 := p.factory.ConnectionOk(ctx, p.conn)
			if err3 != nil {
				logger.Instance().InfofUnstruct("reconnect not ok closing: %v", err3)
				_ = p.conn.Close()
				p.conn = nil
				return nil, ErrClientConnNotOk
			}
		}
	}

	wrapper := ClientConn{
		conn:     p.conn,
		pool:     p,
		returned: false,
	}
	p.clientCount++
	p.clientWg.Add(1)
	logger.Instance().TracefUnstruct("Get connection count=%d", p.clientCount)
	return &wrapper, nil
}

func (c *ClientConn) Return() {
	if c.returned {
		return
	}
	c.pool.Lock()
	defer c.pool.Unlock()
	c.pool.clientCount--
	if c.pool.clientCount == 0 {
		c.pool.timeIdleStart = time.Now()
	}
	c.pool.clientWg.Done()
	c.returned = true
	c.conn = nil
	logger.Instance().TracefUnstruct("Return connection count=%d", c.pool.clientCount)
}

func (c *ClientConn) Connection() *grpc.ClientConn {
	return c.conn
}
