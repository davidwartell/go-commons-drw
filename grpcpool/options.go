/*
 * Copyright (c) 2023 by David Wartell. All Rights Reserved.
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

package grpcpool

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"time"
)

type Options struct {
	keepalive             *keepalive.ClientParameters
	pingFunc              PingFunc
	withSnappyCompression bool
	withOtelTracing       bool
}

// PingFunc should send a GRPC ping/pong to the other side of conn.  Returns err or latency.
type PingFunc func(ctx context.Context, conn *grpc.ClientConn) (time.Duration, error)

type Option func(o *Options)

//goland:noinspection GoUnusedExportedFunction
func WithKeepaliveClientParams(keepalive *keepalive.ClientParameters) Option {
	return func(o *Options) {
		o.keepalive = keepalive
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithPingTestEveryNewConnection(pingFunc PingFunc) Option {
	return func(o *Options) {
		o.pingFunc = pingFunc
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithSnappyCompression(enabled bool) Option {
	return func(o *Options) {
		o.withSnappyCompression = enabled
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithOtelTracing(enabled bool) Option {
	return func(o *Options) {
		o.withOtelTracing = enabled
	}
}
