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

package grpcpool

import (
	"context"
	"google.golang.org/grpc"
)

type PoolClientConn interface {
	Return()
	Connection() *grpc.ClientConn
}

type Pool interface {
	Get(context.Context) (PoolClientConn, error)
}

type ConnectionFactory interface {
	NewConnection(ctx context.Context) (*grpc.ClientConn, error)
	ConnectionOk(ctx context.Context, conn *grpc.ClientConn) error
}
