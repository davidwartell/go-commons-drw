///
// Copyright (c) 2021. StealthMode Inc. All Rights Reserved
///

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
