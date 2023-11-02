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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/security/advancedtls"
	"time"
)

type MutualTLSFactory struct {
	credentials          credentials.TransportCredentials
	dialAddr             string
	keepAliveTime        time.Duration
	keepAliveTimeout     time.Duration
	pingFunc             PingFunc
	useSnappyCompression bool
	useOtel              bool
}

// PingFunc should send a GRPC ping/pong to the other side of conn.  Returns err or latency.
type PingFunc func(ctx context.Context, conn *grpc.ClientConn) (time.Duration, error)

//goland:noinspection GoUnusedExportedFunction
func NewMutualTLSFactory(
	caCertPEM []byte,
	clientCertPEM []byte,
	clientKeyPEM []byte,
	serverAddress string,
	keepAliveTime time.Duration,
	keepAliveTimeout time.Duration,
	pingFunc PingFunc,
	useSnappyCompression bool,
	useOtel bool,
) (MutualTLSFactory, error) {
	var err error
	factory := MutualTLSFactory{
		keepAliveTime:        keepAliveTime,
		keepAliveTimeout:     keepAliveTimeout,
		pingFunc:             pingFunc,
		useSnappyCompression: useSnappyCompression,
		useOtel:              useOtel,
	}
	factory.credentials, err = LoadTLSCredentials(caCertPEM, clientCertPEM, clientKeyPEM)
	if err != nil {
		logger.Instance().Error("error loading TLS credentials", logger.Error(err))
		return factory, err
	}

	factory.dialAddr = serverAddress

	return factory, nil
}

func (f MutualTLSFactory) NewConnection(ctx context.Context) (*grpc.ClientConn, error) {
	return f.NewConnectionWithDialOpts(ctx)
}

func (f MutualTLSFactory) NewConnectionWithDialOpts(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	allOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(f.credentials),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                f.keepAliveTime,
			Timeout:             f.keepAliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	if f.useSnappyCompression {
		allOpts = append(allOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(SnappyCompressor())))
	}

	if f.useOtel {
		allOpts = append(allOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	if len(opts) > 0 {
		allOpts = append(allOpts, opts...)
	}

	conn, err := grpc.DialContext(ctx, f.dialAddr, allOpts...)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		logger.Instance().Info("failed to dial", logger.String("dialAddr", f.dialAddr), logger.Error(err))
		return nil, err
	}

	err = f.ConnectionOk(ctx, conn)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		logger.Instance().Info("failed to ping", logger.String("dialAddr", f.dialAddr), logger.Error(err))
		err = errors.Wrapf(err, "failed to ping %s", f.dialAddr)
		return nil, err
	}

	return conn, nil
}

func (f MutualTLSFactory) ConnectionOk(ctx context.Context, conn *grpc.ClientConn) error {
	if f.pingFunc == nil {
		return nil
	}

	// we have to send a request to the server to see if we can actually write to the socket
	// implementing a ping/pong rpc is useful for this
	_, err := f.pingFunc(ctx, conn)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}
	return nil
}

func LoadTLSCredentials(caCertPEM []byte, clientCertPEM []byte, clientKeyPEM []byte) (credentials.TransportCredentials, error) {
	// Load certificate of the CA who signed server's certificate
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCertPEM) {
		return nil, fmt.Errorf("failed to add server CA's certificate")
	}

	// Load client's certificate and private key
	clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, err
	}

	clientOptions := &advancedtls.ClientOptions{
		IdentityOptions: advancedtls.IdentityCertificateOptions{
			Certificates: []tls.Certificate{clientCert},
		},
		RootOptions: advancedtls.RootCertificateOptions{
			RootCACerts: certPool,
		},
		VType: advancedtls.CertVerification,
	}

	return advancedtls.NewClientCreds(clientOptions)
}
