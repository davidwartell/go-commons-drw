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

package httpcommon

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/elazarl/goproxy"
	"github.com/pkg/errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type HttpTransportWrapper struct {
	transport     *http.Transport
	dialError     bool
	dialErrorLock sync.Mutex
}

func (t *HttpTransportWrapper) Transport() *http.Transport {
	return t.transport
}

func (t *HttpTransportWrapper) DialError() bool {
	t.dialErrorLock.Lock()
	defer t.dialErrorLock.Unlock()
	return t.dialError
}

func NewHttpTransport(addr net.Addr, dialTimeout time.Duration) *HttpTransportWrapper {
	transportWrapper := &HttpTransportWrapper{}

	// abstract the port forward address and port away from the http client
	dialFunc := func(ctx context.Context, network, dialAddr string) (net.Conn, error) {
		dialer := net.Dialer{
			Timeout: dialTimeout,
		}
		conn, err := dialer.DialContext(ctx, "tcp", addr.String())
		if err != nil {
			transportWrapper.dialErrorLock.Lock()
			transportWrapper.dialError = true
			transportWrapper.dialErrorLock.Unlock()
			return nil, err
		}
		return conn, nil
	}

	transportWrapper.transport = &http.Transport{
		DialContext:         dialFunc,
		MaxIdleConnsPerHost: -1,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		DisableKeepAlives: true,
	}

	return transportWrapper
}

type HttpProxy interface {
	Running() bool
	DialError() bool
	ListAddr() net.Addr
	HttpConnectionString() string
	HttpConnectionUrl() (proxyUrl *url.URL, err error)
	Shutdown()
	Get(
		ctx context.Context,
		requestUrl string,
		requestTimeout time.Duration,
	) (resp *http.Response, dialError bool, err error)
}

type httpProxyImpl struct {
	sync.RWMutex
	transportWrapper *HttpTransportWrapper
	listener         net.Listener
	server           *http.Server
	userAgent        string
	localhost        string
	running          bool
}

func (p *httpProxyImpl) Running() bool {
	p.Lock()
	defer p.Unlock()
	return p.running
}

func (p *httpProxyImpl) DialError() bool {
	p.Lock()
	transportWrapper := p.transportWrapper
	p.Unlock()
	if transportWrapper == nil {
		return false
	}
	return transportWrapper.DialError()
}

func (p *httpProxyImpl) ListAddr() net.Addr {
	p.Lock()
	defer p.Unlock()
	return p.listener.Addr()
}

func (p *httpProxyImpl) HttpConnectionString() string {
	listenAddr := p.ListAddr().String()
	//goland:noinspection ALL
	return fmt.Sprintf("http://%s", listenAddr)
}

func (p *httpProxyImpl) HttpConnectionUrl() (proxyUrl *url.URL, err error) {
	proxyUrl, err = url.ParseRequestURI(p.HttpConnectionString())
	if err != nil {
		err2 := errors.Errorf("error parsing proxy url %s: %v", p.HttpConnectionString(), err)
		logger.Instance().ErrorUnstruct(err2)
		err = err2
	}
	return
}

func (p *httpProxyImpl) Shutdown() {
	p.Lock()
	defer p.Unlock()
	if !p.running {
		return
	}
	proxyShutdownCtx, proxyShutdownCancel := context.WithTimeout(context.Background(), time.Second*1)
	defer proxyShutdownCancel()
	if err := p.server.Shutdown(proxyShutdownCtx); err != nil {
		logger.Instance().ErrorfUnstruct("error stopping proxy http server: %s", err)
	}
	p.running = false // even if we failed to shutdown assume its a bad state
}

func (p *httpProxyImpl) Get(
	ctx context.Context,
	requestUrl string,
	requestTimeout time.Duration,
) (resp *http.Response, dialError bool, err error) {
	p.RLock()
	if !p.running {
		err = errors.New("proxy not running")
		p.RUnlock()
		return
	}
	p.RUnlock()

	var proxyUrl *url.URL
	proxyUrl, err = p.HttpConnectionUrl()
	if err != nil {
		err2 := errors.Wrap(err, "error getting proxy connection URL")
		return nil, false, err2
	}

	p.RLock()
	userAgent := p.userAgent
	p.RUnlock()

	transport := &http.Transport{}
	transport.Proxy = http.ProxyURL(proxyUrl)
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	httpClient := &http.Client{
		Timeout: requestTimeout,
	}
	httpClient.Transport = transport

	var httpReq *http.Request
	httpReq, err = http.NewRequest("GET", requestUrl, nil)
	if err != nil {
		err2 := errors.Errorf("error on http.NewRequest for url %s: %v", requestUrl, err)
		logger.Instance().ErrorUnstruct(err2)
		return nil, false, err2
	}
	httpReq.WithContext(ctx)
	httpReq.Header.Set("User-Agent", userAgent)
	httpReq.Header.Add("Accept-Charset", "utf-8")

	var httpResponse *http.Response
	httpResponse, err = httpClient.Do(httpReq)
	if err != nil {
		p.RLock()
		transportWrapper := p.transportWrapper
		p.RUnlock()
		dialError = transportWrapper.DialError()
		err2 := errors.Errorf("error on http request for url %s: %v", requestUrl, err)
		if !dialError {
			logger.Instance().InfoUnstruct(err2)
		}
		return nil, dialError, err2
	}
	return httpResponse, false, nil
}

func NewHttpProxy(
	ctx context.Context,
	addr net.Addr,
	dialTimeout time.Duration,
	userAgent string,
	localhost string,
) (HttpProxy, error) {
	var err error
	httpProxy := &httpProxyImpl{
		userAgent: userAgent,
		localhost: localhost,
	}
	httpProxy.transportWrapper = NewHttpTransport(addr, dialTimeout)
	proxy := goproxy.NewProxyHttpServer()
	proxy.Tr = httpProxy.transportWrapper.Transport()
	proxyListenAddr := net.JoinHostPort(httpProxy.localhost, "") // empty port let O/S choose ephemeral port
	listenConfig := &net.ListenConfig{}
	httpProxy.listener, err = listenConfig.Listen(ctx, "tcp", proxyListenAddr)
	if err != nil {
		logger.Instance().ErrorUnstruct(err)
		return nil, err
	}
	httpProxy.server = &http.Server{
		Handler:      proxy,
		Addr:         httpProxy.listener.Addr().String(),
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
	go func() {
		_ = httpProxy.server.Serve(httpProxy.listener) // Serve always returns a non-nil error and closes listener
	}()

	httpProxy.Lock()
	httpProxy.running = true
	httpProxy.Unlock()

	return httpProxy, nil
}
