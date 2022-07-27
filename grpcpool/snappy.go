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
	"io"
	"sync"

	"github.com/golang/snappy"
	"google.golang.org/grpc/encoding"
)

// Snappy is the name of the grpc registered compressor.
const snappyName = "snappy"

var snappyRegisterOnce sync.Once
var writerPool sync.Pool
var readerPool sync.Pool

type snappyCompressor struct{}

func SnappyCompressor() string {
	RegisterSnappyCompressor()
	return snappyName
}

func RegisterSnappyCompressor() {
	snappyRegisterOnce.Do(func() {
		encoding.RegisterCompressor(&snappyCompressor{})
	})
}

func (c *snappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	wr, inPool := writerPool.Get().(*snappyWriteCloser)
	if !inPool {
		return &snappyWriteCloser{Writer: snappy.NewBufferedWriter(w)}, nil
	}
	wr.Reset(w)

	return wr, nil
}

func (c *snappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	dr, inPool := readerPool.Get().(*snappyReader)
	if !inPool {
		return &snappyReader{Reader: snappy.NewReader(r)}, nil
	}
	dr.Reset(r)

	return dr, nil
}

func (c *snappyCompressor) Name() string {
	return snappyName
}

type snappyWriteCloser struct {
	*snappy.Writer
}

func (w *snappyWriteCloser) Close() error {
	defer func() {
		writerPool.Put(w)
	}()

	return w.Writer.Close()
}

type snappyReader struct {
	*snappy.Reader
}

func (r *snappyReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	if err == io.EOF {
		readerPool.Put(r)
	}
	return n, err
}
