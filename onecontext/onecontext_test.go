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

//
// Derivative work of https://github.com/teivah/onecontext/ (Apache License)
//

package onecontext

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type key int

const (
	foo key = iota
	bar
	baz
)

func eventually(ch <-chan struct{}) bool {
	timeout, cancelFunc := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelFunc()

	select {
	case <-ch:
		return true
	case <-timeout.Done():
		return false
	}
}

func Test_Merge_Nominal(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.WithValue(context.Background(), foo, "foo"))
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.WithValue(context.Background(), bar, "bar"))

	ctx, _ := Merge(ctx1, ctx2)

	deadline, ok := ctx.Deadline()
	assert.True(t, deadline.IsZero())
	assert.False(t, ok)

	assert.Equal(t, "foo", ctx.Value(foo))
	assert.Equal(t, "bar", ctx.Value(bar))
	assert.Nil(t, ctx.Value(baz))

	assert.False(t, eventually(ctx.Done()))
	assert.NoError(t, ctx.Err())

	cancel2()
	assert.True(t, eventually(ctx.Done()))
	assert.Error(t, ctx.Err())
}

func Test_Merge_Deadline_Context1(t *testing.T) {
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()
	ctx2 := context.Background()

	ctx, _ := Merge(ctx1, ctx2)

	deadline, ok := ctx.Deadline()
	assert.False(t, deadline.IsZero())
	assert.True(t, ok)
}

func Test_Merge_Deadline_Context2(t *testing.T) {
	ctx1 := context.Background()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	ctx, _ := Merge(ctx1, ctx2)

	deadline, ok := ctx.Deadline()
	assert.False(t, deadline.IsZero())
	assert.True(t, ok)
}

func Test_Merge_Deadline_ContextN(t *testing.T) {
	ctx1 := context.Background()
	ctxs := make([]context.Context, 0)
	for i := 0; i < 10; i++ {
		ctx := context.Background()
		ctxs = append(ctxs, ctx)
	}
	ctxN, cancel := context.WithTimeout(context.Background(), time.Second)
	ctxs = append(ctxs, ctxN)
	for i := 0; i < 10; i++ {
		ctx := context.Background()
		ctxs = append(ctxs, ctx)
	}

	ctx, _ := Merge(ctx1, ctxs...)

	assert.False(t, eventually(ctx.Done()))
	assert.NoError(t, ctx.Err())

	cancel()
	assert.True(t, eventually(ctx.Done()))
	assert.Error(t, ctx.Err())
}

func Test_Merge_Deadline_None(t *testing.T) {
	ctx1 := context.Background()
	ctx2 := context.Background()

	ctx, _ := Merge(ctx1, ctx2)

	deadline, ok := ctx.Deadline()
	assert.True(t, deadline.IsZero())
	assert.False(t, ok)
}

func Test_Cancel_Two(t *testing.T) {
	ctx1 := context.Background()
	ctx2 := context.Background()

	ctx, cancel := Merge(ctx1, ctx2)

	cancel()
	assert.True(t, eventually(ctx.Done()))
	assert.Error(t, ctx.Err())
	assert.Equal(t, "canceled context", ctx.Err().Error())
	assert.IsType(t, &Canceled{}, ctx.Err())
}

func Test_Cancel_Multiple(t *testing.T) {
	ctx1 := context.Background()
	ctx2 := context.Background()
	ctx3 := context.Background()

	ctx, cancel := Merge(ctx1, ctx2, ctx3)

	cancel()
	assert.True(t, eventually(ctx.Done()))
	assert.Error(t, ctx.Err())
	assert.Equal(t, "canceled context", ctx.Err().Error())
	assert.IsType(t, &Canceled{}, ctx.Err())
}
