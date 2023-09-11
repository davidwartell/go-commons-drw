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

package mongostore

import (
	"context"
	"github.com/davidwartell/go-commons-drw/onecontext"
	"time"
)

// ContextTimeout merges the ctx argument with a new context with a timeout using the WithTimeoutSecondsQuery option on
// the DataStore and returns a new context the client should use on queries to mon go. The cancelFunc returned MUST be
// called when the client is done using the context for queries in the span of the timeout.
// By merging the contexts (see onecontext.Merge) the client can distinguish between a failure from timeout on a query
// v.s. an interrupted client.
//
// Example:
//
//		queryCtx, cancel := mgocluster.Instance().Cluster(impl.clusterName).ContextTimeout(clientCtx)
//		defer cancel()
//
//	 // Note use the clientCtx for obtaining the collection it uses a separate connect timeout.
//		var collection *mongo.Collection
//		if collection, err = mgocluster.Instance().Cluster(impl.clusterName).CollectionLinearWriteRead(clientCtx, impl.collectionName); err != nil {
//			return
//		}
//		...
//		if mongoCursor, err = collection.Find(queryCtx, filter);  err != nil {
//	    return
//	 }
func (a *DataStore) ContextTimeout(ctx context.Context) (clientCtx context.Context, cancelFunc context.CancelFunc) {
	a.rwMutex.RLock()
	timeout := a.queryTimeout()
	a.rwMutex.RUnlock()
	return a.ContextTimeoutWithDuration(ctx, timeout)
}

// ContextTimeoutWithDuration is same as ContextTimeout except the duration is given as input instead of coming from the
// DataStore options.
func (a *DataStore) ContextTimeoutWithDuration(ctx context.Context, timeout time.Duration) (clientCtx context.Context, cancelFunc context.CancelFunc) {
	var queryCtx context.Context
	queryCtx, cancelFunc = context.WithTimeout(context.Background(), timeout)
	clientCtx, _ = onecontext.Merge(ctx, queryCtx)
	return
}

// queryTimeout returns query timeout as time.Duration
// callers MUST hold a.Lock
func (a *DataStore) queryTimeout() time.Duration {
	return time.Duration(a.options.timeoutSecondsQuery) * time.Second
}
