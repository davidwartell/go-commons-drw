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

package mongostore

import (
	"context"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/davidwartell/go-commons-drw/task"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

func (a *DataStore) Ping(ctx context.Context) error {
	defer task.HandlePanic(taskName)
	var err error
	var client *mongo.Client

	var cancel context.CancelFunc
	ctx, cancel = a.ContextTimeout(ctx)
	defer cancel()

	client, err = a.clientUnsafeFastWrites(ctx)
	if err != nil {
		task.LogErrorStruct(taskName, "error getting client for ping", logger.Error(err))
		return err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		err2 := errors.Wrap(err, "Mongo ping failed")
		return err2
	}

	var collection *mongo.Collection
	collection, err = Instance().CollectionLinearWriteRead(ctx, "ping")
	if err != nil {
		task.LogErrorStruct(taskName, "error getting collection for ping write test", logger.Error(err))
		return err
	}

	filter := bson.D{{"_id", "testWrite"}}
	update := bson.D{
		{
			"$inc",
			bson.D{{"count", uint64(1)}},
		},
	}
	updateOptions := &mongooptions.UpdateOptions{}
	updateOptions = updateOptions.SetUpsert(true)
	_, err = collection.UpdateOne(ctx, filter, update, updateOptions)
	if err != nil {
		return err
	}

	return nil
}

func (a *DataStore) runPing(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	task.LogInfoStruct(taskName, "ping runner started")

	a.RLock()
	heartbeatSeconds := a.options.pingHeartbeatSeconds
	a.RUnlock()

	for {
		err := a.Ping(ctx)
		if err != nil {
			task.LogErrorStruct(taskName, "mongo ping failed", logger.Error(err))
		}
		select {
		case <-time.After(time.Second * time.Duration(heartbeatSeconds)):
		case <-ctx.Done():
			task.LogInfoStruct(taskName, "ping runner stopped")
			return
		}
	}
}
