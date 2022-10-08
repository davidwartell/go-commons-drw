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
	"github.com/jpillora/backoff"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"strings"
	"sync"
	"time"
)

const indexNameDelim = "_"

//goland:noinspection GoUnusedConst
const (
	ASC  = 1
	DESC = -1
)

const startupIndexGroupName = "_startup"

type IndexIdentifier string

type Index struct {
	CollectionName string
	Id             IndexIdentifier
	Version        uint64 // increment any time the model or options changes - calling createIndex() with the same name but different \
	// options than an existing index will throw an error MongoError: \
	// Index with name: **MongoIndexName** already exists with different options
	Model mongo.IndexModel
}

type indexGroup struct {
	name    string
	indexes []Index
}

// AddAndEnsureManagedIndexes adds additional indexes to be managed after startup. groupName must be unique and each
// group must operate on a different set of Collections than another group.  If groupName is already registered
// then this function does nothing and returns. If this group has Collections overlapping with another managed group
// then panics.
// AddAndEnsureManagedIndexes will do the work in a new go routine.  If there are problems you will need to watch log messages.
// If it's unable to connect to mongo it will keep retrying using an exponential backoff with a default of
// DefaultMaxFailedEnsureIndexesBackoffSeconds configurable with WithMaxFailedEnsureIndexesBackoffSeconds().
func (a *DataStore) AddAndEnsureManagedIndexes(groupName string, addManagedIndexes []Index) {
	a.addManagedIndexes(groupName, addManagedIndexes)
	a.Lock()
	defer a.Unlock()
	a.wg.Add(1)
	go a.runEnsureIndexes(a.ctx, &a.wg, groupName)
}

func (a *DataStore) Index(collectionName string, indexId IndexIdentifier) (idx Index, err error) {
	a.managedIndexesLock.RLock()
	defer a.managedIndexesLock.RUnlock()
	indexFullName := managedIndexId(collectionName, indexId)
	var exists bool
	idx, exists = a.allIndexesByPath[indexFullName]
	if !exists {
		err = errors.Errorf("index with identifier %s not found", indexFullName)
		return
	}
	return
}

func (a *DataStore) IndexOrPanic(collectionName string, indexId IndexIdentifier) (idx Index) {
	var err error
	idx, err = a.Index(collectionName, indexId)
	if err != nil {
		logger.Instance().Panic("error getting index for identifier", logger.Error(err))
	}
	return
}

func (iid IndexIdentifier) String() string {
	return string(iid)
}

func managedIndexId(collectionName string, indexId IndexIdentifier) string {
	var sb strings.Builder
	sb.WriteString(collectionName)
	sb.WriteString("+")
	sb.WriteString(indexId.String())
	return sb.String()
}

func (a *DataStore) addManagedIndexes(groupName string, addManagedIndexes []Index) {
	logger.Instance().Info("adding managed index group", logger.String("groupName", groupName))
	a.managedIndexesLock.Lock()
	defer a.managedIndexesLock.Unlock()

	// check duplicate group name
	for _, group := range a.managedIndexes {
		if group.name == groupName {
			// if group already is added do nothing
			return
		}
	}

	// get all collections from all existing groups
	allUniqueCollectionNamesMap := make(map[string]indexGroup)
	for _, group := range a.managedIndexes {
		for _, idx := range group.indexes {
			allUniqueCollectionNamesMap[idx.CollectionName] = group
		}
	}

	mapOfUniqueIndexIdsPerCollectionNewGroup := make(map[string]map[string]struct{})
	for _, idx := range addManagedIndexes {
		// make sure this group does not overlap any existing managed collection names
		if existingGroup, duplicateColl := allUniqueCollectionNamesMap[idx.CollectionName]; duplicateColl {
			logger.Instance().Panic(
				"addManagedIndexes encountered index on collection that overlaps with another group",
				logger.String("collectionName", idx.CollectionName),
				logger.String("duplicateIdxName", idx.Id.String()),
				logger.String("existingGroupName", existingGroup.name),
			)
		}

		// make sure each index has a unique Id
		collMap, collMapFound := mapOfUniqueIndexIdsPerCollectionNewGroup[idx.CollectionName]
		if !collMapFound {
			collMap = make(map[string]struct{})
			mapOfUniqueIndexIdsPerCollectionNewGroup[idx.CollectionName] = collMap
		}
		if _, duplicateId := collMap[idx.Id.String()]; duplicateId {
			logger.Instance().Panic(
				"addManagedIndexes encountered collection with duplicate index Ids",
				logger.String("collectionName", idx.CollectionName),
				logger.String("duplicateId", idx.Id.String()),
				logger.String("groupName", groupName),
			)
		}
		collMap[idx.Id.String()] = struct{}{}
	}

	// add group to managed indexes
	a.managedIndexes = append(a.managedIndexes, indexGroup{
		name:    groupName,
		indexes: addManagedIndexes,
	})

	// add indexes to map by index Id
	for _, idx := range addManagedIndexes {
		a.allIndexesByPath[managedIndexId(idx.CollectionName, idx.Id)] = idx
	}
}

// Only return error if connect error.
func (a *DataStore) ensureIndexes(ctx context.Context, groupName string) (okOrNoRetry bool) {
	defer task.HandlePanic(taskName)
	a.managedIndexesLock.Lock()
	defer a.managedIndexesLock.Unlock()

	err := a.Ping(ctx)
	if err != nil {
		task.LogErrorStruct(taskName, "ensure indexes: mongo ping failed aborting", logger.Error(err))
		return false
	}

	//
	// 1. Build map of collections and managed index names
	//
	var theGroup *indexGroup
	for _, grp := range a.managedIndexes {
		unshadowedGrp := grp
		if unshadowedGrp.name == groupName {
			theGroup = &unshadowedGrp
			break
		}
	}

	collectionMapToIndexNameMap := make(map[string]map[string]struct{})
	for _, idx := range theGroup.indexes {
		idxName := idx.MongoIndexName()
		if collectionMapToIndexNameMap[idx.CollectionName] == nil {
			collectionMapToIndexNameMap[idx.CollectionName] = make(map[string]struct{})
		}
		collectionMapToIndexNameMap[idx.CollectionName][idxName] = struct{}{}
	}

	//
	// 2. Find any indexes that are not in our list of what we expect and drop them
	//
CollectionLoop:
	for collectionName := range collectionMapToIndexNameMap {
		var collection *mongo.Collection
		collection, err = Instance().CollectionLinearWriteRead(ctx, collectionName)
		if err != nil {
			task.LogErrorStruct(
				taskName,
				"error getting collection to list indexes",
				logger.String("collectionName", collectionName),
				logger.Error(err),
			)
			continue
		}

		var cursor *mongo.Cursor
		cursor, err = collection.Indexes().List(ctx)
		if err != nil {
			task.LogErrorStruct(
				taskName,
				"error listing indexes on collection",
				logger.String("collectionName", collectionName),
				logger.Error(err),
			)
			continue
		}
		for cursor.Next(ctx) {
			indexDoc := bsoncore.Document{}

			if err = cursor.Decode(&indexDoc); err != nil {
				task.LogErrorStruct(
					taskName,
					"error on Decode index document for list indexes cursor on collection",
					logger.String("collectionName", collectionName),
					logger.Error(err),
				)
				_ = cursor.Close(ctx)
				continue CollectionLoop
			}

			nameVal, idErr := indexDoc.LookupErr("name")
			if idErr != nil {
				task.LogErrorStruct(
					taskName,
					"error on LookupErr of name field in index document for list indexes cursor on collection",
					logger.String("collectionName", collectionName),
					logger.Error(err),
				)
				_ = cursor.Close(ctx)
				continue CollectionLoop
			}
			nameStr, nameStrOk := nameVal.StringValueOK()
			if !nameStrOk {
				task.LogErrorStruct(
					taskName,
					"error on StringValueOK of name field in index document for list indexes cursor on collection",
					logger.String("collectionName", collectionName),
					logger.Error(err),
				)
				_ = cursor.Close(ctx)
				continue CollectionLoop
			}

			if nameStr == "_id_" {
				continue
			}

			// index does not exist in new managed indexes drop it
			if _, ok := collectionMapToIndexNameMap[collectionName][nameStr]; !ok {
				startTime := time.Now()
				task.LogInfoStruct(
					taskName,
					"begin drop index",
					logger.String("collectionName", collectionName),
					logger.String("indexName", nameStr),
				)
				_, err = collection.Indexes().DropOne(ctx, nameStr)
				if err != nil {
					if !IsIndexNotFoundError(err) {
						task.LogErrorStruct(
							taskName,
							"error dropping index",
							logger.String("collectionName", collectionName),
							logger.String("indexName", nameStr),
							logger.Error(err),
						)
					} else {
						task.LogInfoStruct(
							taskName,
							"finished drop index - already dropped",
							logger.String("collectionName", collectionName),
							logger.String("indexName", nameStr),
							logger.Duration("time", time.Since(startTime)),
						)
					}
				} else {
					task.LogInfoStruct(
						taskName,
						"finished drop index",
						logger.String("collectionName", collectionName),
						logger.String("indexName", nameStr),
						logger.Duration("time", time.Since(startTime)),
					)
				}
			}
		}
		if cursor.Err() != nil {
			task.LogErrorStruct(
				taskName,
				"error on list indexes cursor on collection",
				logger.String("collectionName", collectionName),
				logger.Error(err),
			)
		}
		if cursor != nil {
			_ = cursor.Close(ctx)
		}
	}

	createIndexOptions := mongooptions.CreateIndexes().SetCommitQuorumMajority()

	//
	// 3. Attempt to create each index.  If the index already exists create will return and do nothing.
	//
	for _, idx := range theGroup.indexes {
		idxName := idx.MongoIndexName()
		if idx.Model.Options == nil {
			idx.Model.Options = mongooptions.Index()
		}
		idx.Model.Options = idx.Model.Options.SetName(idxName)

		var collection *mongo.Collection
		collection, err = Instance().CollectionLinearWriteRead(ctx, idx.CollectionName)
		if err != nil {
			task.LogErrorStruct(
				taskName,
				"error getting collection to ensure index",
				logger.String("collectionName", idx.CollectionName),
				logger.String("index.id", idx.Id.String()),
				logger.Error(err),
			)
			continue
		}

		var nameReturned string
		startTime := time.Now()
		task.LogInfoStruct(
			taskName,
			"begin ensuring index",
			logger.String("collectionName", idx.CollectionName),
			logger.String("idxName", idxName),
		)
		nameReturned, err = collection.Indexes().CreateOne(ctx, idx.Model, createIndexOptions)
		if err != nil {
			task.LogErrorStruct(
				taskName,
				"error ensuring index",
				logger.String("collectionName", idx.CollectionName),
				logger.String("idxName", idxName),
				logger.Error(err),
			)
		} else {
			task.LogInfoStruct(
				taskName,
				"finished ensuring index",
				logger.String("collectionName", idx.CollectionName),
				logger.String("idxName", nameReturned),
				logger.Duration("time", time.Since(startTime)),
			)
		}
	}

	return true
}

func (a *DataStore) runEnsureIndexes(ctx context.Context, wg *sync.WaitGroup, indexGroupName string) {
	defer wg.Done()
	task.LogInfoStruct(taskName, "ensuring indexes", logger.String("indexGroupName", indexGroupName))

	a.RLock()
	maxBackoffSeconds := a.options.maxFailedEnsureIndexesBackoffSeconds
	a.RUnlock()

	var failedConnectBackoff = &backoff.Backoff{
		Min:    1000 * time.Millisecond,
		Max:    time.Second * time.Duration(maxBackoffSeconds),
		Factor: 2,
		Jitter: true,
	}
	for {
		okOrNoRetry := a.ensureIndexes(ctx, indexGroupName)
		if !okOrNoRetry {
			task.LogErrorStruct(taskName, "error ensuring indexes (will retry)")
		} else {
			return
		}
		select {
		case <-time.After(failedConnectBackoff.Duration()):
		case <-ctx.Done():
			task.LogInfoStruct(taskName, "ensure index runner stopped before complete (context cancelled)")
			return
		}
	}
}
