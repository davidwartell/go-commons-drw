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
	"encoding/binary"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/davidwartell/go-commons-drw/mongouuid"
	"github.com/davidwartell/go-commons-drw/task"
	"github.com/jpillora/backoff"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const indexNameDelim = "_"

//goland:noinspection GoUnusedConst
const ASC = 1

//goland:noinspection GoUnusedConst
const DESC = -1

var DirtyWriteError = errors.New("dirty write error")

type DirtyWriteProtectedFunc func() error
type IndexIdentifier string

type Index struct {
	CollectionName string
	Id             IndexIdentifier
	Version        uint64 // increment any time the model or options changes - calling createIndex() with the same name but different \
	// options than an existing index will throw an error MongoError: \
	// Index with name: **MongoIndexName** already exists with different options
	Model mongo.IndexModel
}

const (
	DefaultDatabaseName                         = "datastore"
	DefaultConnectTimeoutSeconds                = uint64(10)
	DefaultTimeoutSecondsShutdown               = uint64(30)
	DefaultTimeoutSecondsQuery                  = uint64(10)
	DefaultPingHeartbeatSeconds                 = uint64(10)
	DefaultMaxFailedEnsureIndexesBackoffSeconds = uint64(300)
	DefaultUsername                             = ""
	DefaultPassword                             = ""
	DefaultMaxPoolSize                          = uint64(100)
	DefaultHost                                 = "localhost:27017"
	taskName                                    = "Mongo DataStore"
	MaxSliceSizePerMongoDocument                = uint64(10 * 1024 * 1024)
)

type Options struct {
	databaseName                         string
	connectTimeoutSeconds                uint64
	timeoutSecondsShutdown               uint64
	timeoutSecondsQuery                  uint64
	pingHeartbeatSeconds                 uint64
	maxFailedEnsureIndexesBackoffSeconds uint64
	hosts                                []string
	username                             string
	password                             string
	maxPoolSize                          uint64
}

type DataStore struct {
	sync.RWMutex
	task.BaseTask
	started                 bool
	options                 *Options
	mongoClient             *mongo.Client
	mongoClientUnsafeFast   *mongo.Client
	mongoClientFastestReads *mongo.Client
	ctx                     context.Context
	cancel                  context.CancelFunc
	wg                      sync.WaitGroup
	managedIndexes          []Index
	managedIndexMap         map[string]Index
	managedIndexesLock      sync.RWMutex
}

var instance *DataStore
var once sync.Once

type DataStoreOption func(o *Options)

func Instance() *DataStore {
	once.Do(func() {
		instance = &DataStore{
			options: &Options{
				databaseName:                         DefaultDatabaseName,
				connectTimeoutSeconds:                DefaultConnectTimeoutSeconds,
				timeoutSecondsShutdown:               DefaultTimeoutSecondsShutdown,
				timeoutSecondsQuery:                  DefaultTimeoutSecondsQuery,
				pingHeartbeatSeconds:                 DefaultPingHeartbeatSeconds,
				maxFailedEnsureIndexesBackoffSeconds: DefaultMaxFailedEnsureIndexesBackoffSeconds,
				hosts:                                []string{DefaultHost},
				username:                             DefaultUsername,
				password:                             DefaultPassword,
				maxPoolSize:                          DefaultMaxPoolSize,
			},
			managedIndexMap: make(map[string]Index),
		}
	})
	return instance
}

// TruncateStringSliceForMongoDoc ensures a string slice will fit in the mongodb doc size limit and truncates the slice
// if necessary logging a warning.
func TruncateStringSliceForMongoDoc(slice []string) (newSlice []string) {
	var sizeOfSlice uint64
	for index, str := range slice {
		sizeOfSlice = sizeOfSlice + uint64(utf8.RuneCountInString(str))
		if sizeOfSlice > MaxSliceSizePerMongoDocument {
			logger.Instance().WarnfUnstruct("truncating slice of type []string from %d to %d to fit in %d bytes of mongo document", len(slice), index, MaxSliceSizePerMongoDocument)
			newSlice = slice[:index]
			return
		}
	}
	newSlice = slice
	return
}

// TruncateUUIDSliceForMongoDoc ensures a mongouuid.UUID slice will fit in the mongodb doc size limit and truncates the
// slice if necessary logging a warning.
func TruncateUUIDSliceForMongoDoc(slice []mongouuid.UUID) (newSlice []mongouuid.UUID) {
	sizeOfUUID := uint64(binary.Size(mongouuid.UUID{}))
	lenOfSlice := uint64(len(slice))
	sizeOfSlice := lenOfSlice * sizeOfUUID
	if sizeOfSlice > MaxSliceSizePerMongoDocument {
		allowedLength := MaxSliceSizePerMongoDocument / sizeOfUUID
		newSlice = slice[:allowedLength]
		logger.Instance().WarnfUnstruct("truncating slice of type []mongouuid.UUID from %d to %d to fit in %d bytes of mongo document", lenOfSlice, allowedLength, MaxSliceSizePerMongoDocument)
		return
	}
	newSlice = slice
	return
}

// CheckForDirtyWriteOnUpsert is expected to be used like this:
// filter := bson.D{
//		{"_id", device.Id},
//      // where device.DirtyWriteGuard is 0 on new or == to the dirtyWriteGuard field of the entity we expect in the collection
//		{"dirtyWriteGuard", device.DirtyWriteGuard},
//	}
//	update := bson.D{
//		{"$set", impl.toBson(device)},
//		{"$setOnInsert", impl.setOnInsertToBson(device)},
//	}
//	updateOptions := &options.UpdateOptions{}
//	var updateResult *mongo.UpdateResult
//	updateResult, err = deviceCollection.UpdateOne(ctx, filter, update, updateOptions.SetUpsert(true))
//	err = mongostore.CheckForDirtyWriteOnUpsert(updateResult, err)
//	if err != nil {
//		err2 := errors.Wrapf(err, "error on UpdateOne for Device: %v", err)
//		if err != mongostore.DirtyWriteError {
//			logger.Instance().ErrorIgnoreCancelUnstruct(ctx, err2)
//		}
//		return err2
//	}
//
// In the tested and expected case mongo will return E11000 duplicate key error collection in case of dirty write. This
// is because no document will exist that matches _id and dirtyWriteGuard causing mongo to attempt to insert a new document
// which will return duplicate key error.
// In case of no dirty write and no error returned by the UpdateOne() we expect either an insert (updateResult.UpsertedID
// has a value) or an updated existing document (updateResult.MatchedCount == 1).
func CheckForDirtyWriteOnUpsert(updateResult *mongo.UpdateResult, inputErr error) (err error) {
	if inputErr != nil {
		if IsDuplicateKeyError(inputErr) {
			err = DirtyWriteError
			return
		} else {
			err = inputErr
			return
		}
	}
	if updateResult.MatchedCount == 0 && updateResult.UpsertedID == nil {
		// Dirty Write error if filter did not match an existing document (including equality on dirtyWriteGuard field)
		// And no inserted document
		err = DirtyWriteError
		return
	}
	return
}

// RetryDirtyWrite is used by callers of functions that call CheckForDirtyWriteOnUpsert and can return DirtyWriteError.
// It will retry the anonymous function code up to 100 times before giving up if a dirty write error is detected.
// The caller of RetryDirtyWrite needs to ensure it has logic to refresh the copy of the object or objects its updating
// with a fresh copy from the collection.
//
//  Example:
//  // This code will be run repeatedly until there is no DirtyWriteError or the max retries is exceeded.
// 	err = mongostore.RetryDirtyWrite(func() error {
//		var retryErr error
//
//		// query an entity from the collection that has a dirtyWriteGuard model.Version (uint64) field
//		var existingDevice *model.Device
//		existingDevice, retryErr = imongo.Device.FindOne(apiCtx.Ctx(), device.Id)
//
//		// ...logic that makes changes existingDevice which could be stale
//
//		// imongo.Device.Upsert() can return DirtyWriteError
//		if retryErr = imongo.Device.Upsert(apiCtx.Ctx(), existingDevice, deviceEndpoint); retryErr != nil {
//			if retryErr != mongostore.DirtyWriteError {
//				logger.Instance().ErrorIgnoreCancelUnstruct(apiCtx.Ctx(), retryErr)
//			}
//			return retryErr
//		}
//		return nil
//	})
func RetryDirtyWrite(dirtyWriteFunc DirtyWriteProtectedFunc) (err error) {
	var retries uint64
	maxRetries := uint64(100)
	for {
		err = dirtyWriteFunc()
		if !errors.Is(err, DirtyWriteError) {
			// if error is not a DirtyWriteError give up retry
			break
		}
		retries++
		if retries > maxRetries {
			err = errors.Errorf("giving up retry after %d dirty writes", retries)
			break
		}
	}
	return
}

func (a *DataStore) StartTask(managedIndexes []Index, opts ...DataStoreOption) {
	a.Lock()
	defer a.Unlock()
	if a.started {
		return
	}
	task.LogInfo(taskName, "starting")
	a.ctx, a.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(a.options)
	}

	a.addManagedIndexes(managedIndexes)

	a.wg.Add(1)
	go a.runPing(a.ctx, &a.wg)

	a.wg.Add(1)
	go a.runEnsureIndexes(a.ctx, &a.wg)

	a.started = true
	task.LogInfo(taskName, "started")
}

func (a *DataStore) StopTask() {
	a.Lock()
	if !a.started {
		a.Unlock()
		return
	}
	a.Unlock()

	task.LogInfo(taskName, "shutting down")

	a.Lock()
	if a.cancel != nil {
		a.cancel()
	}
	a.Unlock()

	// don't hold the lock while waiting - cause a deadlock
	a.wg.Wait()

	a.Lock()
	defer a.Unlock()
	if !a.started {
		return
	}

	// disconnect from mongo
	var disconnectWg sync.WaitGroup

	disconnectWg.Add(1)
	go func() {
		defer disconnectWg.Done()
		if a.mongoClient != nil {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Duration(a.options.timeoutSecondsShutdown)*time.Second,
			)
			defer cancel()
			err := a.mongoClient.Disconnect(ctx)
			if err != nil {
				task.LogErrorf(taskName, "shutdown: error on disconnect of mongo client: %v", err)
			}
			a.mongoClient = nil
		}
	}()

	disconnectWg.Add(1)
	go func() {
		defer disconnectWg.Done()
		if a.mongoClientUnsafeFast != nil {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Duration(a.options.timeoutSecondsShutdown)*time.Second,
			)
			defer cancel()
			err := a.mongoClientUnsafeFast.Disconnect(ctx)
			if err != nil {
				task.LogErrorf(taskName, "shutdown: error on disconnect of mongo client: %v", err)
			}
			a.mongoClientUnsafeFast = nil
		}
	}()

	disconnectWg.Add(1)
	go func() {
		defer disconnectWg.Done()
		if a.mongoClientFastestReads != nil {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Duration(a.options.timeoutSecondsShutdown)*time.Second,
			)
			defer cancel()
			err := a.mongoClientFastestReads.Disconnect(ctx)
			if err != nil {
				task.LogErrorf(taskName, "shutdown: error on disconnect of mongo client: %v", err)
			}
			a.mongoClientFastestReads = nil
		}
	}()
	disconnectWg.Wait()

	a.started = false
	task.LogInfo(taskName, "stopped")
}

func (a *DataStore) databaseLinearWriteRead(ctx context.Context) (*mongo.Database, error) {
	client, err := a.clientLinearWriteRead(ctx)
	if err != nil {
		task.LogErrorf(taskName, "error getting collection from client: %v", err)
		return nil, err
	}
	a.RLock()
	dbName := a.options.databaseName
	a.RUnlock()
	return client.Database(dbName), nil
}

func (a *DataStore) databaseUnsafeFastWrites(ctx context.Context) (*mongo.Database, error) {
	client, err := a.clientUnsafeFastWrites(ctx)
	if err != nil {
		task.LogErrorf(taskName, "error getting collection from client: %v", err)
		return nil, err
	}
	a.RLock()
	dbName := a.options.databaseName
	a.RUnlock()
	return client.Database(dbName), nil
}

func (a *DataStore) databaseReadNearest(ctx context.Context) (*mongo.Database, error) {
	client, err := a.clientReadNearest(ctx)
	if err != nil {
		task.LogErrorf(taskName, "error getting collection from client: %v", err)
		return nil, err
	}
	a.RLock()
	dbName := a.options.databaseName
	a.RUnlock()
	return client.Database(dbName), nil
}

// Collection calls CollectionLinearWriteRead()
func (a *DataStore) Collection(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.CollectionLinearWriteRead(ctx, name)
}

// CollectionLinearWriteRead creates a connection with:
// - readconcern.Majority()
// - readpref.Primary()
// - writeconcern.J(true)
// - writeconcern.WMajority()
//
// This connection supplies: "Casual Consistency" in a sharded cluster inside a single client thread.
// https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#std-label-sessions
//
//
// Note: readpref.Primary() is critical for reads to consistently return results in the same go routine immediately
// after an insert.  And perhaps not well documented.
//
func (a *DataStore) CollectionLinearWriteRead(ctx context.Context, name string) (*mongo.Collection, error) {
	database, err := a.databaseLinearWriteRead(ctx)
	if err != nil {
		return nil, err
	}
	return database.Collection(name), nil
}

// CollectionUnsafeFastWrites creates a connection with:
// - readconcern.Available()
// - readpref.Nearest()
// - writeconcern.J(false)
// - writeconcern.W(1)
func (a *DataStore) CollectionUnsafeFastWrites(ctx context.Context, name string) (*mongo.Collection, error) {
	database, err := a.databaseUnsafeFastWrites(ctx)
	if err != nil {
		return nil, err
	}
	return database.Collection(name), nil
}

// CollectionReadNearest creates a connection with:
// - readconcern.Majority()
// - readpref.Nearest()
// - writeconcern.J(true)
// - writeconcern.WMajority()
func (a *DataStore) CollectionReadNearest(ctx context.Context, name string) (*mongo.Collection, error) {
	database, err := a.databaseReadNearest(ctx)
	if err != nil {
		return nil, err
	}
	return database.Collection(name), nil
}

func (a *DataStore) ContextTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	a.RLock()
	defer a.RUnlock()
	return context.WithTimeout(ctx, a.queryTimeout())
}

func (a *DataStore) Ping(ctx context.Context) error {
	defer task.HandlePanic(taskName)
	var err error
	var client *mongo.Client

	var cancel context.CancelFunc
	ctx, cancel = a.ContextTimeout(ctx)
	defer cancel()

	client, err = a.clientLinearWriteRead(ctx)
	if err != nil {
		task.LogErrorf(taskName, "error getting client for ping: %v", err)
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
		task.LogErrorf(taskName, "error getting collection for ping write test: %v", err)
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

// queryTimeout returns query timeout as time.Duration
// callers MUST hold a.Lock
func (a *DataStore) queryTimeout() time.Duration {
	return time.Duration(a.options.timeoutSecondsQuery) * time.Second
}

func (a *DataStore) runPing(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	task.LogInfo(taskName, "ping runner started")

	a.RLock()
	heartbeatSeconds := a.options.pingHeartbeatSeconds
	a.RUnlock()

	for {
		err := a.Ping(ctx)
		if err != nil {
			task.LogErrorf(taskName, "mongo ping failed: %v", err)
		}
		select {
		case <-time.After(time.Second * time.Duration(heartbeatSeconds)):
		case <-ctx.Done():
			task.LogInfo(taskName, "ping runner stopped")
			return
		}
	}
}

func (a *DataStore) runEnsureIndexes(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	task.LogInfo(taskName, "ensuring indexes")

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
		okOrNoRetry := a.ensureIndexes(ctx)
		if !okOrNoRetry {
			task.LogError(taskName, "error ensuring indexes (will retry)")
		} else {
			return
		}
		select {
		case <-time.After(failedConnectBackoff.Duration()):
		case <-ctx.Done():
			task.LogInfo(taskName, "ensure index runner stopped before complete")
			return
		}
	}
}

func (a *DataStore) AddAndEnsureManagedIndexes(ctx context.Context, addManagedIndexes []Index) (ok bool) {
	a.addManagedIndexes(addManagedIndexes)
	return a.ensureIndexes(ctx)
}

func (a *DataStore) Index(collectionName string, indexId IndexIdentifier) (idx Index, err error) {
	a.managedIndexesLock.RLock()
	defer a.managedIndexesLock.RUnlock()
	indexFullName := managedIndexId(collectionName, indexId)
	var exists bool
	idx, exists = a.managedIndexMap[indexFullName]
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

func (a *DataStore) addManagedIndexes(addManagedIndexes []Index) {
	a.managedIndexesLock.Lock()
	defer a.managedIndexesLock.Unlock()
	for _, idx := range addManagedIndexes {
		a.managedIndexMap[managedIndexId(idx.CollectionName, idx.Id)] = idx
	}

	var newManagedIndexes []Index
	for _, idx := range a.managedIndexMap {
		newManagedIndexes = append(newManagedIndexes, idx)
	}
	a.managedIndexes = newManagedIndexes
}

// Only return error if connect error.
func (a *DataStore) ensureIndexes(ctx context.Context) (okOrNoRetry bool) {
	defer task.HandlePanic(taskName)
	a.managedIndexesLock.Lock()
	defer a.managedIndexesLock.Unlock()

	err := a.Ping(ctx)
	if err != nil {
		task.LogErrorf(taskName, "ensure indexes: mongo ping failed aborting: %v", err)
		return false
	}

	collectionMapToindexNameMap := make(map[string]map[string]interface{})
	//
	// 1. Build map of collections and managed index names
	//
	var database *mongo.Database
	database, err = a.databaseLinearWriteRead(ctx)
	if err != nil {
		task.LogErrorf(taskName, "ensure indexes: mongo get database failed aborting: %v", err)
		return false
	}
	var collectionNames []string
	collectionNames, err = database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		task.LogErrorf(taskName, "ensure indexes: mongo list collection names failed aborting: %v", err)
		return false
	}
	for _, colName := range collectionNames {
		collectionMapToindexNameMap[colName] = make(map[string]interface{})
	}
	for _, idx := range a.managedIndexes {
		idxName := idx.MongoIndexName()
		if collectionMapToindexNameMap[idx.CollectionName] == nil {
			collectionMapToindexNameMap[idx.CollectionName] = make(map[string]interface{})
		}
		collectionMapToindexNameMap[idx.CollectionName][idxName] = struct{}{}
	}

	//
	// 2. Find any indexes that are not in our list of what we expect and drop them
	//
CollectionLoop:
	for collectionName := range collectionMapToindexNameMap {
		var collection *mongo.Collection
		collection, err = Instance().CollectionLinearWriteRead(ctx, collectionName)
		if err != nil {
			task.LogErrorf(taskName, "error getting collection %s to list indexes: %v", collectionName, err)
			continue
		}

		var cursor *mongo.Cursor
		cursor, err = collection.Indexes().List(ctx)
		if err != nil {
			task.LogErrorf(taskName, "error listing indexes on collection %s: %v", collectionName, err)
			continue
		}
		for cursor.Next(ctx) {
			indexDoc := bsoncore.Document{}

			if err = cursor.Decode(&indexDoc); err != nil {
				task.LogErrorf(taskName, "error on Decode index document for list indexes cursor on collection %s: %v", collectionName, err)
				_ = cursor.Close(ctx)
				continue CollectionLoop
			}

			nameVal, idErr := indexDoc.LookupErr("name")
			if idErr != nil {
				task.LogErrorf(taskName, "error on LookupErr of name field in index document for list indexes cursor on collection %s: %v", collectionName, err)
				_ = cursor.Close(ctx)
				continue CollectionLoop
			}
			nameStr, nameStrOk := nameVal.StringValueOK()
			if !nameStrOk {
				task.LogErrorf(taskName, "error on StringValueOK of name field in index document for list indexes cursor on collection %s: %v", collectionName, err)
				_ = cursor.Close(ctx)
				continue CollectionLoop
			}

			if nameStr == "_id_" {
				continue
			}

			if _, ok := collectionMapToindexNameMap[collectionName][nameStr]; !ok {
				startTime := time.Now()
				task.LogInfof(taskName, "begin drop index %s.%s", collectionName, nameStr)
				_, err = collection.Indexes().DropOne(ctx, nameStr)
				if err != nil {
					task.LogErrorf(taskName, "error dropping index %s.%s: %v", collectionName, nameStr, err)
				} else {
					task.LogInfof(taskName, "finished drop index %s.%s in %v", collectionName, nameStr, time.Since(startTime))
				}
			}
		}
		if cursor.Err() != nil {
			task.LogErrorf(taskName, "error on list indexes cursor on collection %s: %v", collectionName, err)
		}
		if cursor != nil {
			_ = cursor.Close(ctx)
		}
	}

	createIndexOptions := mongooptions.CreateIndexes().SetCommitQuorumMajority()
	//
	// 3. Attempt to create each index.  If the index already exists create will return and do nothing.
	//
	for _, idx := range a.managedIndexes {
		idxName := idx.MongoIndexName()
		if idx.Model.Options == nil {
			idx.Model.Options = mongooptions.Index()
		}
		idx.Model.Options = idx.Model.Options.SetName(idxName)

		var collection *mongo.Collection
		collection, err = Instance().CollectionLinearWriteRead(ctx, idx.CollectionName)
		if err != nil {
			task.LogErrorf(taskName, "error getting collection to ensure index %s.%s: %v", idx.CollectionName, idx.Id, err)
			continue
		}

		var nameReturned string
		startTime := time.Now()
		task.LogInfof(taskName, "begin ensuring index %s.%s", idx.CollectionName, nameReturned)
		nameReturned, err = collection.Indexes().CreateOne(ctx, idx.Model, createIndexOptions)
		if err != nil {
			task.LogErrorf(taskName, "error ensuring index %s.%s: %v", idx.CollectionName, idxName, err)
		} else {
			task.LogInfof(taskName, "finished ensuring index %s.%s in %v", idx.CollectionName, nameReturned, time.Since(startTime))
		}
	}

	return true
}

func (a *DataStore) unsafeFastClient(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = errors.New("getting mongo client failed service is not started or shutdown")
		return
	} else if a.mongoClientUnsafeFast != nil {
		client = a.mongoClientUnsafeFast
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectUnsafeFastWrites(ctx)
	return
}

func (a *DataStore) clientReadNearest(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = errors.New("getting mongo client failed service is not started or shutdown")
		return
	} else if a.mongoClientFastestReads != nil {
		client = a.mongoClientFastestReads
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectReadNearest(ctx)
	return
}

func (a *DataStore) connectReadNearest(clientCtx context.Context) (client *mongo.Client, err error) {
	a.Lock()
	defer a.Unlock()

	if a.mongoClientFastestReads != nil {
		client = a.mongoClientFastestReads
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(a.options.connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfo(taskName, "connecting to mongo")

	clientOptions := a.standardOptions()
	clientOptions.SetReadPreference(readpref.Nearest())
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(true), writeconcern.WMajority(), writeconcern.WTimeout(a.queryTimeout())))
	clientOptions.SetReadConcern(readconcern.Majority())

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClientFastestReads = client

	task.LogInfo(taskName, "connected to mongo")
	return
}

func (a *DataStore) clientLinearWriteRead(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = errors.New("getting mongo client failed service is not started or shutdown")
		return
	} else if a.mongoClient != nil {
		client = a.mongoClient
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectLinearWriteRead(ctx)
	return
}

func (a *DataStore) connectLinearWriteRead(clientCtx context.Context) (client *mongo.Client, err error) {
	a.Lock()
	defer a.Unlock()

	if a.mongoClient != nil {
		client = a.mongoClient
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(a.options.connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfo(taskName, "connecting to mongo")

	clientOptions := a.standardOptions()
	clientOptions.SetReadConcern(readconcern.Majority())
	clientOptions.SetReadPreference(readpref.Primary()) // connect primary for reads or linear reads in same go routine will some times fail to find documents you just inserted in same routine
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(true), writeconcern.WMajority(), writeconcern.WTimeout(a.queryTimeout())))

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClient = client

	task.LogInfo(taskName, "connected to mongo")
	return
}

func (a *DataStore) clientUnsafeFastWrites(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = errors.New("getting mongo client failed service is not started or shutdown")
		return
	} else if a.mongoClientUnsafeFast != nil {
		client = a.mongoClientUnsafeFast
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectUnsafeFastWrites(ctx)
	return
}

func (a *DataStore) connectUnsafeFastWrites(clientCtx context.Context) (client *mongo.Client, err error) {
	a.Lock()
	defer a.Unlock()

	if a.mongoClientUnsafeFast != nil {
		client = a.mongoClientUnsafeFast
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(a.options.connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfo(taskName, "connecting to mongo for unsafe/fast operations")

	clientOptions := a.standardOptions()
	clientOptions.SetReadPreference(readpref.Nearest())
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(false), writeconcern.W(1)))
	clientOptions.SetReadConcern(readconcern.Available())

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClientUnsafeFast = client

	task.LogInfo(taskName, "connected to mongo")
	return
}

// standardOptions sets up standard options consistent across all clients
// caller MUST hold a.Lock
func (a *DataStore) standardOptions() (clientOptions *mongooptions.ClientOptions) {
	clientOptions = mongooptions.Client().SetHosts(a.options.hosts)
	if a.options.username != "" {
		credentials := mongooptions.Credential{
			AuthMechanism: "PLAIN",
			Username:      a.options.username,
			Password:      a.options.password,
		}
		clientOptions.SetAuth(credentials)
	}
	clientOptions.SetRetryWrites(true)
	clientOptions.SetRetryReads(true)
	clientOptions.SetMaxPoolSize(a.options.maxPoolSize)
	clientOptions.SetMinPoolSize(1)
	return
}

func (idx Index) MongoIndexName() string {
	var sb strings.Builder
	sb.WriteString(idx.Id.String())
	sb.WriteString(indexNameDelim)
	sb.WriteString(strconv.FormatUint(idx.Version, 10))
	return sb.String()
}

func WithDatabaseName(databaseName string) DataStoreOption {
	return func(o *Options) {
		o.databaseName = databaseName
	}
}

func WithTimeoutSecondsShutdown(timeoutSecondsShutdown uint64) DataStoreOption {
	return func(o *Options) {
		o.timeoutSecondsShutdown = timeoutSecondsShutdown
	}
}

func WithTimeoutSecondsQuery(timeoutSecondsQuery uint64) DataStoreOption {
	return func(o *Options) {
		o.timeoutSecondsQuery = timeoutSecondsQuery
	}
}

func WithPingHeartbeatSeconds(pingHeartbeatSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.pingHeartbeatSeconds = pingHeartbeatSeconds
	}
}

func WithMaxFailedEnsureIndexesBackoffSeconds(maxFailedEnsureIndexesBackoffSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.maxFailedEnsureIndexesBackoffSeconds = maxFailedEnsureIndexesBackoffSeconds
	}
}

func WithHosts(hosts []string) DataStoreOption {
	return func(o *Options) {
		o.hosts = hosts
	}
}

func WithUsername(username string) DataStoreOption {
	return func(o *Options) {
		o.username = username
	}
}

func WithPassword(password string) DataStoreOption {
	return func(o *Options) {
		o.password = password
	}
}

func WithMaxPoolSize(maxPoolSize uint64) DataStoreOption {
	return func(o *Options) {
		o.maxPoolSize = maxPoolSize
	}
}

func WithConnectTimeoutSeconds(connectTimeoutSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.connectTimeoutSeconds = connectTimeoutSeconds
	}
}

func IsDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	} else if bulkWriteErr, ok := err.(mongo.BulkWriteException); ok && bulkWriteErr.WriteConcernError == nil {
		for _, writeError := range bulkWriteErr.WriteErrors {
			if writeError.Code != 11000 {
				return false
			}
		}
		return true
	} else if writeException, ok := err.(mongo.WriteException); ok && writeException.WriteConcernError == nil {
		for _, writeError := range writeException.WriteErrors {
			if writeError.Code != 11000 {
				return false
			}
		}
		return true
	} else {
		return false
	}
}
