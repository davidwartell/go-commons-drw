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

//goland:noinspection SpellCheckingInspection
import (
	"context"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/davidwartell/go-commons-drw/task"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrorServiceNotStarted = errors.New("getting mongo client failed: service is not started or shutdown")

const (
	DefaultDatabaseName                         = "datastore"
	DefaultConnectTimeoutSeconds                = uint64(10)
	DefaultTimeoutSecondsShutdown               = uint64(10)
	DefaultTimeoutSecondsQuery                  = uint64(10)
	DefaultPingHeartbeatSeconds                 = uint64(10)
	DefaultMaxFailedEnsureIndexesBackoffSeconds = uint64(300)
	DefaultUsername                             = ""
	DefaultPassword                             = ""
	DefaultAuthMechanism                        = "PLAIN"
	DefaultMaxPoolSize                          = uint64(100)
	DefaultHost                                 = "localhost:27017"
	taskName                                    = "Mongo DataStore"
)

type Options struct {
	databaseName                         string
	connectTimeoutSeconds                uint64
	timeoutSecondsShutdown               uint64
	timeoutSecondsQuery                  uint64
	pingHeartbeatSeconds                 uint64
	maxFailedEnsureIndexesBackoffSeconds uint64
	hosts                                []string
	uri                                  string
	username                             string
	password                             string
	authMechanism                        string // Supported values include "SCRAM-SHA-256", "SCRAM-SHA-1", "MONGODB-CR", "PLAIN", "GSSAPI", "MONGODB-X509", and "MONGODB-AWS".
	maxPoolSize                          uint64
}

type DataStore struct {
	sync.RWMutex
	started                           bool
	options                           *Options
	mongoClientForWatch               *mongo.Client
	mongoClientLinearReadWrite        *mongo.Client
	mongoClientUnsafeFast             *mongo.Client
	mongoClientReadNearest            *mongo.Client
	mongoClientReadSecondaryPreferred *mongo.Client
	ctx                               context.Context
	cancel                            context.CancelFunc
	wg                                sync.WaitGroup
	managedIndexes                    []indexGroup
	allIndexesByPath                  map[string]Index // [managedIndexId(idx.CollectionName, idx.Id)] -> Index
	managedIndexesLock                sync.RWMutex
}

var instance *DataStore
var once sync.Once

type DataStoreOption func(o *Options)

// Instance returns an instance of the data store singleton. This ensures you only have once instance of this per program.
// All connections will be polled automatically and you have no work for startup or cleaning up connections than
// running Instance().StartTask() and Instance().StopTask().
//
// The singleton is multithreading safe.  Reference anywhere you need a connection to mongo or want to add Indexes after startup: e.g.
// mongostore.Instance().CollectionLinearWriteRead(...)
// mongostore.Instance().AddAndEnsureManagedIndexes(...)
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
				uri:                                  "",
				username:                             DefaultUsername,
				password:                             DefaultPassword,
				authMechanism:                        DefaultAuthMechanism,
				maxPoolSize:                          DefaultMaxPoolSize,
			},
			allIndexesByPath: make(map[string]Index),
		}
	})
	return instance
}

// StartTask starts the background routines.  Call this once on startup from your main.go.
// Call StopTask() on exit.
// Indexes supplied here will be managed in a separate go routine after startup.
func (a *DataStore) StartTask(managedIndexes []Index, opts ...DataStoreOption) {
	a.Lock()
	defer a.Unlock()
	if a.started {
		return
	}
	task.LogInfoStruct(taskName, "starting")
	a.ctx, a.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(a.options)
	}

	a.wg.Add(1)
	go a.runPing(a.ctx, &a.wg)

	if len(managedIndexes) > 0 {
		a.addManagedIndexes(startupIndexGroupName, managedIndexes)
		a.wg.Add(1)
		go a.runEnsureIndexes(a.ctx, &a.wg, startupIndexGroupName)
	}

	a.started = true
	task.LogInfoStruct(taskName, "started")
}

// StopTask disconnects the mongo clients and stops the background routines.  Call this once on exit of your main.go.
func (a *DataStore) StopTask() {
	a.Lock()
	if !a.started {
		a.Unlock()
		return
	}
	a.Unlock()

	task.LogInfoStruct(taskName, "shutting down")

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
	go disconnectClient(&disconnectWg, a.mongoClientLinearReadWrite, a.options.timeoutSecondsShutdown)

	disconnectWg.Add(1)
	go disconnectClient(&disconnectWg, a.mongoClientUnsafeFast, a.options.timeoutSecondsShutdown)

	disconnectWg.Add(1)
	go disconnectClient(&disconnectWg, a.mongoClientReadNearest, a.options.timeoutSecondsShutdown)

	disconnectWg.Add(1)
	go disconnectClient(&disconnectWg, a.mongoClientReadSecondaryPreferred, a.options.timeoutSecondsShutdown)

	disconnectWg.Add(1)
	go disconnectClient(&disconnectWg, a.mongoClientForWatch, a.options.timeoutSecondsShutdown)

	disconnectWg.Wait()
	a.mongoClientUnsafeFast = nil
	a.mongoClientReadNearest = nil
	a.mongoClientLinearReadWrite = nil
	a.mongoClientReadSecondaryPreferred = nil
	a.mongoClientForWatch = nil

	a.started = false
	task.LogInfoStruct(taskName, "stopped")
}

func disconnectClient(wg *sync.WaitGroup, client *mongo.Client, timeoutSecondsShutdown uint64) {
	defer wg.Done()
	if client != nil {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(timeoutSecondsShutdown)*time.Second,
		)
		defer cancel()
		err := client.Disconnect(ctx)
		if err != nil {
			task.LogErrorStruct(taskName, "shutdown: error on disconnect of mongo client", logger.Error(err))
		}
	}
}

func (a *DataStore) databaseLinearWriteRead(ctx context.Context) (*mongo.Database, error) {
	client, err := a.clientLinearWriteRead(ctx)
	if err != nil {
		task.LogErrorStruct(taskName, "error getting collection from client", logger.Error(err))
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
		task.LogErrorStruct(taskName, "error getting collection from client", logger.Error(err))
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
		task.LogErrorStruct(taskName, "error getting collection from client", logger.Error(err))
		return nil, err
	}
	a.RLock()
	dbName := a.options.databaseName
	a.RUnlock()
	return client.Database(dbName), nil
}

func (a *DataStore) databaseReadSecondaryPreferred(ctx context.Context) (*mongo.Database, error) {
	client, err := a.clientReadSecondaryPreferred(ctx)
	if err != nil {
		task.LogErrorStruct(taskName, "error getting collection from client", logger.Error(err))
		return nil, err
	}
	a.RLock()
	dbName := a.options.databaseName
	a.RUnlock()
	return client.Database(dbName), nil
}

func (a *DataStore) databaseForWatch(ctx context.Context) (*mongo.Database, error) {
	client, err := a.clientForWatch(ctx)
	if err != nil {
		task.LogErrorStruct(taskName, "error getting collection from client", logger.Error(err))
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
// - readconcern.Linearizable()
// - readpref.Primary()
// - writeconcern.J(true)
// - writeconcern.WMajority()
//
// This connection supplies: "Casual Consistency" in a sharded cluster inside a single client thread.
// https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#std-label-sessions
//
// Note: readpref.Primary() is critical for reads to consistently return results in the same go routine immediately
// after an insert.  And perhaps not well documented.
func (a *DataStore) CollectionLinearWriteRead(ctx context.Context, name string) (*mongo.Collection, error) {
	database, err := a.databaseLinearWriteRead(ctx)
	if err != nil {
		return nil, err
	}
	return database.Collection(name), nil
}

// CollectionUnsafeFastWrites creates a connection with:
// - readconcern.Local()
// - readpref.Primary()
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

// CollectionReadSecondaryPreferred creates a connection with:
// - readconcern.Majority()
// - readpref.SecondaryPreferred()
// - writeconcern.J(true)
// - writeconcern.WMajority()
func (a *DataStore) CollectionReadSecondaryPreferred(ctx context.Context, name string) (*mongo.Collection, error) {
	database, err := a.databaseReadSecondaryPreferred(ctx)
	if err != nil {
		return nil, err
	}
	return database.Collection(name), nil
}

// CollectionForWatch creates a connection with:
// - readconcern.Majority()
// - readpref.SecondaryPreferred()
// - writeconcern.J(true)
// - writeconcern.WMajority()
//
// This is recommended for use with Change Streams (Watch()).  The write concerns are just in case you use it for writes by accident.
func (a *DataStore) CollectionForWatch(ctx context.Context, name string) (*mongo.Collection, error) {
	database, err := a.databaseForWatch(ctx)
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

// queryTimeout returns query timeout as time.Duration
// callers MUST hold a.Lock
func (a *DataStore) queryTimeout() time.Duration {
	return time.Duration(a.options.timeoutSecondsQuery) * time.Second
}

//nolint:golint,unused
func (a *DataStore) unsafeFastClient(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = ErrorServiceNotStarted
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
		err = ErrorServiceNotStarted
		return
	} else if a.mongoClientReadNearest != nil {
		client = a.mongoClientReadNearest
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectReadNearest(ctx)
	return
}

func (a *DataStore) clientReadSecondaryPreferred(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = ErrorServiceNotStarted
		return
	} else if a.mongoClientReadSecondaryPreferred != nil {
		client = a.mongoClientReadSecondaryPreferred
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectReadSecondaryPreferred(ctx)
	return
}

func (a *DataStore) connectReadNearest(clientCtx context.Context) (client *mongo.Client, err error) {
	a.Lock()
	defer a.Unlock()

	if a.mongoClientReadNearest != nil {
		client = a.mongoClientReadNearest
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(a.options.connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfoStruct(taskName, "connecting to mongo")

	clientOptions := a.standardOptions()
	clientOptions.SetReadPreference(readpref.Nearest())
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(true), writeconcern.WMajority(), writeconcern.WTimeout(a.queryTimeout())))
	clientOptions.SetReadConcern(readconcern.Majority())

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClientReadNearest = client

	task.LogInfoStruct(taskName, "connected to mongo")
	return
}

func (a *DataStore) connectReadSecondaryPreferred(clientCtx context.Context) (client *mongo.Client, err error) {
	a.Lock()
	defer a.Unlock()

	if a.mongoClientReadSecondaryPreferred != nil {
		client = a.mongoClientReadSecondaryPreferred
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(a.options.connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfoStruct(taskName, "connecting to mongo")

	clientOptions := a.standardOptions()
	clientOptions.SetReadPreference(readpref.SecondaryPreferred())
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(true), writeconcern.WMajority(), writeconcern.WTimeout(a.queryTimeout())))
	clientOptions.SetReadConcern(readconcern.Majority())

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClientReadSecondaryPreferred = client

	task.LogInfoStruct(taskName, "connected to mongo")
	return
}

func (a *DataStore) clientLinearWriteRead(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = ErrorServiceNotStarted
		return
	} else if a.mongoClientLinearReadWrite != nil {
		client = a.mongoClientLinearReadWrite
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectLinearWriteRead(ctx)
	return
}

func (a *DataStore) clientForWatch(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = ErrorServiceNotStarted
		return
	} else if a.mongoClientForWatch != nil {
		client = a.mongoClientForWatch
		a.RUnlock()
		return
	} else {
		a.RUnlock()
	}

	client, err = a.connectForWatch(ctx)
	return
}

func (a *DataStore) connectForWatch(clientCtx context.Context) (client *mongo.Client, err error) {
	a.Lock()
	defer a.Unlock()

	if a.mongoClientForWatch != nil {
		client = a.mongoClientForWatch
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(a.options.connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfoStruct(taskName, "connecting to mongo")

	clientOptions := a.standardOptions()
	clientOptions.SetReadConcern(readconcern.Majority())
	clientOptions.SetReadPreference(readpref.SecondaryPreferred())
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(true), writeconcern.WMajority(), writeconcern.WTimeout(a.queryTimeout())))

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClientForWatch = client

	task.LogInfoStruct(taskName, "connected to mongo")
	return
}

func (a *DataStore) connectLinearWriteRead(clientCtx context.Context) (client *mongo.Client, err error) {
	a.Lock()
	defer a.Unlock()

	if a.mongoClientLinearReadWrite != nil {
		client = a.mongoClientLinearReadWrite
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(a.options.connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfoStruct(taskName, "connecting to mongo")

	clientOptions := a.standardOptions()
	clientOptions.SetReadConcern(readconcern.Linearizable())
	clientOptions.SetReadPreference(readpref.Primary()) // connect primary for reads or linear reads in same go routine will some times fail to find documents you just inserted in same routine
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(true), writeconcern.WMajority(), writeconcern.WTimeout(a.queryTimeout())))

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClientLinearReadWrite = client

	task.LogInfoStruct(taskName, "connected to mongo")
	return
}

func (a *DataStore) clientUnsafeFastWrites(ctx context.Context) (client *mongo.Client, err error) {
	a.RLock()
	if !a.started {
		a.RUnlock()
		err = ErrorServiceNotStarted
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

	task.LogInfoStruct(taskName, "connecting to mongo for unsafe/fast operations")

	clientOptions := a.standardOptions()
	clientOptions.SetReadPreference(readpref.Primary()) // read from primary for linear reads
	clientOptions.SetWriteConcern(writeconcern.New(writeconcern.J(false), writeconcern.W(1)))
	clientOptions.SetReadConcern(readconcern.Local())

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	a.mongoClientUnsafeFast = client

	task.LogInfoStruct(taskName, "connected to mongo")
	return
}

// standardOptions sets up standard options consistent across all clients
// caller MUST hold a.Lock
func (a *DataStore) standardOptions() (clientOptions *mongooptions.ClientOptions) {
	if len(a.options.uri) > 0 {
		clientOptions = mongooptions.Client().ApplyURI(a.options.uri)
	} else {
		clientOptions = mongooptions.Client().SetHosts(a.options.hosts)
	}
	if a.options.username != "" {
		credentials := mongooptions.Credential{
			AuthMechanism: a.options.authMechanism,
			Username:      a.options.username,
			Password:      a.options.password,
		}
		clientOptions.SetAuth(credentials)
	}
	clientOptions.SetRetryWrites(true)
	clientOptions.SetRetryReads(true)
	clientOptions.SetMaxPoolSize(a.options.maxPoolSize)
	clientOptions.SetMinPoolSize(1)
	clientOptions.SetCompressors([]string{"snappy"})
	return
}

func (idx Index) MongoIndexName() string {
	var sb strings.Builder
	sb.WriteString(idx.Id.String())
	sb.WriteString(indexNameDelim)
	sb.WriteString(strconv.FormatUint(idx.Version, 10))
	return sb.String()
}

//goland:noinspection GoUnusedExportedFunction
func WithDatabaseName(databaseName string) DataStoreOption {
	return func(o *Options) {
		o.databaseName = databaseName
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithTimeoutSecondsShutdown(timeoutSecondsShutdown uint64) DataStoreOption {
	return func(o *Options) {
		o.timeoutSecondsShutdown = timeoutSecondsShutdown
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithTimeoutSecondsQuery(timeoutSecondsQuery uint64) DataStoreOption {
	return func(o *Options) {
		o.timeoutSecondsQuery = timeoutSecondsQuery
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithPingHeartbeatSeconds(pingHeartbeatSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.pingHeartbeatSeconds = pingHeartbeatSeconds
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithMaxFailedEnsureIndexesBackoffSeconds(maxFailedEnsureIndexesBackoffSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.maxFailedEnsureIndexesBackoffSeconds = maxFailedEnsureIndexesBackoffSeconds
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithHosts(hosts []string) DataStoreOption {
	return func(o *Options) {
		o.hosts = hosts
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithUri(uri string) DataStoreOption {
	return func(o *Options) {
		o.uri = uri
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithUsername(username string) DataStoreOption {
	return func(o *Options) {
		o.username = username
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithPassword(password string) DataStoreOption {
	return func(o *Options) {
		o.password = password
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithAuthMechanism(authMechanism string) DataStoreOption {
	return func(o *Options) {
		o.authMechanism = authMechanism
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithMaxPoolSize(maxPoolSize uint64) DataStoreOption {
	return func(o *Options) {
		o.maxPoolSize = maxPoolSize
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithConnectTimeoutSeconds(connectTimeoutSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.connectTimeoutSeconds = connectTimeoutSeconds
	}
}

func IsIndexNotFoundError(err error) bool {
	if err == nil {
		return false
	} else if commandErr, ok := err.(mongo.CommandError); ok {
		return commandErr.Code == 27 // Mongo Error Code 27 IndexNotFound
	} else {
		return false
	}
}
