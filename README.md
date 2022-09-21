# go-commons-drw

golang commons libs I have written or curated+bug-fixed.  See README files in packages. Apache License.

### Prerequisites

HomeBrew: https://brew.sh/

```
brew install golangci/tap/golangci-lint
```

### Building
```
make setup
make all
```

### Building for Release
```
make setup
make release all
```

### Unit Tests
```
make test
```

* Linter

```
make golint
```

* Security Linter

```
make gosec
```

### grpcpool
An interface for pooling GRPC client connections. With an implementation that multiplexes client requests to the server over a shared http2 socket.
GRPC client connection multiplexer 

Example:
```
func main() {
    mutualTLSConnectionFactory, err := grpcpool.NewMutualTLSFactory(
        []byte("FIXME"),                        // PEM encoded CA cert string used to verify server cert signature during handshake
        []byte("FIXME"),                        // PEM encoded client certificate string
        []byte("FIXME"),                        // PEM encoded client private RSA key
        "localhost:8080",                       // TCP connection string of server
        
        // After a duration of this time if the pool doesn't see any activity it tests the connection with the server to 
        // see if the transport is still alive. If set below 10s, a minimum value of 10s will be used instead.
        time.Second*time.Duration(30),          
        
        // After having pinged for keepalive check, the client waits for a duration of this timeout and if no activity is seen 
        // even after that the connection is forcibly closed.  Keep in mind on Linux & Windows you will never know if a 
        // connection is alive until you try to write to it.
        time.Second*time.Duration(10),
        
        // pingConn function pointer to test GRPC connections (see below)
        pingConn,
    )
    if err != nil {
        err = errors.Errorf("error loading mutual TLS connection enrollmentFactory for site (enrollment): %v", err)
        panic(err)
    }
    
    // you probably want one pool instance shared by your whole app
    pool := shared.New(
        // Your connectiuon factory.  I have supplied mutual-TLS, other connection types are possible by implementing: 
        // type ConnectionFactory interface {
        //      NewConnection(ctx context.Context) (*grpc.ClientConn, error)
        //      ConnectionOk(ctx context.Context, conn *grpc.ClientConn) error
        // }
        
        // if GRPC connection is idle for longer than this it will be closed even if its good
        time.Second*time.Duration(5*60)
    )
    // the pool should be closed on shutdown of your app
    defer func() {
        if pool != nil {
            pool.Close()
            pool = nil
        }
    }()
    
    ctx := context.Background() // FIXME
    var conn grpcpool.PoolClientConn
    conn, err = pool.Get(ctx)
    if err != nil {
		panic(err)
	}
	defer func() {
		if conn != nil {
			conn.Return()
		}
	}()
	
	// do something useful with the connection to send GRPC request with: conn.Connection()
	
	return
}

// pingConn should implement a GRPC ping/pong to the other side of conn.  Returns err or latency.
func pingConn(ctx context.Context, conn *grpc.ClientConn) (time.Duration, error) {
	panic("unimplemented")
}
```

### httpcommon
Code for http clients and standing up http proxy servers in golang in front of your client to control aspects of the request.

### launchd
MacOS Launch Daemon tooling.
A facade and extra tooling around a fork of https://github.com/DHowett/go-plist that has changes I found necessary for control of MacOS Launch Daemon on customer computers.

### logger
Tooling and facade around https://github.com/uber-go/zap I needed to allow my application to log to a configurable number of loggers and a backup for when it goes wrong.
After swapping logging libs once I wanted a facade to make it easy in the future that supported both structured & unstructured.
Functions for both unstructured and structured logging, panic handling and special handling for cancelled contexts (don't log errors on normal network disconnects).
Unstructured functions are marked Deprecated.

```
// start the logging service and configure name of your app used in log file names if you enable logging to files
logger.Instance().StartTask(logger.WithProductNameShort("example"))        
defer func() {
    // make sure the log buffers are flushed/synced
    _ = logger.Instance().StopTask
}()
isDevEnv := true
if isDevEnv {
    logger.Instance().SetConsoleLogging(true)
}
```

### mongoelector (experimental)
I have a special use case where I needed leader election and relying on an off the shelf DLM (distributed lock manager) or Distributed K/V store like Zookeeper or Consul is not an option.
Experimental use at your own risk.
```
// Creates a new instance of Elector for the given boundary.
// ctx - your context
// boundary - a unique case-sensitive string (conventionally a path). Only one election can take place in a boundary at a time.
// database - a mongodb database name (default: "Elector").  Must be shared by all Electors in a boundary. I suggest be shared by all Electors across all boundaries.
// leaderWorker - leaderWorker.Start() is called when this instance wins an election.  leaderWorker.Stop() is called when this instance loses an election
// followerWorker - followerWorker.Start() is called when this instance loses an election.  followerWorker.Stop() is called when this instance wins an election
// thisInstanceLeaderHostname - a hostname that will be passed to followers they can use to connect to a service on the leader, can be empty
// thisInstanceLeaderPort - a port that will be passed to followers they can use to connect to a service on the leader, can be empty
// type LeaderWorker interface {
// 	// Start the worker. May be called multiple times in a row.
// 	Start(ctx context.Context)
// 
// 	// Stop the worker. May be called multiple times in a row.
// 	Stop()
// }
// 
// type FollowerWorker interface {
// 	// Start the worker. May be called multiple times in a row.
// 	Start(ctx context.Context, electedLeader *ElectedLeader, thisLeaderUUID *mongouuid.UUID)
// 
// 	// Stop the worker. May be called multiple times in a row.
// 	Stop()
// }
elector, err := mongoelector.NewElector(
    ctx,  
    mongostore.Instance(),
    boundary,
    leaderWorker,
    followerWorker,
    "host.docker.internal",
    uint64(8081),
    mongoelector.NewElectorOptions(),
)
```

### mongostore
Lib for managing connections, indexes, and heartbeat for MongoDB clients:
* automatically manage indexes dynamically at startup
* indexes that are found on the MongoDB cluster but not in ManagedIndexes are automatically deleted.
* indexes that do not exist on the MongoDB cluster but are found in ManagedIndexes are automatically created
* indexes found in both MongoDB cluster and ManagedIndexes are updated if they changed signature and version
* background task that keeps a periodic heartbeat to mongo and logs any failures.  Useful for troubleshooting intermittent database connectivity problems.
* handling of large batch find() operations where searching for documents with a field in a large array and the size of the array is not known and could exceed bson document limit.
* error handling and retry of dirty writes

```
var options []mongostore.DataStoreOption
options = append(options, mongostore.WithDatabaseName("demo"))
options = append(options, mongostore.WithHosts([]string{"host.docker.internal:27017", "host.docker.internal:27018", "host.docker.internal:27019"}))
options = append(options, mongostore.WithMaxPoolSize(uint64(100)))
options = append(options, mongostore.WithUsername("FIXME dont put passwords in code"))
options = append(options, mongostore.WithPassword("FIXME dont put passwords in code"))
mongostore.Instance().StartTask(managedIndexes, options...)
defer mongostore.Instance().StopTask()

// Collection returns a mongo collection setup for journaling and safe writes.  Alternatively use CollectionUnsafeFast() when data integrity is not important (eg logs).
var err error
var cancel context.CancelFunc
// put a time limit on your database request(s)
ctx, cancel = mongostore.Instance().ContextTimeout(ctx)
defer cancel()

var myCollection *mongo.Collection
myCollection, err = mongostore.Instance().Collection(ctx, "MyCollection")
if err != nil {
    return err
}

// no need return collections but you DO NEED to close mongo cursors if you use them
var cursor *mongo.Cursor
cursor, err = collection.Find(ctx, FIXME)
if err != nil {
    return err
}
defer func() {
    if cursor != nil {
        _ = cursor.Close(ctx)
    }
}()
```

managedIndexes is a map of index name to mongo index.

```
// type Index struct {
// 	CollectionName string
// 	IndexName      string
// 	Version        uint64 // increment any time the model or options changes - calling createIndex() with the same name but different options than an existing index will throw an error MongoError: Index with name: **indexName** already exists with different options
// 	Model mongo.IndexModel
// }

// ManagedIndexes map of index name to index model
var ManagedIndexes = []mongostore.Index{
	{   // index on field named commandId
		CollectionName: "SomeCollection",
		IndexName:      "commandId",
		Version:        0,
		Model: mongo.IndexModel{
			Keys: bsonx.Doc{
				{Key: "commandId", Value: bsonx.Int32(mongostore.ASC)},
			},
		},
	},
	{ // Delete documents that have expired
		CollectionName: "AnotherCollection",
		IndexName:      "timestamp_ttl",
		Version:        0,
		Model: mongo.IndexModel{
			Options: options.Index().SetExpireAfterSeconds(60 * 60 * 24 * 90), // delete after 90 days
			Keys: bsonx.Doc{
				{Key: "timestamp", Value: bsonx.Int32(mongostore.DESC)},
			},
		},
	},
}
```

### mongouuid
Wrapped mongo-driver/bson/primitive.ObjectId so you can abstract it across APIs

### observer 
Implementation of observer pattern with deduplication of events in the context of a time box for debouncing. Notification is handled in background via go routines. 

```
// new observer with 30 second debounce.  Duration is any valid time.Duration or if 0 then debounce is disabled.
observer := observer.NewBufferedSetObserver(30 * time.Second) 

// observers should be closed before exiting the app
defer func() {
    if observer != nil {
        _ = observer.Close(ctx)
    }
}()

// add a listener function to be called when there are events, you can add any number of listeners
observer.AddListener(
    func() {(ctx context.Context, events []string) {
        // do something when you observe events
    }()
)

// emmit an event to all listeners
observer.Emit(device.Id)
```

### onecontext
Merge multiple context.Context.  I have run across a need for this at least two dozen times.  A common case is when you need to cleanly shutdown an HTTP server in go and you need a way to interrupt all of the requests no matter their state.  And at same time have individual contexts per request.
At start of each request by merging the globalHTTPServerCtx and your request based ctx you get the result.
https://github.com/teivah/onecontext/ - I found some defects under higher rates of events and re-wrote parts of it.
```
ctx, cancel := onecontext.Merge(ctx1, ctx2)
```

### profiler
Scaffolding for doing golang cpu and memory profiling.

### systemsservice
Facade and scaffolding for windows background services and macos launch daemon.

### timeoutlock
A Mutex for long-running requests where Lock can be interrupted by canceling a context.

```
// create a lock
runLock := timeoutlock.NewMutex()

if success := runLock.Lock(ctx); !success {
    return errors.New("error gave up trying to aquiring lock for foo")
}
// do something here you need a lock for
defer runLock.Unlock()
```

### watchdog 
Watchdog will call the cancel function associated with the returned context when a max run time has been exceeded.  
Pass the context returned to a long-running tasks that can be interrupted by a cancelled context.

```
// create a new watchdog
watchDog, watchDogCtx := watchdog.NewWatchDog(
    ctx,
    time.Duration(300) * time.Second,
)
defer watchDog.Cancel()

// SomeLongRunningFunction that we want to run no more than 300 seconds.  Function should exit if watchDogCtx is cancelled.
// Use context aware network/database connections in this function.  If you have long running computations in a loop
// periodically check if watchDogCtx.Err() != nil in SomeLongRunningFunction.
SomeLongRunningFunction(watchDogCtx)
```

