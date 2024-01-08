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
make lint
```

* Security Linter

```
make gosec
```

### launchd
MacOS Launch Daemon tooling.
A facade and extra tooling around a fork of https://github.com/DHowett/go-plist that has changes I found necessary for control of MacOS Launch Daemon on customer computers.

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

### profiler
Scaffolding for doing golang cpu and memory profiling.

### systemsservice
Facade and scaffolding for windows background services and macos launch daemon.

## Contributing

Happy to accept PRs.

# Author

**davidwartell**

* <http://github.com/davidwartell>
* <http://linkedin.com/in/wartell>

## License

Released under the [Apache License](https://github.com/davidwartell/go-commons-drw/blob/master/LICENSE).
