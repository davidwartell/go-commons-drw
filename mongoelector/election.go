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

// Package mongoelector
// Leader election using mongodb.
package mongoelector

import (
	"context"
	"github.com/davidwartell/go-commons-drw/logger"
	"github.com/davidwartell/go-commons-drw/mongostore"
	"github.com/davidwartell/go-commons-drw/mongouuid"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const electorLogPrefix = "Elector"
const collectionName = "Elector"
const defaultLeaderHeartbeatSeconds = uint64(10)
const defaultLeaderTTLSeconds = 2 * defaultLeaderHeartbeatSeconds

var contextCancelledError = errors.New("context cancelled")

type LeaderWorker interface {
	// Start the worker and return. You are responsible for managing your own go routine. May be called multiple times in a row.
	Start(ctx context.Context)

	// Stop the worker and return. You are responsible for managing your own go routine. May be called multiple times in a row.
	Stop()
}

type FollowerWorker interface {
	// Start the worker. May be called multiple times in a row.
	Start(ctx context.Context, thisLeaderUUID mongouuid.UUID)

	// Stop the worker. May be called multiple times in a row.
	Stop()
}

type Elector struct {
	rwMutex            sync.RWMutex
	started            bool
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	database           *mongostore.DataStore
	config             electorConfig
	leaderManagerWg    sync.WaitGroup
	leaderQueryChan    chan<- struct{}
	leaderResponseChan <-chan *ElectedLeader
	newLeaderChan      chan<- *ElectedLeader
}

type electorConfig struct {
	boundary                   string
	thisLeaderUUID             mongouuid.UUID
	thisInstanceLeaderHostname string
	thisInstanceLeaderPort     uint64
	leaderWorker               LeaderWorker
	followerWorker             FollowerWorker
	options                    ElectorOptions
}

type ElectedLeader struct {
	Boundary       string         `bson:"_id" json:"boundary"`                  // boundary
	TTLExpire      time.Time      `bson:"ttlExpire" json:"ttlExpire"`           // the leader must reset this value before it expires and another node wins the election
	LeaderUUID     mongouuid.UUID `bson:"leaderUUID" json:"leaderUUID"`         // unique id generated by each Elector node at startup
	LeaderHostname string         `bson:"leaderHostname" json:"leaderHostname"` // hostname of the leader used by followers to connect to the leader for whatever service it provides
	LeaderPort     uint64         `bson:"leaderPort" json:"leaderPort"`         // port of the leader used by followers to connect to the leader for whatever service it provides
}

type ElectorOptions struct {
	LeaderTTLSeconds       uint64
	LeaderHeartbeatSeconds uint64
}

type ElectorStatus struct {
	Id            mongouuid.UUID `json:"id"`
	Boundary      string         `json:"boundary"`
	Hostname      string         `json:"hostname"`
	Port          uint64         `json:"port"`
	IsLeader      bool           `json:"isLeader"`
	ElectedLeader *ElectedLeader `json:"electedLeader"`
}

func (el ElectedLeader) ConnectionString() string {
	return net.JoinHostPort(el.LeaderHostname, strconv.FormatUint(el.LeaderPort, 10))
}

//goland:noinspection GoUnusedExportedFunction
func NewElectorOptions() ElectorOptions {
	return ElectorOptions{
		LeaderTTLSeconds:       defaultLeaderTTLSeconds,
		LeaderHeartbeatSeconds: defaultLeaderHeartbeatSeconds,
	}
}

// NewElector
// Creates a new instance of Elector for the given boundary.
// boundary - a unique case-sensitive string (conventionally a path). Only one election can take place in a boundary at a time.
// database - a mongodb database.  Must be shared by all Electors in a boundary. Should be shared by all Electors across all boundaries.
// leaderWorker - leaderWorker.Start() is called when this instance wins an election.  leaderWorker.Close() is called when this instance loses an election
// followerWorker - followerWorker.Start() is called when this instance loses an election.  followerWorker.Close() is called when this instance wins an election
// thisInstanceLeaderHostname - a hostname that will be passed to followers they can use to connect to a service on the leader, can be empty
// thisInstanceLeaderPort - a port that will be passed to followers they can use to connect to a service on the leader, can be empty
//goland:noinspection GoUnusedExportedFunction
func NewElector(
	ctx context.Context,
	database *mongostore.DataStore,
	boundary string,
	leaderWorker LeaderWorker,
	followerWorker FollowerWorker,
	thisInstanceLeaderHostname string,
	thisInstanceLeaderPort uint64,
	options ElectorOptions,
) (e *Elector, err error) {
	if boundary == "" {
		err = errors.New("elector boundary is empty")
		return
	}

	if database == nil {
		err = errors.New("elector database is nil")
		return
	}

	e = &Elector{
		database: database,
		config: electorConfig{
			boundary:                   boundary,
			leaderWorker:               leaderWorker,
			followerWorker:             followerWorker,
			thisInstanceLeaderHostname: thisInstanceLeaderHostname,
			thisInstanceLeaderPort:     thisInstanceLeaderPort,
			options:                    options,
			thisLeaderUUID:             mongouuid.MakeUUID(),
		},
	}
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	e.ctx, e.cancel = context.WithCancel(ctx)

	// ask for current leader
	leaderQueryChanRW := make(chan struct{})
	e.leaderQueryChan = leaderQueryChanRW

	// get responses on current leader
	leaderResponseChanRW := make(chan *ElectedLeader)
	e.leaderResponseChan = leaderResponseChanRW

	newLeaderChanRW := make(chan *ElectedLeader)
	e.newLeaderChan = newLeaderChanRW

	e.leaderManagerWg.Add(1)
	go leaderManager(&e.leaderManagerWg, e.config, newLeaderChanRW, leaderQueryChanRW, leaderResponseChanRW)

	leaderExpiredChanRW := make(chan struct{})
	e.wg.Add(1)
	go expireWorker(e.ctx, &e.wg, e.config, e.database, leaderExpiredChanRW, newLeaderChanRW)
	e.wg.Add(1)
	go elect(e.ctx, &e.wg, e.config, leaderExpiredChanRW, newLeaderChanRW)

	e.started = true
	return
}

var ManagedIndexes = []mongostore.Index{
	{ // Delete collectionName documents that have expired
		CollectionName: collectionName,
		Id:             "ttlExpire_ttl",
		Version:        0,
		Model: mongo.IndexModel{
			Options: options.Index().SetExpireAfterSeconds(0),
			Keys: bsonx.Doc{
				{Key: "ttlExpire", Value: bsonx.Int32(mongostore.ASC)},
			},
		},
	},
}

func (e *Elector) Close() {
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()
	if !e.started {
		return
	}
	if e.cancel != nil {
		e.cancel()
	}
	close(e.leaderQueryChan)
	e.wg.Wait()

	// need e.wg to complete before closing this so all writers to e.newLeaderChan have exited
	// closing e.newLeaderChan will signal the leaderManager to exit
	close(e.newLeaderChan)
	e.leaderManagerWg.Wait()
	logger.Instance().Info(getLogPrefix(e.config.boundary, e.config.thisLeaderUUID, "stopped"))
}

func (e *Elector) Status() (status *ElectorStatus) {
	currentLeader := e.GetElectedLeader()

	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()
	status = &ElectorStatus{
		Id:            e.config.thisLeaderUUID,
		Boundary:      e.config.boundary,
		Hostname:      e.config.thisInstanceLeaderHostname,
		Port:          e.config.thisInstanceLeaderPort,
		IsLeader:      currentLeader != nil && e.config.thisLeaderUUID.Equal(currentLeader.LeaderUUID),
		ElectedLeader: currentLeader,
	}
	return
}

func (e *Elector) GetElectedLeader() *ElectedLeader {
	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()
	if !e.started {
		return nil
	}
	e.leaderQueryChan <- struct{}{}
	return <-e.leaderResponseChan
}

func leaderManager(
	wg *sync.WaitGroup,
	config electorConfig,
	newLeaderChan <-chan *ElectedLeader,
	leaderQueryChan <-chan struct{},
	leaderResponseChan chan<- *ElectedLeader,
) {
	defer wg.Done()
	defer close(leaderResponseChan)
	defer func() {
		if err := recover(); err != nil {
			logger.Instance().Error(
				getLogPrefix(config.boundary, config.thisLeaderUUID, "panic occurred in leaderManager"),
				logger.Any("error", err),
				logger.String("stacktrace", string(debug.Stack())),
			)
		}
	}()

	var theLeader *ElectedLeader
	for {
		select {
		case newLeader, ok := <-newLeaderChan:
			if !ok {
				// channel is closed don't read anymore from it
				newLeaderChan = nil
				break
			}
			theLeader = newLeader

		case _, ok := <-leaderQueryChan:
			if !ok {
				// channel is closed don't read anymore from it
				leaderQueryChan = nil
				break
			}
			leaderResponseChan <- theLeader
		}
		// if these channels are closed we break loop
		if newLeaderChan == nil && leaderQueryChan == nil {
			break
		}
	}
}

func doExpireWork(ctxIn context.Context, config electorConfig) (latestLeader *ElectedLeader) {
	var err error
	var collection *mongo.Collection
	collection, err = mongostore.Instance().CollectionLinearWriteRead(ctxIn, collectionName)
	if err != nil {
		logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error"), logger.Error(err))
		return
	}

	filter := bson.M{}
	filter["_id"] = config.boundary
	filter["ttlExpire"] = bson.D{{"$lt", time.Now()}}
	ctx, cancel := context.WithTimeout(ctxIn, time.Duration(config.options.LeaderHeartbeatSeconds)*time.Second)
	_, err = collection.DeleteOne(ctx, filter)
	cancel()
	if err != nil {
		logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error on DeleteOne for expireWorker"), logger.Error(err))
		return
	}

	newLeader := &ElectedLeader{}
	filter = bson.M{}
	filter["_id"] = config.boundary
	ctx, cancel = context.WithTimeout(ctxIn, time.Duration(config.options.LeaderHeartbeatSeconds)*time.Second)
	err = collection.FindOne(ctx, filter).Decode(newLeader)
	cancel()
	if err != nil {
		if err != mongo.ErrNoDocuments {
			logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error on FindOne for expireWorker"), logger.Error(err))
		}
	} else {
		latestLeader = newLeader
	}
	return
}

func notifyLeaderExpired(ctx context.Context, leaderExpired chan<- struct{}) {
	select {
	case <-time.After(time.Millisecond * time.Duration(250)):
	case leaderExpired <- struct{}{}:
	case <-ctx.Done():
		return
	}
}

func expireWorker(ctx context.Context, wg *sync.WaitGroup, config electorConfig, database *mongostore.DataStore, leaderExpiredChan chan<- struct{}, newLeaderChan chan<- *ElectedLeader) {
	defer wg.Done()
	defer close(leaderExpiredChan)
	defer func() {
		newLeaderChan <- nil
	}()
	defer func() {
		if err := recover(); err != nil {
			logger.Instance().Error(
				getLogPrefix(config.boundary, config.thisLeaderUUID, "panic occurred in expireWorker"),
				logger.Any("error", err),
				logger.String("stacktrace", string(debug.Stack())),
			)
		}
	}()

	indexEnsured := false
	for {
		select {
		case <-time.After(time.Second * time.Duration(config.options.LeaderHeartbeatSeconds)):
		case <-ctx.Done():
			return
		}

		if !indexEnsured {
			ok := database.AddAndEnsureManagedIndexes(ctx, ManagedIndexes)
			if !ok {
				logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error ensuring indexes"), logger.Stack("stacktrace"))
				continue
			} else {
				indexEnsured = true
			}
		}

		latestLeader := doExpireWork(ctx, config)
		newLeaderChan <- latestLeader
		// notify the elector if there is no leader
		if latestLeader == nil {
			notifyLeaderExpired(ctx, leaderExpiredChan)
		}
	}
}

func followerLeaderWatch(ctx context.Context, leaderExpiredChan <-chan struct{}) {
	select {
	case <-leaderExpiredChan:
		break
	case <-ctx.Done():
		break
	}
}

// elect
// Runs until the context cancelled. Stops leaderWorker and followerWorker then attempts to elect itself leader.
// If election won:
//		1) start the leaderWorker
//		2) update ElectedLeader.TTLExpire record in mongo every e.leaderHeartbeatSeconds, and if fail run election
// If election lost:
//		1) start followerWorker
//		2) watch for deletes to the collection and if the leader for our e.boundary is deleted run election
func elect(ctx context.Context, wg *sync.WaitGroup, config electorConfig, leaderExpiredChan <-chan struct{}, newLeaderChan chan<- *ElectedLeader) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil {
			logger.Instance().Error(
				getLogPrefix(config.boundary, config.thisLeaderUUID, "panic occurred in elect"),
				logger.Any("error", err),
				logger.String("stacktrace", string(debug.Stack())),
			)
		}
	}()

	logger.Instance().Info(getLogPrefix(config.boundary, config.thisLeaderUUID, "started"), logger.String("hostname", config.thisInstanceLeaderHostname), logger.Uint64("port", config.thisInstanceLeaderPort))
	for {
		// check context cancelled
		if ctx.Err() != nil {
			break
		}

		won, err := tryWinElectionLoop(ctx, config, newLeaderChan)
		if err != nil {
			break
		}

		if won {
			if config.leaderWorker != nil {
				config.leaderWorker.Start(ctx)
			}
			if config.followerWorker != nil {
				config.followerWorker.Start(ctx, config.thisLeaderUUID)
			}
			leaderHeartbeat(ctx, config)
			logger.Instance().Info(getLogPrefix(config.boundary, config.thisLeaderUUID, "leadership lost"))
		} else {
			if config.followerWorker != nil {
				config.followerWorker.Start(ctx, config.thisLeaderUUID)
			}
			followerLeaderWatch(ctx, leaderExpiredChan)
		}
		stopWorkers(config)
	}

	stopWorkers(config)
}

// leaderHeartbeat update ttlExpire to keep leadership. Return if leadership lost, context cancelled, or operations on mongo fail.
func leaderHeartbeat(ctx context.Context, config electorConfig) {
	for {
		select {
		case <-time.After(time.Second * time.Duration(config.options.LeaderHeartbeatSeconds)):
		case <-ctx.Done():
			return
		}

		// get a collection, if fail wait and try again
		collection, err := mongostore.Instance().CollectionLinearWriteRead(ctx, collectionName)
		if err != nil {
			logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error getting mongo collection"), logger.Error(err))
			return
		}

		setDoc := bson.D{
			{"ttlExpire", time.Now().Add(time.Duration(config.options.LeaderTTLSeconds) * time.Second)},
		}
		queueInsertFilter := bson.D{
			{"_id", config.boundary},
			{"leaderUUID", config.thisLeaderUUID.Raw()},
		}
		queueUpdateOnInsert := bson.D{
			{"$set", setDoc},
		}
		var updateResult *mongo.UpdateResult
		ctx, cancel := context.WithTimeout(ctx, time.Duration(config.options.LeaderHeartbeatSeconds)*time.Second)
		updateResult, err = collection.UpdateOne(ctx, queueInsertFilter, queueUpdateOnInsert)
		cancel()
		if err != nil {
			logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error on UpdateOne for leaderHeartbeat"), logger.Error(err))
			return
		}
		if updateResult == nil || updateResult.MatchedCount == 0 {
			return
		}
	}
}

// tryWinElectionLoop
// runs tryWinElection in a loop until it wins or loses without or the context is cancelled
// returns err only if context cancelled
func tryWinElectionLoop(ctx context.Context, config electorConfig, newLeaderChan chan<- *ElectedLeader) (won bool, err error) {
	for {
		// if we win or lose election without unexpected error then return
		won, err = tryWinElection(ctx, config, newLeaderChan)
		if err == nil {
			return
		}

		// had unexpected error try again after e.leaderHeartbeatSeconds or context cancel
		select {
		case <-time.After(time.Second * time.Duration(config.options.LeaderHeartbeatSeconds)):
		case <-ctx.Done():
			return false, contextCancelledError
		}
	}
}

func tryWinElection(ctxIn context.Context, config electorConfig, newLeaderChan chan<- *ElectedLeader) (won bool, err error) {
	var currentLeader *ElectedLeader
	defer func() {
		newLeaderChan <- currentLeader
	}()

	// get a collection, if fail wait and try again
	var collection *mongo.Collection
	collection, err = mongostore.Instance().CollectionLinearWriteRead(ctxIn, collectionName)
	if err != nil {
		logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error getting mongo collection"), logger.Error(err))
		err = errors.Wrap(err, "error getting mongo collection")
		return false, err
	}

	thisLeader := &ElectedLeader{
		Boundary:       config.boundary,
		TTLExpire:      time.Now().Add(time.Duration(config.options.LeaderTTLSeconds) * time.Second),
		LeaderUUID:     config.thisLeaderUUID,
		LeaderHostname: config.thisInstanceLeaderHostname,
		LeaderPort:     config.thisInstanceLeaderPort,
	}

	var insertResult *mongo.InsertOneResult
	ctx, cancel := context.WithTimeout(ctxIn, time.Duration(config.options.LeaderHeartbeatSeconds)*time.Second)
	insertResult, err = collection.InsertOne(ctx, thisLeader)
	cancel()
	if err != nil && !mongostore.IsDuplicateKeyError(err) {
		logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error on InsertOne for tryWinElection"), logger.Error(err))
		err = errors.Wrap(err, "error on InsertOne for tryWinElection")
		return false, err
	} else if err == nil && insertResult != nil && insertResult.InsertedID != nil {
		currentLeader = thisLeader
		logger.Instance().Info(getLogPrefix(config.boundary, config.thisLeaderUUID, "election won"))
		return true, nil
	} else {
		logger.Instance().Info(getLogPrefix(config.boundary, config.thisLeaderUUID, "election lost"))
		newLeader := &ElectedLeader{}
		filter := bson.M{}
		filter["_id"] = config.boundary
		ctx, cancel = context.WithTimeout(ctx, time.Duration(config.options.LeaderHeartbeatSeconds)*time.Second)
		err = collection.FindOne(ctx, filter).Decode(newLeader)
		cancel()
		if err != nil {
			// this could happen if we failed to win election and then before our find the leader's timeout expires
			if err == mongo.ErrNoDocuments {
				return false, mongo.ErrNoDocuments
			}
			logger.Instance().Error(getLogPrefix(config.boundary, config.thisLeaderUUID, "error on FindOne for tryWinElection"), logger.Error(err))
			err = errors.Wrapf(err, "error on FindOne for tryWinElection")
			return false, err
		}
		currentLeader = newLeader
		return false, nil
	}
}

func stopWorkers(config electorConfig) {
	var stopWorkersWg sync.WaitGroup
	if config.followerWorker != nil {
		stopWorkersWg.Add(1)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Instance().Error(
						getLogPrefix(config.boundary, config.thisLeaderUUID, "panic occurred during stop follower worker"),
						logger.Any("error", err),
						logger.String("stacktrace", string(debug.Stack())),
					)
				}
			}()
			config.followerWorker.Stop()
			stopWorkersWg.Done()
		}()
	}
	if config.leaderWorker != nil {
		stopWorkersWg.Add(1)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Instance().Error(
						getLogPrefix(config.boundary, config.thisLeaderUUID, "panic occurred during stop leader worker"),
						logger.Any("error", err),
						logger.String("stacktrace", string(debug.Stack())),
					)
				}
			}()
			config.leaderWorker.Stop()
			stopWorkersWg.Done()
		}()
	}
	stopWorkersWg.Wait()
}

func getLogPrefix(boundary string, thisLeaderUUID mongouuid.UUID, format string) string {
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(electorLogPrefix)
	sb.WriteString(" ")
	sb.WriteString(boundary)
	sb.WriteString(":")
	sb.WriteString(thisLeaderUUID.String())
	sb.WriteString("] ")
	sb.WriteString(format)
	return sb.String()
}
