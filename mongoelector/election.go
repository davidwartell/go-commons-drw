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
	Start(ctx context.Context, electedLeader *ElectedLeader, thisLeaderUUID *mongouuid.UUID)

	// Stop the worker. May be called multiple times in a row.
	Stop()
}

type Elector struct {
	ctx                        context.Context
	cancel                     context.CancelFunc
	wg                         sync.WaitGroup
	database                   *mongostore.DataStore
	boundary                   string
	leaderWorker               LeaderWorker
	followerWorker             FollowerWorker
	thisInstanceLeaderHostname string
	thisInstanceLeaderPort     uint64
	options                    ElectorOptions
	thisLeaderUUID             *mongouuid.UUID
	leaderExpired              chan struct{}

	electedLeaderLock sync.Mutex // protects electedLeader
	electedLeader     *ElectedLeader
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
// leaderWorker - leaderWorker.Start() is called when this instance wins an election.  leaderWorker.Stop() is called when this instance loses an election
// followerWorker - followerWorker.Start() is called when this instance loses an election.  followerWorker.Stop() is called when this instance wins an election
// thisInstanceLeaderHostname - a hostname that will be passed to followers they can use to connect to a service on the leader, can be empty
// thisInstanceLeaderPort - a port that will be passed to followers they can use to connect to a service on the leader, can be empty
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
		database:                   database,
		boundary:                   boundary,
		leaderWorker:               leaderWorker,
		followerWorker:             followerWorker,
		thisInstanceLeaderHostname: thisInstanceLeaderHostname,
		thisInstanceLeaderPort:     thisInstanceLeaderPort,
		options:                    options,
		thisLeaderUUID:             mongouuid.NewUUID(),
	}
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.leaderExpired = make(chan struct{})

	e.wg.Add(1)
	go e.expireWorker()
	e.wg.Add(1)
	go e.elect()

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

func (e *Elector) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	logger.Instance().InfofUnstruct(e.getLogPrefix("stopped"))
}

func (e *Elector) Status() (status *ElectorStatus) {
	status = &ElectorStatus{
		Id:            *e.thisLeaderUUID,
		Boundary:      e.boundary,
		Hostname:      e.thisInstanceLeaderHostname,
		Port:          e.thisInstanceLeaderPort,
		IsLeader:      e.getElectedLeader() != nil && e.thisLeaderUUID.Equal(e.getElectedLeader().LeaderUUID),
		ElectedLeader: e.getElectedLeader(),
	}
	return
}

func (e *Elector) expireWorker() {
	defer e.wg.Done()
	defer close(e.leaderExpired)
	indexEnsured := false

expireWorkerLoop:
	for {
		select {
		case <-time.After(time.Second * time.Duration(e.options.LeaderHeartbeatSeconds)):
		case <-e.ctx.Done():
			break expireWorkerLoop
		}

		if !indexEnsured {
			ok := e.database.AddAndEnsureManagedIndexes(e.ctx, ManagedIndexes)
			if !ok {
				err := errors.New("error ensuring indexes")
				logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err)
				continue
			} else {
				indexEnsured = true
			}
		}

		var err error
		var collection *mongo.Collection
		collection, err = mongostore.Instance().CollectionLinearWriteRead(e.ctx, collectionName)
		if err != nil {
			logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err)
			continue
		}

		filter := bson.M{}
		filter["_id"] = e.boundary
		filter["ttlExpire"] = bson.D{{"$lt", time.Now()}}
		ctx, cancel := context.WithTimeout(e.ctx, time.Duration(e.options.LeaderHeartbeatSeconds)*time.Second)
		_, err = collection.DeleteOne(ctx, filter)
		cancel()
		if err != nil {
			err2 := errors.Wrapf(err, "error on DeleteOne for expireWorker: %v", err)
			logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err2)
			continue
		}

		newLeader := &ElectedLeader{}
		filter = bson.M{}
		filter["_id"] = e.boundary
		ctx, cancel = context.WithTimeout(e.ctx, time.Duration(e.options.LeaderHeartbeatSeconds)*time.Second)
		err = collection.FindOne(ctx, filter).Decode(newLeader)
		cancel()
		if err != nil {
			if err != mongo.ErrNoDocuments {
				err2 := errors.Wrapf(err, "error on FindOne for expireWorker: %v", err)
				logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err2)
			}
			select {
			case <-time.After(time.Millisecond * time.Duration(250)):
			case e.leaderExpired <- struct{}{}:
			case <-e.ctx.Done():
				break
			}
		} else {
			e.setElectedLeader(newLeader)
		}
	}
}

func (e *Elector) followerLeaderWatch() {
	select {
	case <-e.leaderExpired:
		break
	case <-e.ctx.Done():
		break
	}
	return
}

// elect
// Runs until the context cancelled. Stops leaderWorker and followerWorker then attempts to elect itself leader.
// If election won:
//		1) start the leaderWorker
//		2) update ElectedLeader.TTLExpire record in mongo every e.leaderHeartbeatSeconds, and if fail run election
// If election lost:
//		1) start followerWorker
//		2) watch for deletes to the collection and if the leader for our e.boundary is deleted run election
func (e *Elector) elect() {
	defer e.wg.Done()
	logger.Instance().InfofUnstruct(e.getLogPrefix("started with hostname %s:%s"), e.thisInstanceLeaderHostname, strconv.FormatUint(e.thisInstanceLeaderPort, 10))

	for {
		// check context cancelled
		if e.ctx.Err() != nil {
			break
		}

		won, err := e.tryWinElectionLoop()
		if err != nil {
			break
		}

		if won {
			if e.leaderWorker != nil {
				e.leaderWorker.Start(e.ctx)
			}
			if e.followerWorker != nil {
				e.followerWorker.Start(e.ctx, e.electedLeader, e.thisLeaderUUID)
			}
			e.leaderHeartbeat()
			logger.Instance().InfofUnstruct(e.getLogPrefix("leadership lost"))
		} else {
			if e.followerWorker != nil {
				e.followerWorker.Start(e.ctx, e.electedLeader, e.thisLeaderUUID)
			}
			e.followerLeaderWatch()
		}
		e.stopWorkers()
	}

	e.stopWorkers()
	return
}

// leaderHeartbeat update ttlExpire to keep leadership. Return if leadership lost, context cancelled, or operations on mongo fail.
func (e *Elector) leaderHeartbeat() {
	for {
		select {
		case <-time.After(time.Second * time.Duration(e.options.LeaderHeartbeatSeconds)):
		case <-e.ctx.Done():
			return
		}

		// get a collection, if fail wait and try again
		collection, err := mongostore.Instance().CollectionLinearWriteRead(e.ctx, collectionName)
		if err != nil {
			logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err)
			return
		}

		setDoc := bson.D{
			{"ttlExpire", time.Now().Add(time.Duration(e.options.LeaderTTLSeconds) * time.Second)},
		}
		queueInsertFilter := bson.D{
			{"_id", e.boundary},
			{"leaderUUID", e.thisLeaderUUID.Raw()},
		}
		queueUpdateOnInsert := bson.D{
			{"$set", setDoc},
		}
		var updateResult *mongo.UpdateResult
		ctx, cancel := context.WithTimeout(e.ctx, time.Duration(e.options.LeaderHeartbeatSeconds)*time.Second)
		updateResult, err = collection.UpdateOne(ctx, queueInsertFilter, queueUpdateOnInsert)
		cancel()
		if err != nil {
			err2 := errors.Wrapf(err, "error on UpdateOne for leaderHeartbeat: %v", err)
			logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err2)
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
func (e *Elector) tryWinElectionLoop() (won bool, err error) {
	for {
		// if we win or lose election without unexpected error then return
		won, err = e.tryWinElection()
		if err == nil {
			return
		}

		// had unexpected error try again after e.leaderHeartbeatSeconds or context cancel
		select {
		case <-time.After(time.Second * time.Duration(e.options.LeaderHeartbeatSeconds)):
		case <-e.ctx.Done():
			return false, contextCancelledError
		}
	}
}

func (e *Elector) setElectedLeader(newLeader *ElectedLeader) {
	e.electedLeaderLock.Lock()
	defer e.electedLeaderLock.Unlock()
	e.electedLeader = newLeader
}

func (e *Elector) getElectedLeader() *ElectedLeader {
	e.electedLeaderLock.Lock()
	defer e.electedLeaderLock.Unlock()
	return e.electedLeader
}

func (e *Elector) tryWinElection() (won bool, err error) {
	// get a collection, if fail wait and try again
	var collection *mongo.Collection
	collection, err = mongostore.Instance().CollectionLinearWriteRead(e.ctx, collectionName)
	if err != nil {
		logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err)
		return false, err
	}

	thisLeader := &ElectedLeader{
		Boundary:       e.boundary,
		TTLExpire:      time.Now().Add(time.Duration(e.options.LeaderTTLSeconds) * time.Second),
		LeaderUUID:     *e.thisLeaderUUID,
		LeaderHostname: e.thisInstanceLeaderHostname,
		LeaderPort:     e.thisInstanceLeaderPort,
	}

	var insertResult *mongo.InsertOneResult
	ctx, cancel := context.WithTimeout(e.ctx, time.Duration(e.options.LeaderHeartbeatSeconds)*time.Second)
	insertResult, err = collection.InsertOne(ctx, thisLeader)
	cancel()
	if err != nil && !mongostore.IsDuplicateKeyError(err) {
		err2 := errors.Wrapf(err, "error on InsertOne for tryWinElection: %v", err)
		logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err2)
		return false, err2
	} else if err == nil && insertResult != nil && insertResult.InsertedID != nil {
		e.setElectedLeader(thisLeader)
		logger.Instance().InfofUnstruct(e.getLogPrefix("election won"))
		return true, nil
	} else {
		logger.Instance().InfofUnstruct(e.getLogPrefix("election lost"))
		newLeader := &ElectedLeader{}
		filter := bson.M{}
		filter["_id"] = e.boundary
		ctx, cancel = context.WithTimeout(e.ctx, time.Duration(e.options.LeaderHeartbeatSeconds)*time.Second)
		err = collection.FindOne(ctx, filter).Decode(newLeader)
		cancel()
		if err != nil {
			// this could happen if we failed to win election and then before our find the leader's timeout expires
			if err == mongo.ErrNoDocuments {
				return false, mongo.ErrNoDocuments
			}
			err2 := errors.Wrapf(err, "error on FindOne for tryWinElection: %v", err)
			logger.Instance().ErrorfUnstruct(e.getLogPrefix("error: %v"), err2)
			return false, err2
		}
		e.setElectedLeader(newLeader)
		return false, nil
	}
}

func (e *Elector) stopWorkers() {
	var stopWorkersWg sync.WaitGroup
	if e.followerWorker != nil {
		stopWorkersWg.Add(1)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Instance().ErrorfUnstruct("panic occurred during stopWorkers: %v stacktrace from panic: %s", err, string(debug.Stack()))
				}
			}()
			e.followerWorker.Stop()
			stopWorkersWg.Done()
		}()
	}
	if e.leaderWorker != nil {
		stopWorkersWg.Add(1)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Instance().ErrorfUnstruct("panic occurred during stopWorkers: %v stacktrace from panic: %s", err, string(debug.Stack()))
				}
			}()
			e.leaderWorker.Stop()
			stopWorkersWg.Done()
		}()
	}
	stopWorkersWg.Wait()
}

func (e *Elector) getLogPrefix(format string) string {
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(electorLogPrefix)
	sb.WriteString(" ")
	sb.WriteString(e.boundary)
	sb.WriteString(":")
	sb.WriteString(e.thisLeaderUUID.String())
	sb.WriteString("] ")
	sb.WriteString(format)
	return sb.String()
}
