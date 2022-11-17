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
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
)

type DirtyWriteProtectedFunc func() error

var DirtyWriteError = errors.New("dirty write error")

// CheckForDirtyWriteOnUpsert is expected to be used like this:
// Add a field to your struct called "DirtyWriteGuard"
//
//	type Person struct {
//	  ...
//	  DirtyWriteGuard uint64  `bson:"dirtyWriteGuard"`
//	}
//
// Then when you update mongo:
//
//		filter := bson.D{
//			{"_id", person.Id},	// Note: must use _id or some indexed field with a unique constraint.
//	     	// where device.DirtyWriteGuard is 0 on new or == to the dirtyWriteGuard field of the entity we expect in the collection
//			{"dirtyWriteGuard", person.DirtyWriteGuard}, // this value should be unmodified by your code as it was loaded from mongo.
//		}
//
//		// increment the counter before update or insert
//		person.DirtyWriteGuard++
//		defer func() {
//			if err != nil {
//				// if upsert fails decrement the counter
//				person.DirtyWriteGuard--
//			}
//		}()
//
//		updateOptions := &options.ReplaceOptions{}
//		updateOptions.SetUpsert(true)
//
//		var updateResult *mongo.UpdateResult
//		updateResult, err = collection.ReplaceOne(ctx, filter, person, updateOptions)
//		err = mongostore.CheckForDirtyWriteOnUpsert(updateResult, err)
//		if err != nil {
//			if err != mongostore.DirtyWriteError {
//				// only log or mess with err returned if not a DirtyWriteError
//				logger.Instance().ErrorIgnoreCancel(ctx, "error on ReplaceOne for Person", logger.Error(err))
//				err = errors.Wrap(err, "error on ReplaceOne for Person")
//			}
//			return
//		}
//
// In the expected dirty write case mongo will return updateResult.MatchedCount == 0 && updateResult.UpsertedID == nil.
// Meaning that 0 documents matched the filter with the unique id and the dirtyWriteGuard equality.
//
// In case of no dirty write and no error returned by the UpdateOne() we expect either an insert
// (updateResult.UpsertedID has a value) or an updated existing document (updateResult.MatchedCount == 1).
//
// In the the real-world and tested case mongo will return E11000 duplicate key error collection in case of dirty write.
// This is because no document will exist that matches _id and dirtyWriteGuard causing mongo to attempt to insert a new
// document which will return duplicate key error.
//
// return E11000 duplicate key error collection in case of dirty write. This is because no document will exist that
// matches _id and dirtyWriteGuard causing mongo to attempt to insert a new document which will return duplicate key
// error. In case of no dirty write and no error returned by the UpdateOne() we expect either an insert
// (updateResult.UpsertedID has a value) or an updated existing document (updateResult.MatchedCount == 1).
//
//goland:noinspection GoUnusedExportedFunction
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
//	 Example:
//	 // This code will be run repeatedly until there is no DirtyWriteError or the max retries is exceeded.
//		err = mongostore.RetryDirtyWrite(func() error {
//			var retryErr error
//
//			// query an entity from the collection that has a dirtyWriteGuard uint64 field
//			var existingPerson *Person
//			existingPerson, retryErr = YourFunctionThatDoesMongoFind(ctx, personId)
//
//			// ...logic that makes changes existingPerson which could be now stale
//
//			// YourFunctionThatDoesMongoUpsert can return DirtyWriteError
//			if retryErr = YourFunctionThatDoesMongoUpsert(ctx, existingPerson); retryErr != nil {
//				if retryErr != mongostore.DirtyWriteError {
//					logger.Instance().ErrorIgnoreCancel(ctx, "error in YourFunctionThatDoesMongoUpsert", logger.Error(retryErr))
//				}
//				return retryErr
//			}
//			return nil
//		})
//
//goland:noinspection GoUnusedExportedFunction
func RetryDirtyWrite(dirtyWriteFunc DirtyWriteProtectedFunc) (err error) {
	var retries int
	maxRetries := 100
	for {
		err = dirtyWriteFunc()
		if !errors.Is(err, DirtyWriteError) {
			// if error is not a DirtyWriteError give up retry
			break
		}
		retries++
		if retries >= maxRetries {
			err = errors.Errorf("giving up retry after %d dirty writes", retries)
			break
		}
	}
	return
}

// IsDuplicateKeyError can help handle expected behavior for any mongo command that uses Upsert. If IsDuplicateKeyError
// returns true for the error returned by a mongo operation, and you are using Upsert then you are expected to retry.
//
// Will return false if there are multiple nested mongo writeExceptions and one of the errors has a Code != 11000
// (duplicate key) indicating there are other errors that should be handled and not ignored or handled the same.
//
// BE WARNED: Mongo can at any time return E11000 duplicate key error for ANY command with Upsert enabled. Mongo expects
// the application to handle this error. This happens if: "During an update with upsert:true option, two (or more)
// threads may attempt an upsert operation using the same query predicate and, upon not finding a match, the threads
// will attempt to insert a new document. Both inserts will (and should) succeed, unless the second causes a unique
// constraint violation." See: https://jira.mongodb.org/browse/SERVER-14322
//
// Note: its documented that mongo retries  under the usual cases. While that may be true im seeing it still returns an
// error.
//
//goland:noinspection GoUnusedExportedFunction
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
