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
	"github.com/davidwartell/go-logger-facade/logger"
	"reflect"
	"unicode/utf8"
)

const (
	MaxSliceSizePerMongoDocument = uint64(10 * 1024 * 1024)
)

var stringSliceType = reflect.TypeOf([]string{})

// TruncateStringSliceForMongoDoc ensures a string slice will fit in the mongodb doc size limit and truncates the slice
// if necessary logging a warning.
//
//goland:noinspection GoUnusedExportedFunction
func TruncateStringSliceForMongoDoc(slice []string) (newSlice []string) {
	var sizeOfSlice uint64
	for index, str := range slice {
		sizeOfSlice = sizeOfSlice + uint64(utf8.RuneCountInString(str))
		if sizeOfSlice > MaxSliceSizePerMongoDocument {
			logger.Instance().Warn(
				"truncating slice to fit in mongo document",
				logger.String("type", stringSliceType.String()),
				logger.Int("initialSliceLength", len(slice)),
				logger.Int("truncatedSliceLength", index),
				logger.Uint64("maxLengthBytes", MaxSliceSizePerMongoDocument),
			)
			newSlice = slice[:index]
			return
		}
	}
	newSlice = slice
	return
}
