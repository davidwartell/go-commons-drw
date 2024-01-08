/*
 * Copyright (c) 2024 by David Wartell. All Rights Reserved.
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

package mongoelector

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"strings"
)

type CandidateId primitive.ObjectID

//goland:noinspection GoUnusedGlobalVariable
var ZeroCandidateId = CandidateId(primitive.NilObjectID)

func (i CandidateId) String() string {
	return (primitive.ObjectID(i)).Hex()
}

//goland:noinspection GoUnusedExportedFunction
func MakeCandidateId() CandidateId {
	return CandidateId(primitive.NewObjectID())
}

//goland:noinspection GoUnusedExportedFunction
func CandidateIdPointerSliceToSlice(inputSlice []*CandidateId) (outputSlice []CandidateId) {
	outputSlice = make([]CandidateId, len(inputSlice))
	for i, ptr := range inputSlice {
		outputSlice[i] = *ptr
	}
	return
}

//goland:noinspection GoUnusedExportedFunction
func OrganizationidSliceToPointerSlice(inputSlice []CandidateId) (outputSlice []*CandidateId) {
	outputSlice = make([]*CandidateId, len(inputSlice))
	for i, ptr := range inputSlice {
		uuid := ptr
		outputSlice[i] = &uuid
	}
	return
}

func (i CandidateId) Clone() (clone CandidateId) {
	copy(clone[:], i[:])
	return
}

func (i CandidateId) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bson.MarshalValue(primitive.ObjectID(i))
}

func (i *CandidateId) UnmarshalBSONValue(t bsontype.Type, raw []byte) error {
	if t == bsontype.Null {
		return nil
	}
	if t != bsontype.ObjectID {
		return fmt.Errorf("unable to unmarshal CandidateId from bson type: %v", t)
	}

	var ok bool
	var objId primitive.ObjectID
	if objId, _, ok = bsoncore.ReadObjectID(raw); !ok {
		return errors.New("unable to read bson ObjectId to unmarshal CandidateId")
	}
	*i = CandidateId(objId)
	return nil
}

func (i CandidateId) MarshalJSON() ([]byte, error) {
	return []byte("\"" + (primitive.ObjectID(i)).Hex() + "\""), nil
}

func (i *CandidateId) UnmarshalJSON(data []byte) (err error) {
	var u CandidateId
	err = u.UnmarshalText(data)
	if err != nil {
		return
	}
	*i = u
	return err
}

func (i *CandidateId) MarshalText() ([]byte, error) {
	if i == nil {
		return nil, nil
	}
	return []byte((primitive.ObjectID(*i)).Hex()), nil
}

func (i *CandidateId) UnmarshalText(text []byte) error {
	// remove any quotes
	s := strings.Replace(string(text), "\"", "", -1)
	o, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return errors.Wrapf(err, "text (%v) is not a valid EndpointId: %v", s, err)
	}
	*i = CandidateId(o)
	return nil
}

func (i CandidateId) Equal(x CandidateId) bool {
	return bytes.Equal(i[:], x[:])
}

// Compare returns an integer comparing two CandidateIds. The result will be 0 if this == x, -1 if this < x, and +1
// if this > x.
func (i CandidateId) Compare(x CandidateId) int {
	return bytes.Compare(i[:], x[:])
}

// CandidateIdCompare returns an integer comparing two CandidateIds. The result will be 0 if a == b, -1 if a < b,
// and +1 if a > b.
func CandidateIdCompare(a CandidateId, b CandidateId) int {
	return bytes.Compare(a[:], b[:])
}
