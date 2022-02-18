///
// Copyright (c) 2021. StealthMode Inc. All Rights Reserved
///

package mongouuid

import (
	_ "encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"strings"
)

// UUID Global unique id.
//
// swagger:type string
type UUID struct {
	id primitive.ObjectID
}

func (g UUID) Raw() primitive.ObjectID {
	return g.id
}

func (g UUID) String() string {
	return g.id.Hex()
}

func NewUUID() *UUID {
	return &UUID{
		id: primitive.NewObjectID(),
	}
}

func (g *UUID) SetUUID(i *UUID) {
	g.id = i.id
}

func UUIDFromObjectId(oid primitive.ObjectID) UUID {
	return UUID{
		id: oid,
	}
}

func UUIDFromString(s string) (UUID, error) {
	// remove any quotes
	s = strings.Replace(s, "\"", "", -1)
	o, err := primitive.ObjectIDFromHex(s)
	if err != nil {
		return UUID{
			id: primitive.NewObjectID(),
		}, errors.Wrapf(err, "String (%v) is not a valid UUID: %v", s, err)
	}
	return UUID{
		id: o,
	}, nil
}

func (g UUID) MarshalJSON() ([]byte, error) {
	return []byte("\"" + g.id.Hex() + "\""), nil
}

func (g *UUID) UnmarshalJSON(data []byte) error {
	var err error
	var u UUID
	u, err = UUIDFromString(string(data))
	*g = u
	return err
}

func (g *UUID) UnmarshalBSONValue(t bsontype.Type, raw []byte) error {
	if t == bsontype.Null {
		return nil
	}
	if t != bsontype.ObjectID {
		return fmt.Errorf("unable to unmarshal UUID from bson type: %v", t)
	}

	var ok bool
	if g.id, _, ok = bsoncore.ReadObjectID(raw); !ok {
		return errors.New("unable to read bson ObjectId to unmarshal UUID")
	}

	return nil
}

func (g UUID) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bson.MarshalValue(g.Raw())
}

func (g UUID) Equal(x UUID) bool {
	if g.id.Hex() != x.id.Hex() {
		return false
	}
	return true
}
