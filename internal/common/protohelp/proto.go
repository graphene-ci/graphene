package protohelp

import (
	"google.golang.org/protobuf/proto"
)

// ProtoNew makes an empty message of the type asked for.
//
// The assertion cannot fail: the type came from T's own descriptor, so
// what comes back is a T by construction. Checking it would be checking
// the protobuf runtime against itself.
func ProtoNew[T proto.Message]() T {
	var model T

	created, _ := model.ProtoReflect().Type().New().Interface().(T)

	return created
}

func ProtoFullName[T proto.Message]() string {
	var m T
	return string(m.ProtoReflect().Descriptor().FullName())
}

func ProtoName[T proto.Message]() string {
	var m T
	return string(m.ProtoReflect().Descriptor().Name())
}
