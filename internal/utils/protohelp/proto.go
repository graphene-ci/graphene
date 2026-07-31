// Package protohelp holds small generic helpers over protobuf reflection.
package protohelp

import (
	"google.golang.org/protobuf/proto"
)

// ProtoNew constructs a fresh message of the given type.
func ProtoNew[T proto.Message]() T {
	var model T

	message, ok := model.ProtoReflect().Type().New().Interface().(T)
	if !ok {
		panic("protohelp: protoreflect returned an unexpected message type")
	}

	return message
}

// ProtoFullName reports the fully qualified proto name of the type.
func ProtoFullName[T proto.Message]() string {
	var model T

	return string(model.ProtoReflect().Descriptor().FullName())
}

// ProtoName reports the short proto name of the type.
func ProtoName[T proto.Message]() string {
	var model T

	return string(model.ProtoReflect().Descriptor().Name())
}
