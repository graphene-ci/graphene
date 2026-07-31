package protohelp

import (
	"google.golang.org/protobuf/proto"
)

func ProtoNew[T proto.Message]() (model T) {
	return model.ProtoReflect().Type().New().Interface().(T)
}

func ProtoFullName[T proto.Message]() string {
	var m T
	return string(m.ProtoReflect().Descriptor().FullName())
}

func ProtoName[T proto.Message]() string {
	var m T
	return string(m.ProtoReflect().Descriptor().Name())
}
