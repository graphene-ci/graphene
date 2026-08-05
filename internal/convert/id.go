// Package convert carries values between the domain types and the
// protobuf messages they are stored and sent as.
//
// It sits apart from both on purpose. The domain packages do not know
// what a message looks like — a type that carried IntoPb would be a
// domain package importing a wire format, and the second format to
// arrive would either join it there or break the rule on its first day.
// The message package does not know what a domain type is, because it is
// generated.
//
// Which leaves the conversion needing a home of its own, and this is it.
// It is used by two callers with nothing else in common: the store, which
// frames these messages into bytes, and the API, which puts them on a
// wire.
//
// Everything here goes INTO the domain through the ordinary doors —
// resource.Restore, def.New, kind.New — so a message that would make an
// impossible value produces an error instead of one.
package convert

import (
	"fmt"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// IdToPb writes an id down.
//
// The position names go with the values. They are redundant against the
// definition and stored anyway: a record has to stay readable when its
// definition does not, and it has to decode with the shape it was written
// under rather than whatever the current version of the kind says.
func IdToPb(id resource.Id) *graphenepbv1.Id {
	if id.IsZero() {
		return nil
	}

	message := &graphenepbv1.Id{Kind: id.Kind().String()}

	for name, value := range id.Path().All() {
		message.Path = append(message.Path, &graphenepbv1.Segment{
			Name:  name.String(),
			Value: value.String(),
		})
	}

	return message
}

// kindFromPb reads a kind name, saying which name it was that failed.
// Kinds arrive from four places in these files and "invalid kind" alone
// is not enough to find which.
func kindFromPb(raw string) (kind.Kind, error) {
	named, err := kind.New(raw)
	if err != nil {
		return "", fmt.Errorf("kind %q: %w", raw, err)
	}

	return named, nil
}

// IdFromPb reads one back, rebuilding the shape from the names it
// carries and then filling it.
//
// A nil message is the zero id and not an error: an id is optional in
// exactly one place — an owner — and "belongs to nobody" is a thing to
// say rather than a thing to refuse.
func IdFromPb(message *graphenepbv1.Id) (resource.Id, error) {
	if message == nil {
		return resource.Id{}, nil
	}

	named, err := kindFromPb(message.GetKind())
	if err != nil {
		return resource.Id{}, err
	}

	names := make([]string, 0, len(message.GetPath()))
	values := make([]string, 0, len(message.GetPath()))

	for _, segment := range message.GetPath() {
		names = append(names, segment.GetName())
		values = append(values, segment.GetValue())
	}

	shape, err := path.NewTPath(names...)
	if err != nil {
		return resource.Id{}, fmt.Errorf("%s: path shape: %w", named, err)
	}

	filled, err := shape.New(values...)
	if err != nil {
		return resource.Id{}, fmt.Errorf("%s: path: %w", named, err)
	}

	return resource.NewId(named, filled), nil
}
