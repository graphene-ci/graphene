package commands

import (
	"context"
	"errors"
	"fmt"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/client"
)

// addressed turns "Process" and "local/one" into the id a call takes.
//
// THE NAMES COME FROM THE KIND. A path's shape is part of its identity —
// "local/one" under (kernel, name) and under (tenant, name) are two
// different resources — and only the definition knows which. So the
// definition is read, once, and the values a person typed are filled into
// it in order.
//
// Fewer values than positions is a PREFIX, which is what makes `gctl get
// Process local` the subtree and `gctl get Process` the whole kind. More
// is refused here rather than by the kernel, because here is where the
// shape that was violated can be shown.
// errPathTooLong — more values than the kind's shape has positions. A
// path can be SHORTER than its shape, which names a subtree; longer is
// not a subtree of anything.
var errPathTooLong = errors.New("path has more values than the kind's shape")

func addressed(
	ctx context.Context, on *client.Kernel, named, written string,
) (*graphenepbv1.Id, error) {
	shape, err := shapeOf(ctx, on, named)
	if err != nil {
		return nil, err
	}

	values := split(written)
	if len(values) > len(shape) {
		return nil, fmt.Errorf("%w: %s is /%s, and %q has %d values",
			errPathTooLong, named, strings.Join(shape, "/"), written, len(values))
	}

	at := &graphenepbv1.Id{Kind: named}

	for position, value := range values {
		at.Path = append(at.Path, &graphenepbv1.Segment{
			Name:  shape[position],
			Value: value,
		})
	}

	return at, nil
}

// shapeOf asks the kernel what a kind's paths look like.
func shapeOf(ctx context.Context, on *client.Kernel, named string) ([]string, error) {
	answer, err := on.Calls().GetDefinition(ctx, &graphenepbv1.GetDefinitionRequest{
		Kind: named,
	})
	if err != nil {
		return nil, err
	}

	return answer.GetDefinition().GetShape(), nil
}

// split takes a written path apart. The empty string is no values at all,
// which names the whole kind.
func split(written string) []string {
	trimmed := strings.Trim(strings.TrimSpace(written), "/")
	if trimmed == "" {
		return nil
	}

	return strings.Split(trimmed, "/")
}

// pathOf reads an id back out as a person wrote it.
func pathOf(at *graphenepbv1.Id) string {
	values := make([]string, 0, len(at.GetPath()))

	for _, segment := range at.GetPath() {
		values = append(values, segment.GetValue())
	}

	return strings.Join(values, "/")
}
