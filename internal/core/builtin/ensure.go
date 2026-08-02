package builtin

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/registry"
)

// Ensure makes the compiled-in definitions present in the store; run at
// every control-kernel start, idempotent:
//   - absent kind        → Define (v1);
//   - schema drifted     → Define a NEW version (a binary upgrade shipped
//     a changed schema; existing instances stay pinned to their versions);
//   - identical          → skip.
func Ensure(ctx context.Context, reg *registry.Registry) error {
	for _, want := range Definitions() {
		if err := ensureOne(ctx, reg, want); err != nil {
			return err
		}
	}

	return nil
}

func ensureOne(ctx context.Context, reg *registry.Registry, want *graphenepbv1.ResourceDefinition) error {
	have, err := reg.Get(ctx, want.GetKind(), 0)

	switch {
	case errors.Is(err, registry.ErrUnknownKind):
		// fall through to Define
	case err != nil:
		return fmt.Errorf("builtin: read %s: %w", want.GetKind(), err)
	case sameShape(have, want):
		return nil
	}

	if _, err := reg.Define(ctx, want); err != nil {
		return fmt.Errorf("builtin: define %s: %w", want.GetKind(), err)
	}

	return nil
}

// sameShape compares everything but the store-assigned version.
func sameShape(have, want *graphenepbv1.ResourceDefinition) bool {
	a, ok := proto.Clone(have).(*graphenepbv1.ResourceDefinition)
	if !ok {
		return false
	}

	b, ok := proto.Clone(want).(*graphenepbv1.ResourceDefinition)
	if !ok {
		return false
	}

	a.Version = 0
	b.Version = 0

	return proto.Equal(a, b)
}
