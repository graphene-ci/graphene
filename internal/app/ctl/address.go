package ctl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// ErrPathTooLong — the address names more segments than the kind has.
var ErrPathTooLong = errors.New("ctl: path has more segments than the kind defines")

// Address is a resource address as an operator types it: a kind and a
// slash-separated path. A path with every segment of the kind names ONE
// resource; a shorter path is a prefix and names a subtree — the same
// distinction the store makes, so no --exact flag is needed.
type Address struct {
	Kind string
	Path []string
}

// ParseAddress splits "Kernel acme/k1" style input.
func ParseAddress(kind, path string) Address {
	return Address{Kind: kind, Path: splitPath(path)}
}

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}

	return strings.Split(trimmed, "/")
}

// String renders the address the way it is typed.
func (a Address) String() string {
	if len(a.Path) == 0 {
		return a.Kind
	}

	return a.Kind + " " + strings.Join(a.Path, "/")
}

// Resolve looks an address up against its kind's definition: the caller
// gets both answers a read needs — the definition (column names, arity)
// and whether the path names one resource or a subtree — from a single
// round trip.
func (c *Client) Resolve(ctx context.Context, addr Address) (*graphenepbv1.ResourceDefinition, bool, error) {
	def, err := c.Definition(ctx, addr.Kind)
	if err != nil {
		return nil, false, err
	}

	arity := len(def.GetPathSegments())
	if len(addr.Path) > arity {
		return nil, false, fmt.Errorf("%w: %s wants %d", ErrPathTooLong, addr.Kind, arity)
	}

	return def, len(addr.Path) == arity, nil
}

// Exact reports whether the path names a single resource.
func (c *Client) Exact(ctx context.Context, addr Address) (bool, error) {
	_, exact, err := c.Resolve(ctx, addr)

	return exact, err
}

// Definition returns one kind's definition.
func (c *Client) Definition(ctx context.Context, kind string) (*graphenepbv1.ResourceDefinition, error) {
	defs, err := c.Definitions(ctx)
	if err != nil {
		return nil, err
	}

	for _, def := range defs {
		if def.GetKind() == kind {
			return def, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrUnknownKind, kind)
}

// KindArity reports how many path segments a kind takes.
func (c *Client) KindArity(ctx context.Context, kind string) (int, error) {
	defs, err := c.Definitions(ctx)
	if err != nil {
		return 0, err
	}

	for _, def := range defs {
		if def.GetKind() == kind {
			return len(def.GetPathSegments()), nil
		}
	}

	return 0, fmt.Errorf("%w: %s", ErrUnknownKind, kind)
}

// ErrUnknownKind — the kernel defines no such kind.
var ErrUnknownKind = errors.New("ctl: unknown kind")
