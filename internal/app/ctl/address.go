package ctl

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// Exact reports whether the path names a single resource: the kind's
// definition says how many segments that takes.
func (c *Client) Exact(ctx context.Context, addr Address) (bool, error) {
	arity, err := c.KindArity(ctx, addr.Kind)
	if err != nil {
		return false, err
	}

	switch {
	case len(addr.Path) > arity:
		return false, fmt.Errorf("%w: %s wants %d", ErrPathTooLong, addr.Kind, arity)
	case len(addr.Path) == arity:
		return true, nil
	default:
		return false, nil
	}
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
