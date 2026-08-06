package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/common/str"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// Principal is who a caller is: the name of their Identity.
//
// It is a path segment and takes a path segment's rules, because that is
// literally what it becomes — Identity/<name>. One set of rules rather
// than two that agree until they do not.
type Principal str.String

// NewPrincipal normalizes and checks a caller's name.
func NewPrincipal(raw string) (Principal, error) {
	at, err := IdentityShape.New(raw)
	if err != nil {
		return "", err
	}

	return Principal(at.Values()[0]), nil
}

func (p Principal) String() string { return string(p) }

// IsZero reports the unnamed caller — nobody, which is granted nothing.
func (p Principal) IsZero() bool { return p == "" }

// Guard is the kernel with a question asked first.
//
// It holds the unguarded kernel and hands out Sessions, which are the
// same kernel bound to one caller. Anything holding the Guard's kernel
// directly is trusted by construction: the collector recursing through a
// cascade, the bootstrap publishing builtin kinds, this package reading
// the identities it authorises against.
//
// That last one is why the wrapper shape matters rather than being a
// preference. Authorising a request means reading an Identity, and
// reading is a request; a guard that asked ITSELF would never answer.
// Reading through the kernel below closes the regress by construction.
type Guard struct {
	kernel kernel.Kernel
}

// New puts a guard in front of a kernel.
func New(k kernel.Kernel) Guard { return Guard{kernel: k} }

// As binds the guard to one caller.
//
// The caller has already been established by whatever answered "who is
// this" — a certificate, a token. Nothing here checks that; by the time
// this runs the answer is in hand and the only question left is what it
// is allowed to do.
func (g Guard) As(who Principal) Session { return Session{guard: g, who: who} }

// Session is the kernel as one caller sees it.
type Session struct {
	guard Guard
	who   Principal
}

// Who is the caller this session speaks for.
func (s Session) Who() Principal { return s.who }

// allow refuses a verb on a resource the caller may not touch.
//
// Grants are resolved on every call rather than once per session, which
// costs reads and buys the property that a permission taken away is taken
// away now. The reads are of two keys through a cache, which is what
// makes that affordable.
func (s Session) allow(ctx context.Context, verb Verb, id resource.Id) error {
	held, err := s.grants(ctx)
	if err != nil {
		return err
	}

	for _, grant := range held {
		if grant.Allows(verb, id) {
			return nil
		}
	}

	return fmt.Errorf("%w: %s may not %s %s", ErrForbidden, s.who, verb, id)
}

// allowKind refuses a kind-level verb — one with no instance to confine.
func (s Session) allowKind(ctx context.Context, verb Verb, named kind.Kind) error {
	held, err := s.grants(ctx)
	if err != nil {
		return err
	}

	for _, grant := range held {
		if grant.AllowsKind(verb, named) {
			return nil
		}
	}

	return fmt.Errorf("%w: %s may not %s %s", ErrForbidden, s.who, verb, named)
}

// grants is everything this caller may do, gathered from the roles their
// identity names.
//
// An identity that is not there is not an error worth telling apart from
// having no permissions: saying "no such identity" to an unauthenticated
// caller answers a question they should not be able to ask.
func (s Session) grants(ctx context.Context) ([]Grant, error) {
	if s.who.IsZero() {
		return nil, ErrNoPrincipal
	}

	id, err := IdentityId(s.who)
	if err != nil {
		return nil, err
	}

	stored, err := s.guard.kernel.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}

		return nil, err
	}

	var held []Grant

	for _, named := range roleNames(stored.Value) {
		granted, err := s.role(ctx, named)
		if err != nil {
			return nil, err
		}

		held = append(held, granted...)
	}

	return held, nil
}

// role reads one role and turns its stored grants into real ones.
func (s Session) role(ctx context.Context, named string) ([]Grant, error) {
	id, err := RoleId(named)
	if err != nil {
		return nil, err
	}

	stored, err := s.guard.kernel.Get(ctx, id)
	if err != nil {
		// A role an identity names and the store does not have is not a
		// reason to refuse everything else that identity holds. It cannot
		// normally happen — the reference is strong, so the role cannot be
		// deleted while it is named — which is exactly why finding one is
		// worth stepping over rather than failing on.
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}

		return nil, err
	}

	granted, err := grantsOf(stored.Value.Spec(), s.shapeOf(ctx))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", id, err)
	}

	return granted, nil
}

// shapeOf hands grantsOf a way to ask what a kind's paths look like,
// which is what a stored prefix needs to become a real one.
func (s Session) shapeOf(ctx context.Context) func(kind.Kind) (path.TPath, error) {
	return func(named kind.Kind) (path.TPath, error) {
		head, err := s.guard.kernel.Definition(ctx, named)
		if err != nil {
			return path.TPath{}, err
		}

		return head.Definition().Shape(), nil
	}
}

// roleNames reads the roles an identity names.
func roleNames(identity resource.Resource) []string {
	items, ok := identity.Spec().Field(rolesField).AsList()
	if !ok {
		return nil
	}

	names := make([]string, 0, len(items))

	for _, item := range items {
		if name, ok := schemapb.As[string](item); ok && name != "" {
			names = append(names, name)
		}
	}

	return names
}
