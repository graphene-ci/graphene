package auth

import (
	"errors"
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// Grant is one permission: a verb, a kind, and how much of that kind.
//
// The confinement is a PATH PREFIX and nothing else. There is no
// predicate over field values, which the old code had — and doing without
// it is a choice rather than an omission, because a prefix is already
// exactly the right tool and it is machinery the store, the scan and the
// watch are all built out of.
//
// The price is that a kind must put its discriminating value in the path:
// a Process addressed /{kernel}/{name} can be confined to one kernel, one
// addressed /{name} cannot. That is pressure in the right direction — the
// path is where "which one" belongs — and it is pressure applied at the
// moment a kind is declared rather than at the moment somebody needs a
// rule that cannot be written.
type Grant struct {
	verb   Verb
	kind   kind.Kind
	prefix path.Path
}

// NewGrant states one permission.
//
// A verb that addresses one resource may carry a prefix; one that does
// not — Define, Undefine — may not, because there is no instance for a
// prefix to confine and a grant that looked confined but was not would
// read as narrower than it is.
func NewGrant(verb Verb, named kind.Kind, prefix path.Path) (Grant, error) {
	switch {
	case verb.IsZero():
		return Grant{}, ErrNoVerb
	case named.IsZero():
		return Grant{}, fmt.Errorf("%w: %s grants nothing", ErrNoKind, verb)
	case !verb.AddressesOne() && !prefix.IsZero():
		return Grant{}, fmt.Errorf("%w: %s is about the kind itself", ErrKindVerbPath, verb)
	}

	return Grant{verb: verb, kind: named, prefix: prefix}, nil
}

// Verb is what this permits.
func (g Grant) Verb() Verb { return g.verb }

// Kind is what it permits it on.
func (g Grant) Kind() kind.Kind { return g.kind }

// Prefix is how much of that kind, the zero path meaning all of it.
func (g Grant) Prefix() path.Path { return g.prefix }

// IsZero reports a grant that was never stated.
func (g Grant) IsZero() bool { return g.verb.IsZero() }

// Allows reports whether this grant permits a verb on a resource.
//
// An empty prefix is not "nothing", it is EVERYTHING of that kind — the
// same reading the byte layer gives an empty key, and for the same
// reason: a prefix confines by what it says, and one that says nothing
// confines nothing.
func (g Grant) Allows(verb Verb, id resource.Id) bool {
	if g.verb != verb || !g.kind.Eq(id.Kind()) {
		return false
	}

	return id.Path().HasPrefix(g.prefix)
}

// AllowsKind reports whether this grant permits a kind-level verb.
func (g Grant) AllowsKind(verb Verb, named kind.Kind) bool {
	return g.verb == verb && g.kind.Eq(named)
}

// Covers reports whether holding this grant is enough to hand out the
// other one.
//
// This is the whole of the no-escalation rule. Writing a role means
// handing out permissions, and handing out what you do not have is how a
// caller allowed to manage users becomes a caller allowed to do anything.
// Covering is the same question a scan asks — is this under that — which
// is why the answer is a prefix comparison and not a policy engine.
func (g Grant) Covers(other Grant) bool {
	if g.verb != other.verb || !g.kind.Eq(other.kind) {
		return false
	}

	return other.prefix.HasPrefix(g.prefix)
}

func (g Grant) String() string {
	if !g.verb.AddressesOne() {
		return g.verb.String() + " " + g.kind.String()
	}

	return g.verb.String() + " " + g.kind.String() + g.prefix.String()
}

// ParseGrant reads a grant as it is stored: three strings.
//
// The prefix is text until here because turning it into a path needs the
// SHAPE of the kind it confines, and that lives in the kind's definition.
// So a grant is parsed where the definitions are, once when a role is
// read, rather than on every request.
func ParseGrant(verb, named, prefix string, shape path.TPath) (Grant, error) {
	parsedVerb, err := ParseVerb(verb)
	if err != nil {
		return Grant{}, err
	}

	parsedKind, err := kind.New(named)
	if err != nil {
		return Grant{}, fmt.Errorf("grant kind: %w", err)
	}

	if !parsedVerb.AddressesOne() {
		return NewGrant(parsedVerb, parsedKind, path.Path{})
	}

	at, err := shape.Parse(prefix)
	if err != nil {
		return Grant{}, fmt.Errorf("grant prefix: %w", err)
	}

	return NewGrant(parsedVerb, parsedKind, at)
}

// grantsOf turns a role's stored grants into the real thing.
//
// It takes the SPEC and not the resource, because the grants being
// checked for escalation are in an intent that has not been admitted yet
// — and checking them afterwards would be checking them too late.
//
// shapeOf hands back the path shape of a kind, which is what a prefix
// needs to become a path. It is a function and not a kernel so that this
// stays testable without one, and so that the lookup it does is visible
// at the call site rather than hidden in here.
func grantsOf(
	spec *schemapb.StructValue,
	shapeOf func(kind.Kind) (path.TPath, error),
) ([]Grant, error) {
	stated, _ := spec.Field(grantsField).AsList()
	granted := make([]Grant, 0, len(stated))

	for _, one := range stated {
		verb := text(one, grantVerbField)
		namedText := text(one, grantKindField)

		named, err := kind.New(namedText)
		if err != nil {
			return nil, fmt.Errorf("grant kind: %w", err)
		}

		prefix := text(one, grantPrefixField)

		shape, err := shapeOf(named)
		if err != nil {
			// A grant may name a kind nobody has defined yet, and the
			// commonest one is exactly that: `define Process` is what
			// brings Process into existence, so requiring Process to exist
			// before the grant can be read would mean nobody could ever be
			// allowed to create it.
			//
			// Without a shape a prefix cannot become a path, so a grant
			// that confines itself to one is dropped — it authorizes
			// nothing either way, since a kind with no definition has no
			// instances. One that confines nothing needs no shape and is
			// kept, which is what makes the bootstrap possible.
			if !errors.Is(err, kernel.ErrNoSuchKind) {
				return nil, err
			}

			if prefix != "" {
				continue
			}

			shape = path.TPath{}
		}

		grant, err := ParseGrant(verb, namedText, prefix, shape)
		if err != nil {
			return nil, err
		}

		granted = append(granted, grant)
	}

	return granted, nil
}

// text reads one field of a stored grant.
//
// A field that is absent and a field holding the empty string come back
// the same, and that is right here: a grant with no verb and a grant with
// an empty verb are both refused a line later, by the one place that
// decides what a verb may be.
func text(at *schemapb.Value, field string) string {
	found, _ := schemapb.As[string](at.Field(field))

	return found
}

// covered reports whether every grant in want is covered by one in held.
func covered(held, want []Grant) (Grant, bool) {
	for _, one := range want {
		if !hasCover(held, one) {
			return one, false
		}
	}

	return Grant{}, true
}

func hasCover(held []Grant, want Grant) bool {
	for _, one := range held {
		if one.Covers(want) {
			return true
		}
	}

	return false
}
