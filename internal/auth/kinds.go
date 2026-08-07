package auth

import (
	"context"
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/kind"
	"github.com/graphene-ci/graphene/internal/types/path"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// The two kinds authorisation is made of, and the fields they carry.
//
// Named once because a field name is a wire format twice over: it is in
// the schema, and it is in every stored value written under it. Two
// spellings of "grants" agree until one of them is changed, and then a
// role full of permissions reads as a role with none.
const (
	// RolesField and DigestsField are exported because things outside
	// this package WRITE identities: the transport checks a credential at
	// the edge, before there is a session to check it with, and joining a
	// kernel writes one. The one place that knows where each lives has to
	// be reachable from both.
	RolesField   = "roles"
	DigestsField = "digests"

	rolesField = RolesField

	grantsField      = "grants"
	grantVerbField   = "verb"
	grantKindField   = "kind"
	grantPrefixField = "prefix"
)

// IdentityKind and RoleKind are the names those kinds go by.
//
// Both are addressed by a single name, flat: these are objects of the
// installation itself and belong to nobody, the way a cluster role
// belongs to no namespace.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var (
	IdentityKind = kind.MustNew("Identity")
	RoleKind     = kind.MustNew("Role")

	IdentityShape = path.MustNewTPath("name")
	RoleShape     = path.MustNewTPath("name")
)

// Role is a named set of permissions.
//
// Grants live HERE and only here. An identity names roles and carries no
// permissions of its own, so there is one place to look when the question
// is "what may this caller do" — and one place to guard when the question
// is "who may change that".
func Role() def.Definition {
	return def.MustNew(
		RoleKind,
		RoleShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "role-spec"}).
			Fields(schemapb.List(grantsField, schemapb.Object("grant",
				schemapb.Str(grantVerbField).Required(),
				schemapb.Str(grantKindField).Required(),
				schemapb.Str(grantPrefixField),
			))).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "role-status"}).MustBuild()),
	)
}

// Identity is who a caller is, and which roles they hold.
//
// The digests are of credentials and not the credentials themselves, so
// that reading an identity — which anyone granted `get` on it can do —
// hands out nothing that could be used to become it.
//
// The roles are a STRONG reference: a role cannot be deleted while an
// identity holds it. Without that, removing a role would silently strip
// permissions from callers nobody was looking at, and the first sign of
// it would be a controller failing somewhere unrelated.
func Identity() def.Definition {
	roles, err := def.NewRef(mustFieldPath(def.SpecRoot, rolesField), RoleKind, def.Strong)
	if err != nil {
		panic("builtin identity: " + err.Error())
	}

	return def.MustNew(
		IdentityKind,
		IdentityShape,
		def.Spec(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "identity-spec"}).
			Fields(
				schemapb.List(rolesField, schemapb.Str("role")),
				schemapb.List(DigestsField, schemapb.Str("digest")),
			).
			MustBuild()),
		def.Status(schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "identity-status"}).MustBuild()),
		def.Reference(roles),
	)
}

// Bootstrap publishes the kinds authorisation needs.
//
// It goes through Define like anybody's kind, which means it is
// idempotent for free: declaring the same shape twice leaves one version.
// So this runs at every start and does nothing on all but the first.
//
// It takes the UNGUARDED kernel, and there is no other way it could work:
// there is nothing to authorize against until the kinds that hold the
// authorisation exist. This is the one call that has to happen before a
// guard is built, which is why it is a function and not a method on one.
func Bootstrap(ctx context.Context, k kernel.Kernel) error {
	for _, definition := range []def.Definition{Role(), Identity()} {
		if _, err := k.Define(ctx, definition); err != nil {
			return fmt.Errorf("bootstrap %s: %w", definition.Kind(), err)
		}
	}

	return nil
}

// IdentityId addresses one identity.
func IdentityId(named Principal) (resource.Id, error) {
	at, err := IdentityShape.New(named.String())
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(IdentityKind, at), nil
}

// RoleId addresses one role.
func RoleId(named string) (resource.Id, error) {
	at, err := RoleShape.New(named)
	if err != nil {
		return resource.Id{}, err
	}

	return resource.NewId(RoleKind, at), nil
}

// mustFieldPath builds a field path written into the binary.
func mustFieldPath(names ...string) path.FieldPath {
	built, err := path.NewFieldPath(names...)
	if err != nil {
		panic("builtin field path: " + err.Error())
	}

	return built
}
