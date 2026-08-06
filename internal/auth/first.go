package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The first caller, and how much of a secret it gets.
//
// 32 bytes because a credential that lives for the life of an
// installation has to survive being guessed at for that long, and there
// is no reason to be careful with bytes here.
const (
	First       = "root"
	secretBytes = 32
)

// ErrNoFirstIdentity is what a kernel with an empty store answers before
// anybody has been made. It should be impossible to see — First is
// created at the same moment the kinds are — and exists so that a kernel
// which somehow lost it fails loudly rather than serving a store nobody
// can reach.
var ErrNoFirstIdentity = errors.New("no identity exists")

// Token is a credential in the form a caller sends it: the name, and the
// secret, joined. It is the ONLY moment the secret exists in the clear —
// what the store keeps is a digest of it — so whatever is handed this has
// to write it down or lose it.
type Token string

func (t Token) String() string { return string(t) }

// Begin makes the first caller, if there is not one already.
//
// THE LOCK HAS ITS KEY INSIDE IT. Identities and roles are ordinary
// resources in the kernel they authorize, so creating one through the API
// needs a grant, and holding a grant needs an identity. Nothing can enter
// a fresh store from outside; the first way in has to be made from
// within, before the guard is in front of anything.
//
// So it is made HERE, beside the kinds it is written under, and by the
// same unguarded kernel for the same reason. What comes back is the
// credential, once — the store keeps a digest and cannot hand the secret
// back, which is the property that makes identities safe to read.
//
// A store that already has this identity is left ALONE and answers with
// no token: a kernel restarting is not a reason to mint a new credential
// and quietly invalidate what an operator saved.
func Begin(ctx context.Context, k kernel.Kernel, given string) (Token, bool, error) {
	who, err := NewPrincipal(First)
	if err != nil {
		return "", false, err
	}

	id, err := IdentityId(who)
	if err != nil {
		return "", false, err
	}

	switch _, err := k.Get(ctx, id); {
	case err == nil:
		return "", false, nil
	case !errors.Is(err, store.ErrNotFound):
		return "", false, err
	}

	secret, err := chosen(given)
	if err != nil {
		return "", false, err
	}

	if err := everything(ctx, k); err != nil {
		return "", false, err
	}

	written, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{
		rolesField:   []any{First},
		DigestsField: []any{Digest(secret)},
	}))
	if err != nil {
		return "", false, err
	}

	if _, err := k.Put(ctx, written, revision.Absent); err != nil {
		return "", false, fmt.Errorf("create %s: %w", id, err)
	}

	return Token(First + separator + secret), true, nil
}

// everything writes the role the first caller holds: every verb, on every
// kind there is.
//
// EVERY KIND THERE IS, not a wildcard, because there is no wildcard —
// a grant names one kind, which is what makes "what may this caller do"
// answerable by reading. The cost is that a kind defined later is not
// covered, and that is right: the first caller can grant itself the new
// one, and a role that silently grew to cover whatever anybody defined
// next would be a role nobody could reason about.
func everything(ctx context.Context, k kernel.Kernel) error {
	id, err := RoleId(First)
	if err != nil {
		return err
	}

	granted := []any{}

	for _, named := range kinds(ctx, k) {
		for _, verb := range Verbs() {
			granted = append(granted, map[string]any{
				grantVerbField:   verb.String(),
				grantKindField:   named,
				grantPrefixField: "",
			})
		}
	}

	written, err := resource.NewIntent(id,
		schemapb.MustStructFromGo(map[string]any{grantsField: granted}))
	if err != nil {
		return err
	}

	if _, err := k.Put(ctx, written, revision.Absent); err != nil {
		return fmt.Errorf("create %s: %w", id, err)
	}

	return nil
}

// kinds is every kind the first caller is granted: the ones defined at
// this moment, plus the one that is never defined.
//
// KIND ITSELF IS NOT A DEFINITION. Define, Undefine, GetDefinition and
// the two kind listings are authorized against the kind named "Kind" —
// the one a definition is stored under — and nothing ever declares it,
// so walking the definitions alone leaves the first caller unable to
// declare anything or even list what is there.
func kinds(ctx context.Context, k kernel.Kernel) []string {
	defined := []string{def.HeadKind.String()}

	for head, err := range k.Kinds(ctx) {
		if err != nil {
			continue
		}

		defined = append(defined, head.Kind().String())
	}

	return defined
}

// chosen is the secret a caller supplied, or a new one.
//
// Supplied wins, and that is what lets a kernel be installed with a
// credential somebody already knows — written into the file before the
// first start, rather than read out of it after. What is given is a whole
// token; what is stored is a digest of its secret half.
func chosen(given string) (string, error) {
	if given == "" {
		return secret()
	}

	_, half, err := Split(given)
	if err != nil {
		return "", fmt.Errorf("the token in the configuration is not one: %w", err)
	}

	return half, nil
}

// secret makes one, from the source that is allowed to.
func secret() (string, error) {
	raw := make([]byte, secretBytes)

	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("make a secret: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(raw), nil
}
