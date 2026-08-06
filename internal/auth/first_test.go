package auth_test

import (
	"context"
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
)

// The first caller can do everything, and is made once.
//
// Everything, because the store it is made in has nobody else in it: a
// first identity that could not grant the second would leave a kernel
// nothing could ever reach. Once, because a kernel restarting is not a
// reason to mint a new credential and quietly invalidate the one an
// operator saved.
func TestTheFirstCallerIsMadeOnceAndCanDoEverything(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if err := auth.Bootstrap(ctx, k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	token, made, err := auth.Begin(ctx, k, "")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	if !made {
		t.Fatal("an empty store did not make a first caller")
	}

	name, secret, err := auth.Split(token.String())
	if err != nil {
		t.Fatalf("the token is not one: %v", err)
	}

	if name != auth.First || len(secret) < 32 {
		t.Fatalf("the token is %q", token)
	}

	// It reaches its own identity and its own role, which are the two it
	// needs to make anybody else.
	session := auth.New(k).As(auth.Principal(name))

	roleId, err := auth.RoleId(auth.First)
	if err != nil {
		t.Fatalf("role id: %v", err)
	}

	if _, err := session.Get(ctx, roleId); err != nil {
		t.Fatalf("the first caller cannot read its own role: %v", err)
	}

	// It can also work with KINDS, which is not one of the definitions:
	// Define, Undefine and the kind listings authorise against "Kind",
	// and nothing ever declares it — so walking the definitions alone
	// left the first caller unable to list what a kernel holds. Found by
	// running gctl against a kernel, not by reading this.
	for _, err := range session.Kinds(ctx) {
		if err != nil {
			t.Fatalf("the first caller cannot list kinds: %v", err)
		}
	}

	// And again is not a second credential.
	again, made, err := auth.Begin(ctx, k, "")
	if err != nil {
		t.Fatalf("begin again: %v", err)
	}

	if made || again != "" {
		t.Fatalf("starting again minted %q", again)
	}
}

// The secret is not what the store keeps.
//
// Reading an identity is something anybody granted `get` on it may do, so
// what is stored has to be useless to whoever reads it. This is the
// property that makes identities safe to list at all.
func TestTheStoreKeepsNoSecret(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bytes := memory.New()

	t.Cleanup(func() { _ = bytes.Close() })

	k := kernel.New(bytes)

	if err := auth.Bootstrap(ctx, k); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	token, _, err := auth.Begin(ctx, k, "")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	_, secret, err := auth.Split(token.String())
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	who, err := auth.NewPrincipal(auth.First)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		t.Fatalf("id: %v", err)
	}

	stored, err := k.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	written := stored.Value.Spec().String()
	if strings.Contains(written, secret) {
		t.Fatal("the identity carries the secret itself")
	}
}
