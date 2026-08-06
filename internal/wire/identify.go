package wire

import (
	"context"
	"errors"
	"strings"

	"github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/grpc/metadata"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/store"
)

// Where a credential rides, and how it is introduced. Both are the usual
// spellings, because a caller reaching for a gRPC library will find them
// already filled in.
const (
	header = "authorization"
	scheme = "bearer "
)

// identify works out who is calling.
//
// It reads through the UNGUARDED kernel, and that is not a shortcut. The
// question "who is this" cannot be asked of the guard, because asking the
// guard anything means already knowing who is asking. The regress is
// closed here, once, at the edge — and this is the only place in the
// program that reads an identity without a session.
//
// A caller with no credential is not refused here. It becomes the unnamed
// principal, and the guard refuses it a moment later along with everybody
// else who holds no grant — which is the same answer, given in the same
// words, and does not tell an anonymous caller whether the name they
// guessed exists.
func (s *Server) identify(ctx context.Context) (auth.Principal, error) {
	token, found := bearer(ctx)
	if !found {
		return "", nil
	}

	name, secret, err := split(token)
	if err != nil {
		return "", err
	}

	who, err := auth.NewPrincipal(name)
	if err != nil {
		return "", ErrMalformedToken
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		return "", ErrMalformedToken
	}

	stored, err := s.kernel.Get(ctx, id)
	if err != nil {
		// An identity that is not there and a secret that does not match
		// are the same answer. Telling them apart would turn the login
		// into a way to find out who is registered.
		if errors.Is(err, store.ErrNotFound) {
			return "", ErrBadToken
		}

		return "", err
	}

	if !matches(secret, digestsOf(stored.Value.Spec())) {
		return "", ErrBadToken
	}

	return who, nil
}

// bearer pulls a credential out of the call's metadata.
func bearer(ctx context.Context) (string, bool) {
	md, found := metadata.FromIncomingContext(ctx)
	if !found {
		return "", false
	}

	for _, value := range md.Get(header) {
		if len(value) > len(scheme) && strings.EqualFold(value[:len(scheme)], scheme) {
			return value[len(scheme):], true
		}
	}

	return "", false
}

// digestsOf reads the digests an identity knows.
func digestsOf(spec *schemapb.StructValue) []string {
	items, ok := spec.Field(auth.DigestsField).AsList()
	if !ok {
		return nil
	}

	digests := make([]string, 0, len(items))

	for _, item := range items {
		if digest, ok := schemapb.As[string](item); ok && digest != "" {
			digests = append(digests, digest)
		}
	}

	return digests
}
