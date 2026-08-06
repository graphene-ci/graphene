package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/process"
	"github.com/graphene-ci/graphene/internal/store"
)

// Where a credential rides, and how it is introduced. Both are the usual
// spellings, because a caller reaching for a gRPC library will find them
// already filled in.
const (
	header = "authorization"
	scheme = "bearer "

	// actingFor is how a kernel says it is speaking for a process it
	// started rather than for itself.
	//
	// A process holds no credential — that is the design, not an
	// omission — so somebody has to say who it is. The kernel that
	// started it does, signing with its OWN credential, and this names
	// which of its processes it means.
	//
	// The claim is not taken on trust. It is CHECKED, and the check needs
	// nothing new: a Process is addressed /{kernel}/{name}, so a kernel
	// asking to act for one can only ever name a path beginning with its
	// own name. A caller claiming somebody else's process is asking about
	// a record that does not exist under it, and gets the answer that
	// record deserves.
	actingFor = "graphene-acting-for"
)

// byCredential works out who is calling by reading what they presented.
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
func (s *Service) byCredential(ctx context.Context) (auth.Principal, error) {
	token, found := bearer(ctx)
	if !found {
		return "", nil
	}

	name, secret, err := auth.Split(token)
	if err != nil {
		return "", err
	}

	who, err := auth.NewPrincipal(name)
	if err != nil {
		return "", auth.ErrMalformedToken
	}

	id, err := auth.IdentityId(who)
	if err != nil {
		return "", auth.ErrMalformedToken
	}

	stored, err := s.kernel.Get(ctx, id)
	if err != nil {
		// An identity that is not there and a secret that does not match
		// are the same answer. Telling them apart would turn the login
		// into a way to find out who is registered.
		if errors.Is(err, store.ErrNotFound) {
			return "", auth.ErrBadToken
		}

		return "", err
	}

	if !auth.Matches(secret, digestsOf(stored.Value.Spec())) {
		return "", auth.ErrBadToken
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

// vouched resolves "this kernel is acting for one of its processes".
//
// It runs AFTER the caller has been established, and the caller is what
// makes it safe: a Process is addressed /{kernel}/{name}, so the record
// looked up here is always under the caller's own name. A kernel claiming
// another kernel's process asks about a path that does not exist under it.
//
// The identity comes from the record and not from the request. Whoever
// wrote that record was allowed to hand that identity out — that check
// happened when it was written — so nothing is being granted here that
// was not already granted then.
func (s *Service) vouched(ctx context.Context, caller auth.Principal) (auth.Principal, string, error) {
	named := metadata.ValueFromIncomingContext(ctx, actingFor)
	if len(named) == 0 {
		return caller, "", nil
	}

	// An unnamed caller may not vouch for anybody. It holds nothing, so
	// what it is asking for is somebody else's authority for free.
	if caller.IsZero() {
		return "", "", auth.ErrNoPrincipal
	}

	id, err := process.Id(caller.String(), named[0])
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", auth.ErrForbidden, err)
	}

	stored, err := s.kernel.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", "", fmt.Errorf("%w: %s has no process %q", auth.ErrForbidden, caller, named[0])
		}

		return "", "", err
	}

	identity, _ := schemapb.As[string](stored.Value.Spec().GetFields()["identity"])
	if identity == "" {
		// A process that asked for no credentials gets none, and a vouch
		// cannot conjure them. Answering as nobody is the truthful
		// answer, and nobody holds nothing.
		return "", named[0], nil
	}

	who, err := auth.NewPrincipal(identity)
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", auth.ErrForbidden, err)
	}

	return who, named[0], nil
}
