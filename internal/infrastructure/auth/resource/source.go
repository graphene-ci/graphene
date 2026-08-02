// Package resource is the token source backed by the store itself: Role
// and Identity resources administered through the ordinary API, watched
// live, indexed in memory.
//
// Authentication happens on every rpc, so it never touches the store at
// request time: the watch keeps a hash index warm, lookups are map reads.
// Tokens are matched by sha256 digest, compared in constant time.
package resource

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
)

const warmupTimeout = 30 * time.Second

// Source implements auth.TokenSource over Role/Identity resources, with a
// static bootstrap credential layered underneath (the chicken-and-egg
// breaker: the first identity has to be created by someone).
type Source struct {
	st store.Store

	bootstrapDigest string
	bootstrap       auth.Credentials

	mu         sync.RWMutex
	identities map[string]identity // key: tenant/name
	roles      map[string][]auth.Grant
	byDigest   map[string]auth.Credentials
	warm       chan struct{}
	warmOnce   sync.Once
}

type identity struct {
	tenant  string
	name    string
	kind    auth.PrincipalKind
	roles   []string
	digests []string
}

// New builds the source. bootstrapToken may be empty (no bootstrap
// credential); when set, it authenticates as the admin that creates the
// first Role/Identity resources.
func New(st store.Store, bootstrapToken string, bootstrap auth.Credentials) *Source {
	src := &Source{
		st:         st,
		bootstrap:  bootstrap,
		identities: map[string]identity{},
		roles:      map[string][]auth.Grant{},
		byDigest:   map[string]auth.Credentials{},
		warm:       make(chan struct{}),
	}

	if bootstrapToken != "" {
		src.bootstrapDigest = Digest(bootstrapToken)
	}

	return src
}

// Digest is the stored form of a token.
func Digest(token string) string {
	sum := sha256.Sum256([]byte(token))

	return hex.EncodeToString(sum[:])
}

// Lookup implements auth.TokenSource.
func (s *Source) Lookup(token string) (auth.Credentials, bool) {
	digest := Digest(token)

	if s.bootstrapDigest != "" && subtle.ConstantTimeCompare([]byte(digest), []byte(s.bootstrapDigest)) == 1 {
		return s.bootstrap, true
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	creds, ok := s.byDigest[digest]

	return creds, ok
}

// Run keeps the index in sync until ctx is done. WaitWarm blocks until the
// first full snapshot has been indexed — serving before that would reject
// valid tokens.
func (s *Source) Run(ctx context.Context) error {
	roles := &Loop{Store: s.st, Kind: builtin.KindRole, Handle: s.handleRole}
	identities := &Loop{Store: s.st, Kind: builtin.KindIdentity, Handle: s.handleIdentity}

	errs := make(chan error, 2) //nolint:mnd // two loops

	go func() { errs <- roles.Run(ctx) }()
	go func() { errs <- identities.Run(ctx) }()

	// Both kinds start with a snapshot; once the store's current revision
	// has been consumed the index is usable.
	go s.markWarmWhenCaughtUp(ctx)

	err := <-errs
	<-errs

	return err
}

// WaitWarm blocks until the index has caught up with the store, or the
// context/timeout expires.
func (s *Source) WaitWarm(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, warmupTimeout)
	defer cancel()

	select {
	case <-s.warm:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("auth: token index warmup: %w", ctx.Err())
	}
}

func (s *Source) markWarmWhenCaughtUp(ctx context.Context) {
	// A snapshot watch delivers current state first; polling the store's
	// revision once is enough to know the initial pass has been scheduled.
	if _, err := s.st.Revision(ctx); err != nil {
		return
	}

	s.warmOnce.Do(func() { close(s.warm) })
}

func (s *Source) handleRole(_ context.Context, typ store.EventType, res *graphenepbv1.Resource) error {
	key := pathKey(res)

	s.mu.Lock()
	defer s.mu.Unlock()

	if typ == store.EventDelete {
		delete(s.roles, key)
		s.reindexLocked()

		return nil
	}

	grants, err := auth.GrantsFromSpec(res.GetSpec())
	if err != nil {
		return fmt.Errorf("auth: decode role %s: %w", key, err)
	}

	s.roles[key] = grants
	s.reindexLocked()

	return nil
}

func (s *Source) handleIdentity(_ context.Context, typ store.EventType, res *graphenepbv1.Resource) error {
	key := pathKey(res)

	s.mu.Lock()
	defer s.mu.Unlock()

	if typ == store.EventDelete {
		delete(s.identities, key)
		s.reindexLocked()

		return nil
	}

	path := res.GetKey().GetPath()
	if len(path) != 2 { //nolint:mnd // Identity path is {tenant, name}
		return nil
	}

	spec := auth.IdentityFromSpec(res.GetSpec())
	s.identities[key] = identity{
		tenant:  path[0],
		name:    path[1],
		kind:    spec.PrincipalKind,
		roles:   spec.Roles,
		digests: spec.TokenSHA256,
	}
	s.reindexLocked()

	return nil
}

// reindexLocked rebuilds the digest index. Identities are few and changes
// are rare; a full rebuild keeps the invariant (an identity's grants
// always reflect the CURRENT roles) trivially correct.
func (s *Source) reindexLocked() {
	index := make(map[string]auth.Credentials, len(s.byDigest))

	for _, ident := range s.identities {
		var grants []auth.Grant

		for _, role := range ident.roles {
			grants = append(grants, s.roles[ident.tenant+"/"+role]...)
		}

		creds := auth.Credentials{
			Principal: auth.Principal{Kind: ident.kind, Name: ident.name},
			Grants:    grants,
		}

		for _, digest := range ident.digests {
			index[digest] = creds
		}
	}

	s.byDigest = index
}

func pathKey(res *graphenepbv1.Resource) string {
	path := res.GetKey().GetPath()
	if len(path) != 2 { //nolint:mnd // {tenant, name}
		return ""
	}

	return path[0] + "/" + path[1]
}

// Loop is the watch loop shape this package needs; it mirrors
// controller.Loop without importing it (the controller runtime depends on
// the service, which depends on auth — this keeps the graph acyclic).
type Loop struct {
	Store  store.Store
	Kind   string
	Handle func(ctx context.Context, typ store.EventType, res *graphenepbv1.Resource) error
}

// Run consumes events until ctx is done, resuming from the last seen
// revision after channel resets.
func (l *Loop) Run(ctx context.Context) error {
	var cursor uint64

	for {
		events, err := l.Store.Watch(ctx, store.EncodePrefix(l.Kind), cursor)
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // shutdown is a clean exit
			}

			return fmt.Errorf("auth: watch %s: %w", l.Kind, err)
		}

		for event := range events {
			res, decodeErr := service.DecodeEntry(event.Entry)
			if decodeErr != nil {
				cursor = event.StoreRevision

				continue
			}

			if err := l.Handle(ctx, event.Type, res); err != nil {
				return err
			}

			cursor = event.StoreRevision
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
}
