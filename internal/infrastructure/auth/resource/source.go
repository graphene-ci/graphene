// Package resource is the token source backed by the store itself: Role
// and Identity resources administered through the ordinary API, watched
// live, indexed in memory.
//
// Authentication happens on every rpc, so it never touches the store at
// request time: the index is loaded before serving starts and kept current
// by watches; lookups are map reads.
//
// Tokens are matched by sha256 digest. The bootstrap token is compared in
// constant time; index lookups are ordinary map reads, which is adequate
// for 256-bit random tokens (there is no secret to recover byte by byte).
package resource

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
)

const (
	scanPage    = 512
	retryPause  = time.Second
	pathSegKind = 2 // Role and Identity both live at {tenant, name}
)

// Source implements auth.TokenSource over Role/Identity resources, with a
// static bootstrap credential layered underneath (the chicken-and-egg
// breaker: the first identity has to be created by someone).
type Source struct {
	st  store.Store
	log *slog.Logger

	bootstrapDigest string
	bootstrap       auth.Credentials

	mu         sync.RWMutex
	identities map[string]identity     // key: tenant/name
	roles      map[string][]auth.Grant // key: tenant/name
	byDigest   map[string]auth.Credentials

	warmOnce sync.Once
	warm     chan struct{}
}

type identity struct {
	tenant  string
	name    string
	kind    auth.PrincipalKind
	roles   []string
	digests []string
}

// New builds the source. bootstrapToken may be empty (no bootstrap
// credential); when set, it authenticates as the identity that creates the
// first Role and Identity resources.
func New(st store.Store, bootstrapToken string, bootstrap auth.Credentials) *Source {
	src := &Source{
		st:         st,
		log:        slog.Default(),
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

// WithLogger sets the logger used for index diagnostics.
func (s *Source) WithLogger(log *slog.Logger) *Source {
	s.log = log

	return s
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

// Run loads the index and keeps it current until ctx is done.
//
// The initial load is a synchronous scan, not a watch snapshot: a snapshot
// arrives in key order carrying each entry's own revision, so an
// interrupted one cannot be resumed without losing entries. Scanning
// first, then watching from the revision observed before the scan, has
// neither gap nor loss.
func (s *Source) Run(ctx context.Context) error {
	head, err := s.st.Revision(ctx)
	if err != nil {
		return fmt.Errorf("auth: read store revision: %w", err)
	}

	if err := s.load(ctx); err != nil {
		return err
	}

	s.warmOnce.Do(func() { close(s.warm) })

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return s.watch(ctx, builtin.KindRole, head, s.handleRole) })
	group.Go(func() error { return s.watch(ctx, builtin.KindIdentity, head, s.handleIdentity) })

	if err := group.Wait(); err != nil {
		return fmt.Errorf("auth: token source: %w", err)
	}

	return nil
}

// WaitWarm blocks until the initial load has been indexed. Serving before
// that would reject valid tokens.
func (s *Source) WaitWarm(ctx context.Context) error {
	select {
	case <-s.warm:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("auth: token index warmup: %w", ctx.Err())
	}
}

// load reads every Role and Identity currently in the store.
func (s *Source) load(ctx context.Context) error {
	if err := s.scanKind(ctx, builtin.KindRole, s.handleRole); err != nil {
		return err
	}

	return s.scanKind(ctx, builtin.KindIdentity, s.handleIdentity)
}

type handler func(res *graphenepbv1.Resource, gone bool)

func (s *Source) scanKind(ctx context.Context, kind string, handle handler) error {
	var cursor []byte

	for {
		entries, next, err := s.st.Scan(ctx, store.EncodePrefix(kind), scanPage, cursor)
		if err != nil {
			return fmt.Errorf("auth: scan %s: %w", kind, err)
		}

		for _, entry := range entries {
			res, err := service.DecodeEntry(entry)
			if err != nil {
				s.log.Error("auth: undecodable resource skipped", "kind", kind, "error", err)

				continue
			}

			handle(res, false)
		}

		if next == nil {
			return nil
		}

		cursor = next
	}
}

// watch follows one kind from the given revision, re-establishing the
// stream after resets (slow-consumer eviction, store restarts) from the
// last revision it consumed — replay from a revision is gapless.
func (s *Source) watch(ctx context.Context, kind string, from uint64, handle handler) error {
	cursor := from

	for {
		events, err := s.st.Watch(ctx, store.EncodePrefix(kind), cursor)
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // shutdown is a clean exit
			}

			return fmt.Errorf("auth: watch %s: %w", kind, err)
		}

		for event := range events {
			// The catch-up boundary carries no record: it is the only
			// revision safe to resume from (catch-up events arrive in key
			// order, so their revisions are not monotonic).
			if event.Type == store.EventSync {
				cursor = event.StoreRevision

				continue
			}

			res, decodeErr := service.DecodeEntry(event.Entry)
			if decodeErr != nil {
				s.log.Error("auth: undecodable event skipped", "kind", kind, "error", decodeErr)

				continue
			}

			handle(res, event.Type == store.EventDelete)

			if event.StoreRevision > cursor {
				cursor = event.StoreRevision
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryPause):
		}
	}
}

func (s *Source) handleRole(res *graphenepbv1.Resource, gone bool) {
	key := pathKey(res)
	if key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// A resource marked deleting is on its way out: its authority must
	// stop applying at the mark, not at the end of finalization.
	if gone || res.GetDeleting() {
		delete(s.roles, key)
		s.reindexLocked()

		return
	}

	grants, err := auth.GrantsFromSpec(res.GetSpec())
	if err != nil {
		s.log.Error("auth: undecodable role skipped", "role", key, "error", err)

		return
	}

	// The grants of a role are confined to the tenant that role lives in,
	// whatever its author wrote: authority never crosses tenants.
	s.roles[key] = auth.ScopeToTenant(grants, res.GetKey().GetPath()[0])
	s.reindexLocked()
}

func (s *Source) handleIdentity(res *graphenepbv1.Resource, gone bool) {
	key := pathKey(res)
	if key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if gone || res.GetDeleting() {
		delete(s.identities, key)
		s.reindexLocked()

		return
	}

	path := res.GetKey().GetPath()
	spec := auth.IdentityFromSpec(res.GetSpec())

	s.identities[key] = identity{
		tenant:  path[0],
		name:    path[1],
		kind:    spec.PrincipalKind,
		roles:   spec.Roles,
		digests: spec.TokenSHA256,
	}
	s.reindexLocked()
}

// reindexLocked rebuilds the digest index from scratch. Identities are few
// and changes are rare; a full rebuild makes the invariant — an identity's
// grants always reflect the CURRENT roles — trivially true.
//
// A digest claimed by more than one identity is indexed for NONE of them:
// ambiguous credentials must not resolve to whichever identity a map
// iteration happened to visit last.
func (s *Source) reindexLocked() {
	index := make(map[string]auth.Credentials, len(s.byDigest))
	owner := make(map[string]string, len(s.byDigest))

	for key := range s.identities {
		ident := s.identities[key]
		creds := auth.Credentials{
			Principal: auth.Principal{
				Kind:   ident.kind,
				Tenant: ident.tenant,
				Name:   ident.name,
			},
			Grants: s.grantsForLocked(&ident),
		}

		for _, digest := range ident.digests {
			if previous, taken := owner[digest]; taken && previous != key {
				s.log.Error("auth: token digest claimed by several identities, disabled",
					"identities", previous+" and "+key)
				delete(index, digest)

				continue
			}

			owner[digest] = key
			index[digest] = creds
		}
	}

	s.byDigest = index
}

func (s *Source) grantsForLocked(ident *identity) []auth.Grant {
	var grants []auth.Grant

	for _, role := range ident.roles {
		grants = append(grants, s.roles[ident.tenant+"/"+role]...)
	}

	return grants
}

func pathKey(res *graphenepbv1.Resource) string {
	path := res.GetKey().GetPath()
	if len(path) != pathSegKind {
		return ""
	}

	return strings.Join(path, "/")
}
