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
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
)

const (
	scanPage    = 512
	retryPause  = time.Second
	pathSegKind = 1 // Role and Identity both live at {name}
	// processSegments — a Process lives at {kernel, name}.
	processSegments = 2
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
	identities map[string]identity     // key: the identity name
	roles      map[string][]auth.Grant // key: the role name
	// byDigest maps a token digest to the identity NAME holding it, not to
	// credentials: grants are resolved on lookup so they always reflect
	// the roles known right now.
	byDigest map[string]string
	// processes maps "<kernel>/<process>" to the identity that process
	// runs as — the whole of what a vouch is checked against.
	processes map[string]string

	warmOnce sync.Once
	warm     chan struct{}
}

type identity struct {
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
		byDigest:   map[string]string{},
		processes:  map[string]string{},
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

	name, ok := s.byDigest[digest]
	if !ok {
		return auth.Credentials{}, false
	}

	return s.credentialsLocked(name)
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
	group.Go(func() error { return s.watch(ctx, builtin.KindProcess, head, s.handleProcess) })

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

	if err := s.scanKind(ctx, builtin.KindIdentity, s.handleIdentity); err != nil {
		return err
	}

	return s.scanKind(ctx, builtin.KindProcess, s.handleProcess)
}

type handler func(res *graphenepbv1.Resource, gone bool)

func (s *Source) scanKind(ctx context.Context, kind string, handle handler) error {
	var cursor []byte

	for {
		entries, next, err := s.st.Scan(ctx, key.New(kind).Encode(), scanPage, cursor)
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
	loop := &controller.Loop{
		Stream: controller.Local(s.st, kind),
		From:   from,
		OnError: func(err error) {
			s.log.Error("auth: watch", "kind", kind, "error", err)
		},
		Handle: func(_ context.Context, event controller.Event) error {
			handle(event.Resource, event.Type == store.EventDelete)

			return nil
		},
	}

	if err := loop.Run(ctx); err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	return nil
}

func (s *Source) handleRole(res *graphenepbv1.Resource, gone bool) {
	pathName := pathKey(res)
	if pathName == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// A resource marked deleting is on its way out: its authority must
	// stop applying at the mark, not at the end of finalization.
	if gone || res.GetDeleting() {
		delete(s.roles, pathName)
		s.reindexLocked()

		return
	}

	grants, err := auth.GrantsFromSpec(res.GetSpec())
	if err != nil {
		s.log.Error("auth: undecodable role skipped", "role", pathName, "error", err)

		return
	}

	// What the role says is what the role grants. Nothing narrows it
	// afterwards — the escalation guard already refused to store any grant
	// its author did not itself hold.
	s.roles[pathName] = auth.Clone(grants)
	s.reindexLocked()
}

// handleProcess indexes which identity a process runs as. A Process
// exists only because someone allowed to hand out that identity wrote it
// — the escalation guard saw to that — so its mere presence IS the
// authorization. Its status is irrelevant here: whether the process is
// running is the kernel's business, whether it may act is ours.
func (s *Source) handleProcess(res *graphenepbv1.Resource, gone bool) {
	path := res.GetKey().GetPath()
	if len(path) != processSegments {
		return
	}

	name := strings.Join(path, "/")

	s.mu.Lock()
	defer s.mu.Unlock()

	// Deleting a Process is how a vouch is revoked, so the mark has to
	// take effect at once, not at the end of finalization.
	if gone || res.GetDeleting() {
		delete(s.processes, name)

		return
	}

	identity, _ := res.GetSpec().ToGo()["identity"].(string)
	if identity == "" {
		delete(s.processes, name) // asked for no credentials, gets none

		return
	}

	s.processes[name] = identity
}

// ActingFor implements auth.Vouching.
func (s *Source) ActingFor(kernel, process string) (auth.Credentials, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	name, ok := s.processes[kernel+"/"+process]
	if !ok {
		return auth.Credentials{}, false
	}

	return s.credentialsLocked(name)
}

// credentialsLocked resolves an identity name into what it may do, from
// the roles known at THIS moment.
//
// Resolving here rather than at index time is what makes the order in
// which watch streams deliver irrelevant: an identity that arrives before
// its role is simply an identity whose role is not known yet, and it
// becomes whole the moment the role lands — with no stale copy of its
// grants sitting in an index in the meantime.
func (s *Source) credentialsLocked(name string) (auth.Credentials, bool) {
	ident, ok := s.identities[name]
	if !ok {
		return auth.Credentials{}, false
	}

	return auth.Credentials{
		Principal: auth.Principal{Kind: ident.kind, Name: ident.name},
		Grants:    s.grantsForLocked(&ident),
	}, true
}

func (s *Source) handleIdentity(res *graphenepbv1.Resource, gone bool) {
	pathName := pathKey(res)
	if pathName == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if gone || res.GetDeleting() {
		delete(s.identities, pathName)
		s.reindexLocked()

		return
	}

	spec := auth.IdentityFromSpec(res.GetSpec())

	s.identities[pathName] = identity{
		name:    pathName,
		kind:    spec.PrincipalKind,
		roles:   spec.Roles,
		digests: spec.TokenSHA256,
	}
	s.reindexLocked()
}

// reindexLocked rebuilds the digest index from scratch. Identities are few
// and changes are rare, so a full rebuild keeps the mapping obviously
// correct instead of subtly incremental.
//
// A digest claimed by more than one identity is indexed for NONE of them:
// ambiguous credentials must not resolve to whichever identity a map
// iteration happened to visit last.
func (s *Source) reindexLocked() {
	index := make(map[string]string, len(s.byDigest))
	owner := make(map[string]string, len(s.byDigest))

	for pathName := range s.identities {
		for _, digest := range s.identities[pathName].digests {
			if previous, taken := owner[digest]; taken && previous != pathName {
				s.log.Error("auth: token digest claimed by several identities, disabled",
					"identities", previous+" and "+pathName)
				delete(index, digest)

				continue
			}

			owner[digest] = pathName
			index[digest] = pathName
		}
	}

	s.byDigest = index
}

func (s *Source) grantsForLocked(ident *identity) []auth.Grant {
	var grants []auth.Grant

	for _, role := range ident.roles {
		grants = append(grants, s.roles[role]...)
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
